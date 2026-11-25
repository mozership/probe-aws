package main

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/mozership/probe-aws/internal/sigv4"
	"github.com/mozership/probe/sdk"
)

// The operations a step can ask for, each by a key of with.
const (
	opGet    = "get"
	opHead   = "head"
	opList   = "list"
	opPut    = "put"
	opDelete = "delete"
)

// operations are the keys of with that name an operation. A step has
// exactly one of them.
var operations = []string{opGet, opHead, opList, opPut, opDelete}

// readOperations are the operations that change nothing in the bucket, and
// so the only ones run under read-only.
var readOperations = []string{opGet, opHead, opList}

var (
	// bucketName is a name S3 takes for a bucket. It becomes part of the
	// host or of the path, so nothing else is let through.
	bucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	// headerToken is a name that can stand in a header after x-amz-meta-.
	headerToken = regexp.MustCompile(`^[A-Za-z0-9!#$%&'*+.^_|~-]+$`)
)

// DefaultRegion is the region an S3 request is signed for when neither
// with.region nor the environment names one. S3-compatible storage takes it
// as a rule.
const DefaultRegion = "us-east-1"

// s3Params are the keys of with.s3.
var s3Params = []string{
	"get", "head", "list", "put", "delete",
	"bucket", "path_style",
	"body", "file", "content_type", "metadata",
	"max_keys",
}

// runS3 does the operation in with.s3 on a bucket of S3, or of storage that
// speaks its protocol.
func (a *Action) runS3(cfg config, given map[string]any, guard sdk.Guard) (map[string]any, error) {
	r, err := parseS3(cfg, given)
	if err != nil {
		return nil, err
	}
	if err := checkReadOnly(guard, r); err != nil {
		return nil, err
	}
	return a.do(r, guard)
}

// checkReadOnly returns a Refused error when the run is read-only and the
// step writes to the bucket.
func checkReadOnly(guard sdk.Guard, r *s3Request) error {
	if !guard.ReadOnly || slices.Contains(readOperations, r.op) {
		return nil
	}
	return sdk.Refuse("%s writes to the bucket, and the run is read-only; only %s are run", r.op, strings.Join(readOperations, ", "))
}

type s3Request struct {
	// op is the operation, and key the key it is done on, or the prefix a
	// list is narrowed to.
	op  string
	key string

	bucket string
	region string
	// endpoint is where S3-compatible storage is served, or nil for AWS.
	endpoint  *url.URL
	pathStyle bool
	creds     sigv4.Credentials

	// body is what a put sends, unless file names a file to send.
	body []byte
	// written is the body as with has it, for req.
	written     any
	file        string
	contentType string
	metadata    map[string]string

	// maxKeys is the most keys a list returns, or 0 for as many as the
	// storage returns by default.
	maxKeys int
	timeout time.Duration
}

func parseS3(cfg config, with map[string]any) (*s3Request, error) {
	r := &s3Request{
		region:   cmp.Or(cfg.region, DefaultRegion),
		endpoint: cfg.endpoint,
		creds:    cfg.creds,
		timeout:  cfg.timeout,
	}

	for k := range with {
		if !slices.Contains(s3Params, k) {
			return nil, fmt.Errorf("with.s3 takes %s, not %s", strings.Join(s3Params, ", "), k)
		}
	}

	var given []string
	for _, op := range operations {
		if _, ok := with[op]; ok {
			given = append(given, op)
		}
	}
	if len(given) != 1 {
		if len(given) == 0 {
			return nil, fmt.Errorf("with.s3 requires exactly one of %s", strings.Join(operations, ", "))
		}
		return nil, fmt.Errorf("with.s3 takes exactly one of %s, not %s together", strings.Join(operations, ", "), strings.Join(given, " and "))
	}
	r.op = given[0]
	switch v := with[r.op].(type) {
	case string:
		r.key = v
	case nil:
	default:
		return nil, fmt.Errorf("with.s3.%s must be a string, not %T", r.op, v)
	}
	if r.key == "" && r.op != opList {
		return nil, fmt.Errorf("with.s3.%s must be the key of an object", r.op)
	}

	var err error
	if r.bucket, err = optionalString(with, "bucket", "with.s3.bucket"); err != nil {
		return nil, err
	}
	if r.bucket == "" {
		return nil, errors.New("with.s3 requires bucket")
	}
	if !bucketName.MatchString(r.bucket) || strings.Contains(r.bucket, "..") {
		return nil, fmt.Errorf("with.s3.bucket must be a bucket name of 3 to 63 lowercase letters, digits, dots and hyphens, not %q", r.bucket)
	}

	// A custom endpoint is addressed by path, as MinIO and the like are
	// served under one name. So is a bucket with a dot on AWS, whose name
	// the certificate of *.s3.<region>.amazonaws.com does not cover.
	r.pathStyle = r.endpoint != nil || strings.Contains(r.bucket, ".")
	if v, ok := with["path_style"]; ok && v != nil {
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("with.s3.path_style must be true or false, not %v", v)
		}
		r.pathStyle = b
	}

	if err := r.parsePut(with); err != nil {
		return nil, err
	}

	if v, ok := with["max_keys"]; ok && v != nil {
		if r.op != opList {
			return nil, fmt.Errorf("with.s3.max_keys is only for list, not %s", r.op)
		}
		n, ok := toInt(v)
		if !ok || n < 1 {
			return nil, fmt.Errorf("with.s3.max_keys must be a positive integer, not %v", v)
		}
		r.maxKeys = n
	}

	return r, nil
}

// parsePut reads what a put sends: with.s3.body or with.s3.file, and
// with.s3.content_type and with.s3.metadata. No other operation takes them.
func (r *s3Request) parsePut(with map[string]any) error {
	for _, k := range []string{"body", "file", "content_type", "metadata"} {
		if v, ok := with[k]; ok && v != nil && r.op != opPut {
			return fmt.Errorf("with.s3.%s is only for put, not %s", k, r.op)
		}
	}
	if r.op != opPut {
		return nil
	}

	var err error
	if r.file, err = optionalString(with, "file", "with.s3.file"); err != nil {
		return err
	}
	body, hasBody := with["body"]
	hasBody = hasBody && body != nil
	switch {
	case hasBody && r.file != "":
		return errors.New("put takes with.s3.body or with.s3.file, not both")
	case !hasBody && r.file == "":
		return errors.New("put requires with.s3.body or with.s3.file")
	}

	if r.contentType, err = optionalString(with, "content_type", "with.s3.content_type"); err != nil {
		return err
	}
	if hasBody {
		r.written = body
		if s, ok := body.(string); ok {
			r.body = []byte(s)
		} else {
			// Anything but a string is sent as JSON, as the http action
			// sends a body written as an object.
			if r.body, err = json.Marshal(body); err != nil {
				return fmt.Errorf("with.s3.body cannot be sent as JSON: %w", err)
			}
			if r.contentType == "" {
				r.contentType = "application/json"
			}
		}
	}

	if v, ok := with["metadata"]; ok && v != nil {
		meta, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("with.s3.metadata must be an object, not %T", v)
		}
		r.metadata = make(map[string]string, len(meta))
		for k, v := range meta {
			if !headerToken.MatchString(k) {
				return fmt.Errorf("with.s3.metadata has a name a header cannot carry: %q", k)
			}
			s := fmt.Sprint(v)
			if !printableASCII(s) {
				return fmt.Errorf("with.s3.metadata.%s must be printable ASCII, which is what a header carries", k)
			}
			r.metadata[strings.ToLower(k)] = s
		}
	}
	return nil
}

func printableASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

// toInt returns v as an int when it is a whole number.
func toInt(v any) (int, bool) {
	var f float64
	switch n := v.(type) {
	case int:
		f = float64(n)
	case int32:
		f = float64(n)
	case int64:
		f = float64(n)
	case uint64:
		f = float64(n)
	case float32:
		f = float64(n)
	case float64:
		f = n
	default:
		return 0, false
	}
	if f != math.Trunc(f) || f > math.MaxInt32 || f < math.MinInt32 {
		return 0, false
	}
	return int(f), true
}
