package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/mozership/probe/sdk"
)

// The operations of with.sqs, each by the key that holds the URL of the
// queue.
const (
	sqsReceive = "receive"
	sqsSend    = "send"
	sqsDelete  = "delete"
)

var sqsOperations = []string{sqsReceive, sqsSend, sqsDelete}

// sqsParams are the keys of with.sqs, and sqsParamsOf those each operation
// takes besides its own.
var (
	sqsParams   = []string{sqsReceive, sqsSend, sqsDelete, "wait", "max", "visibility_timeout", "delete_received", "body", "delay", "group_id", "deduplication_id", "attributes", "receipt_handle"}
	sqsParamsOf = map[string][]string{
		sqsReceive: {"wait", "max", "visibility_timeout", "delete_received"},
		sqsSend:    {"body", "delay", "group_id", "deduplication_id", "attributes"},
		sqsDelete:  {"receipt_handle"},
	}
)

// The limits SQS sets.
const (
	maxWait       = 20 * time.Second
	maxVisibility = 12 * time.Hour
	maxDelay      = 15 * time.Minute
	maxMessages   = 10
)

// sqsHost is the host of a queue URL of AWS, which names its region.
var sqsHost = regexp.MustCompile(`^sqs\.([a-z0-9-]+)\.amazonaws\.com(\.cn)?$`)

type sqsRequest struct {
	op       string
	queueURL string
	region   string
	// target is where the request is sent: the endpoint, or the host of
	// the queue.
	target string

	wait           *time.Duration
	max            int
	visibility     *time.Duration
	deleteReceived bool

	body        string
	written     any
	delay       *time.Duration
	groupID     string
	dedupID     string
	attributes  map[string]string
	receiptHand string
}

// runSQS does the operation in with.sqs on a queue of SQS, or of a service
// that speaks its JSON protocol.
func (a *Action) runSQS(cfg config, given map[string]any, guard sdk.Guard) (map[string]any, error) {
	r, err := parseSQS(cfg, given)
	if err != nil {
		return nil, err
	}
	if err := r.checkReadOnly(guard); err != nil {
		return nil, err
	}
	if err := requireCredentials(cfg, "sqs"); err != nil {
		return nil, err
	}

	what := "sqs " + r.op
	payload, target := r.payload()
	rep, err := a.callSQS(cfg, r, target, payload, guard, what)
	if err != nil {
		return nil, err
	}
	res, err := r.read(rep)
	if err != nil {
		return nil, err
	}

	rt := rep.rt
	if r.op == sqsReceive && r.deleteReceived && rep.code == http.StatusOK {
		deleted := 0
		for _, m := range res["messages"].([]any) {
			handle, _ := m.(map[string]any)["receipt_handle"].(string)
			del, err := a.callSQS(cfg, r, "DeleteMessage", map[string]any{"QueueUrl": r.queueURL, "ReceiptHandle": handle}, guard, "sqs delete of a received message")
			if err != nil {
				return nil, err
			}
			rt += del.rt
			if del.code != http.StatusOK {
				code, message := sqsError(del.body)
				return nil, fmt.Errorf("sqs could not delete a received message: %d %s %s", del.code, code, message)
			}
			deleted++
		}
		res["deleted"] = deleted
	}

	return map[string]any{
		"req":    r.req(cfg),
		"res":    res,
		"rt":     rt.String(),
		"status": statusOf(rep.code),
	}, nil
}

func (a *Action) callSQS(cfg config, r *sqsRequest, target string, payload map[string]any, guard sdk.Guard, what string) (*reply, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return a.send(signed{
		method: http.MethodPost,
		url:    r.target,
		headers: map[string]string{
			"Content-Type": "application/x-amz-json-1.0",
			"X-Amz-Target": "AmazonSQS." + target,
		},
		body:    body,
		service: "sqs",
		region:  r.region,
		creds:   cfg.creds,
	}, cfg.timeout, guard, what)
}

func parseSQS(cfg config, with map[string]any) (*sqsRequest, error) {
	for k := range with {
		if !slices.Contains(sqsParams, k) {
			return nil, fmt.Errorf("with.sqs takes %s, not %s", strings.Join(sqsParams, ", "), k)
		}
	}
	var named []string
	for _, op := range sqsOperations {
		if _, ok := with[op]; ok {
			named = append(named, op)
		}
	}
	switch len(named) {
	case 0:
		return nil, fmt.Errorf("with.sqs requires exactly one of %s, each the URL of a queue", strings.Join(sqsOperations, ", "))
	case 1:
	default:
		return nil, fmt.Errorf("with.sqs takes exactly one of %s, not %s together", strings.Join(sqsOperations, ", "), strings.Join(named, " and "))
	}
	r := &sqsRequest{op: named[0]}
	for k := range with {
		if k != r.op && !slices.Contains(sqsParamsOf[r.op], k) {
			return nil, fmt.Errorf("with.sqs.%s is not for %s", k, r.op)
		}
	}

	var err error
	if r.queueURL, err = optionalString(with, r.op, "with.sqs."+r.op); err != nil {
		return nil, err
	}
	u, err := url.Parse(r.queueURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
		return nil, fmt.Errorf("with.sqs.%s must be the URL of a queue, such as https://sqs.us-east-1.amazonaws.com/123456789012/jobs, not %q", r.op, r.queueURL)
	}

	r.region = cfg.region
	if r.region == "" {
		if m := sqsHost.FindStringSubmatch(u.Hostname()); m != nil {
			r.region = m[1]
		}
	}
	if r.region == "" {
		return nil, errors.New("sqs needs a region: give with.region, or set AWS_REGION, when the URL of the queue does not name one")
	}

	if cfg.endpoint != nil {
		r.target = cfg.endpoint.String() + "/"
	} else {
		r.target = (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: "/"}).String()
	}

	switch r.op {
	case sqsReceive:
		err = r.parseReceive(with)
	case sqsSend:
		err = r.parseSend(with)
	case sqsDelete:
		r.receiptHand, err = optionalString(with, "receipt_handle", "with.sqs.receipt_handle")
		if err == nil && r.receiptHand == "" {
			err = errors.New("with.sqs.delete requires receipt_handle, which receive returns for each message")
		}
	}
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (r *sqsRequest) parseReceive(with map[string]any) error {
	var err error
	if r.wait, err = optionalDuration(with, "wait", 0, maxWait); err != nil {
		return err
	}
	if r.visibility, err = optionalDuration(with, "visibility_timeout", 0, maxVisibility); err != nil {
		return err
	}
	r.max = 1
	if v, ok := with["max"]; ok && v != nil {
		n, ok := toInt(v)
		if !ok || n < 1 || n > maxMessages {
			return fmt.Errorf("with.sqs.max must be a whole number from 1 to %d, not %v", maxMessages, v)
		}
		r.max = n
	}
	if v, ok := with["delete_received"]; ok && v != nil {
		b, ok := v.(bool)
		if !ok {
			return fmt.Errorf("with.sqs.delete_received must be true or false, not %v", v)
		}
		r.deleteReceived = b
	}
	return nil
}

func (r *sqsRequest) parseSend(with map[string]any) error {
	body, ok := with["body"]
	if !ok || body == nil {
		return errors.New("with.sqs.send requires body")
	}
	r.written = body
	if s, ok := body.(string); ok {
		r.body = s
	} else {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("with.sqs.body cannot be sent as JSON: %w", err)
		}
		r.body = string(b)
	}
	if r.body == "" {
		return errors.New("with.sqs.body must not be empty, which SQS refuses")
	}

	var err error
	if r.delay, err = optionalDuration(with, "delay", 0, maxDelay); err != nil {
		return err
	}
	if r.groupID, err = optionalString(with, "group_id", "with.sqs.group_id"); err != nil {
		return err
	}
	if r.dedupID, err = optionalString(with, "deduplication_id", "with.sqs.deduplication_id"); err != nil {
		return err
	}
	if v, ok := with["attributes"]; ok && v != nil {
		attrs, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("with.sqs.attributes must be an object, not %T", v)
		}
		r.attributes = make(map[string]string, len(attrs))
		for k, v := range attrs {
			r.attributes[k] = fmt.Sprint(v)
		}
	}
	return nil
}

// optionalDuration reads with.sqs[key], a duration such as 20s or a number
// of seconds, within min and max, as SQS takes it in whole seconds. It
// returns nil when the key is not given.
func optionalDuration(with map[string]any, key string, lowest, highest time.Duration) (*time.Duration, error) {
	v, ok := with[key]
	if !ok || v == nil {
		return nil, nil
	}
	d, err := parseTimeout(v)
	if err != nil {
		return nil, fmt.Errorf("with.sqs.%s must be a duration or a number of seconds, not %v", key, v)
	}
	if d < lowest || d > highest || d%time.Second != 0 {
		return nil, fmt.Errorf("with.sqs.%s must be whole seconds from %s to %s, not %v", key, lowest, highest, v)
	}
	return &d, nil
}

// checkReadOnly returns a Refused error when the run is read-only and the
// step would change the queue. A receive changes it too, as the messages it
// takes are hidden from other consumers for the visibility timeout, unless
// that timeout is 0, which leaves them in the queue as they were.
func (r *sqsRequest) checkReadOnly(guard sdk.Guard) error {
	if !guard.ReadOnly {
		return nil
	}
	switch {
	case r.op != sqsReceive:
		return sdk.Refuse("sqs %s changes the queue, and the run is read-only; only receive with visibility_timeout: 0 is run", r.op)
	case r.deleteReceived:
		return sdk.Refuse("sqs receive with delete_received changes the queue, and the run is read-only")
	case r.visibility == nil || *r.visibility != 0:
		return sdk.Refuse("sqs receive hides the messages it takes from other consumers, and the run is read-only; give visibility_timeout: 0 to leave them in the queue")
	}
	return nil
}

// payload returns the target of the operation, as X-Amz-Target names it,
// and the JSON it sends.
func (r *sqsRequest) payload() (map[string]any, string) {
	p := map[string]any{"QueueUrl": r.queueURL}
	switch r.op {
	case sqsReceive:
		p["MaxNumberOfMessages"] = r.max
		p["MessageSystemAttributeNames"] = []string{"All"}
		p["MessageAttributeNames"] = []string{"All"}
		if r.wait != nil {
			p["WaitTimeSeconds"] = int(r.wait.Seconds())
		}
		if r.visibility != nil {
			p["VisibilityTimeout"] = int(r.visibility.Seconds())
		}
		return p, "ReceiveMessage"
	case sqsSend:
		p["MessageBody"] = r.body
		if r.delay != nil {
			p["DelaySeconds"] = int(r.delay.Seconds())
		}
		if r.groupID != "" {
			p["MessageGroupId"] = r.groupID
		}
		if r.dedupID != "" {
			p["MessageDeduplicationId"] = r.dedupID
		}
		if len(r.attributes) > 0 {
			attrs := map[string]any{}
			for k, v := range r.attributes {
				attrs[k] = map[string]string{"DataType": "String", "StringValue": v}
			}
			p["MessageAttributes"] = attrs
		}
		return p, "SendMessage"
	default:
		p["ReceiptHandle"] = r.receiptHand
		return p, "DeleteMessage"
	}
}

// read turns what SQS answered into res.
func (r *sqsRequest) read(rep *reply) (map[string]any, error) {
	res := map[string]any{
		"code":          rep.code,
		"status":        rep.status,
		"headers":       flattenHeaders(rep.headers),
		"error_code":    "",
		"error_message": "",
	}
	if rep.code != http.StatusOK {
		res["error_code"], res["error_message"] = sqsError(rep.body)
		if r.op == sqsReceive {
			res["messages"] = []any{}
			res["count"] = 0
		}
		return res, nil
	}

	switch r.op {
	case sqsReceive:
		var out struct {
			Messages []struct {
				MessageID         string            `json:"MessageId"`
				ReceiptHandle     string            `json:"ReceiptHandle"`
				MD5OfBody         string            `json:"MD5OfBody"`
				Body              string            `json:"Body"`
				Attributes        map[string]string `json:"Attributes"`
				MessageAttributes map[string]struct {
					DataType    string `json:"DataType"`
					StringValue string `json:"StringValue"`
				} `json:"MessageAttributes"`
			} `json:"Messages"`
		}
		if err := json.Unmarshal(rep.body, &out); err != nil {
			return nil, fmt.Errorf("sqs receive answered what is not JSON: %w", err)
		}
		messages := make([]any, 0, len(out.Messages))
		for _, m := range out.Messages {
			attrs := map[string]any{}
			for k, v := range m.MessageAttributes {
				attrs[k] = v.StringValue
			}
			system := map[string]any{}
			for k, v := range m.Attributes {
				system[k] = v
			}
			msg := map[string]any{
				"id":             m.MessageID,
				"receipt_handle": m.ReceiptHandle,
				"md5":            m.MD5OfBody,
				"body":           m.Body,
				"attributes":     attrs,
				"system":         system,
			}
			// A body written as JSON is read as what it holds, as Probe
			// reads res.body; rawbody keeps it as it was sent.
			var parsed any
			if t := strings.TrimSpace(m.Body); (strings.HasPrefix(t, "{") || strings.HasPrefix(t, "[")) && json.Unmarshal([]byte(t), &parsed) == nil {
				msg["body"] = parsed
				msg["rawbody"] = m.Body
			}
			messages = append(messages, msg)
		}
		res["messages"] = messages
		res["count"] = len(messages)
	case sqsSend:
		var out struct {
			MessageID      string `json:"MessageId"`
			MD5OfBody      string `json:"MD5OfMessageBody"`
			SequenceNumber string `json:"SequenceNumber"`
		}
		if err := json.Unmarshal(rep.body, &out); err != nil {
			return nil, fmt.Errorf("sqs send answered what is not JSON: %w", err)
		}
		res["message_id"] = out.MessageID
		res["md5"] = out.MD5OfBody
		res["sequence_number"] = out.SequenceNumber
	}
	return res, nil
}

// sqsError reads the error an answer of SQS carries: its code, without the
// namespace, as QueueDoesNotExist, and its message.
func sqsError(body []byte) (string, string) {
	var e struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &e) != nil {
		return "", strings.TrimSpace(string(body))
	}
	code := e.Type
	if i := strings.LastIndex(code, "#"); i >= 0 {
		code = code[i+1:]
	}
	return code, e.Message
}

// req tells what the step did, without the credentials.
func (r *sqsRequest) req(cfg config) map[string]any {
	req := map[string]any{
		"operation": r.op,
		"queue_url": r.queueURL,
		"region":    r.region,
		"url":       r.target,
		"signed":    true,
	}
	switch r.op {
	case sqsReceive:
		req["max"] = r.max
		req["delete_received"] = r.deleteReceived
		if r.wait != nil {
			req["wait"] = r.wait.String()
		}
		if r.visibility != nil {
			req["visibility_timeout"] = r.visibility.String()
		}
	case sqsSend:
		req["body"] = r.written
		req["group_id"] = r.groupID
		req["deduplication_id"] = r.dedupID
		req["attributes"] = r.attributes
		if r.delay != nil {
			req["delay"] = r.delay.String()
		}
	case sqsDelete:
		req["receipt_handle"] = r.receiptHand
	}
	return req
}
