package main

import (
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/mozership/probe-aws/internal/sigv4"
	"github.com/mozership/probe/sdk"
)

// DefaultTimeout bounds a step that does not set with.timeout.
const DefaultTimeout = 30 * time.Second

// The services a step can call, each by a key of with that holds what to
// do. A step has exactly one of them.
const (
	serviceS3      = "s3"
	serviceSQS     = "sqs"
	serviceRequest = "request"
)

var services = []string{serviceS3, serviceSQS, serviceRequest}

// commonParams are the keys of with that every service reads.
var commonParams = []string{"region", "endpoint", "access_key_id", "secret_access_key", "session_token", "timeout"}

// params are the keys the action takes in with. action.yml declares them,
// for probe check to report a key the action does not take.
var params = append(slices.Clone(services), commonParams...)

// keeps are the kinds of guard the action keeps to, as action.yml declares
// them: it only reads under read-only, and connects only to a host the run
// allows.
var keeps = []string{sdk.KindReadOnly, sdk.KindAllowHost}

// secretParams are the keys of with whose values are credentials, and are
// kept out of the log.
var secretParams = []string{"secret_access_key", "session_token"}

// regionName is the name of a region, which becomes part of a host.
var regionName = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// Action calls the AWS service a step names, or a service that speaks its
// protocol, and returns what the service answered.
type Action struct {
	log hclog.Logger
	// transport sends the requests. It defaults to http.DefaultTransport.
	transport http.RoundTripper
	// getenv reads the environment the credentials and the region fall
	// back to. It defaults to os.Getenv.
	getenv func(string) string
	// now tells the time a request is signed at. It defaults to time.Now.
	now func() time.Time
}

// Run calls the service in with, under no guard.
func (a *Action) Run(with map[string]any) (map[string]any, error) {
	ret, _, err := a.RunStep(sdk.Call{With: with})
	return ret, err
}

// RunStep calls the service in call.With under the guard of the run. Once
// the service answers, what it answered is a result, a 404 or a 403
// included; only a request that gets no answer, or a step the guard
// refuses, is an error.
func (a *Action) RunStep(call sdk.Call) (map[string]any, map[string]any, error) {
	if a.log == nil {
		a.log = hclog.NewNullLogger()
	}
	sdk.LogParams(a.log, "received request parameters", withoutSecrets(call.With))

	getenv := a.getenv
	if getenv == nil {
		getenv = os.Getenv
	}
	cfg, service, given, err := parseConfig(call.With, getenv)
	if err != nil {
		return nil, nil, err
	}

	var ret map[string]any
	switch service {
	case serviceS3:
		ret, err = a.runS3(cfg, given, call.Guard)
	case serviceSQS:
		ret, err = a.runSQS(cfg, given, call.Guard)
	case serviceRequest:
		ret, err = a.runRequest(cfg, given, call.Guard)
	}
	sdk.LogOutcome(a.log, service, ret, err)
	return ret, nil, err
}

// withoutSecrets returns with as it can be logged: the secret access key
// and the session token replaced, which the log would otherwise show to a
// Probe that does not know their names to hold a credential.
func withoutSecrets(with map[string]any) map[string]any {
	out := maps.Clone(with)
	for _, k := range secretParams {
		if _, ok := out[k]; ok {
			out[k] = "<redacted>"
		}
	}
	return out
}

// config is what every service reads of with: where the service is, how to
// sign for it and how long to wait.
type config struct {
	// region is the region given, or the one of the environment; empty
	// when neither names one, for the service to tell its own.
	region string
	// endpoint is where a service compatible with AWS is served, or nil
	// for AWS.
	endpoint *url.URL
	creds    sigv4.Credentials
	timeout  time.Duration
}

func (c config) anonymous() bool {
	return c.creds.AccessKeyID == ""
}

// parseConfig reads the keys every service reads, and returns the service
// the step calls with what it gives it.
func parseConfig(with map[string]any, getenv func(string) string) (config, string, map[string]any, error) {
	c := config{timeout: DefaultTimeout}
	for k := range with {
		if !slices.Contains(params, k) {
			return c, "", nil, fmt.Errorf("aws action takes %s, not %s", strings.Join(params, ", "), k)
		}
	}

	var named []string
	for _, s := range services {
		if _, ok := with[s]; ok {
			named = append(named, s)
		}
	}
	switch len(named) {
	case 0:
		return c, "", nil, fmt.Errorf("aws action requires exactly one of with.%s", strings.Join(services, ", with."))
	case 1:
	default:
		return c, "", nil, fmt.Errorf("aws action takes exactly one of %s, not %s together", strings.Join(services, ", "), strings.Join(named, " and "))
	}
	service := named[0]
	given, ok := with[service].(map[string]any)
	if !ok {
		return c, "", nil, fmt.Errorf("with.%s must be an object of what to do, not %T", service, with[service])
	}

	var err error
	if c.region, err = optionalString(with, "region", "with.region"); err != nil {
		return c, "", nil, err
	}
	if c.region == "" {
		c.region = getenv("AWS_REGION")
	}
	if c.region == "" {
		c.region = getenv("AWS_DEFAULT_REGION")
	}
	if c.region != "" && !regionName.MatchString(c.region) {
		return c, "", nil, fmt.Errorf("with.region must be the name of a region, such as us-east-1, not %q", c.region)
	}

	raw, err := optionalString(with, "endpoint", "with.endpoint")
	if err != nil {
		return c, "", nil, err
	}
	if raw != "" {
		u, err := url.Parse(raw)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
			return c, "", nil, fmt.Errorf("with.endpoint must be an http or https URL without a query, not %q", raw)
		}
		u.Path = strings.TrimRight(u.Path, "/")
		u.RawPath = ""
		c.endpoint = u
	}

	if c.creds, err = parseCredentials(with, getenv); err != nil {
		return c, "", nil, err
	}

	if v, ok := with["timeout"]; ok {
		if c.timeout, err = parseTimeout(v); err != nil {
			return c, "", nil, err
		}
	}
	return c, service, given, nil
}

// parseCredentials reads the credentials from with, or, when with has none
// of them, from the environment variables the AWS tools read. Without any,
// it returns none, for a service that takes a request unsigned.
func parseCredentials(with map[string]any, getenv func(string) string) (sigv4.Credentials, error) {
	var c sigv4.Credentials
	var err error
	if c.AccessKeyID, err = optionalString(with, "access_key_id", "with.access_key_id"); err != nil {
		return c, err
	}
	if c.SecretAccessKey, err = optionalString(with, "secret_access_key", "with.secret_access_key"); err != nil {
		return c, err
	}
	if c.SessionToken, err = optionalString(with, "session_token", "with.session_token"); err != nil {
		return c, err
	}
	if c == (sigv4.Credentials{}) {
		return sigv4.FromEnv(getenv)
	}
	if c.AccessKeyID == "" || c.SecretAccessKey == "" {
		return c, errors.New("with.access_key_id and with.secret_access_key must be given together")
	}
	return c, nil
}

// requireCredentials returns an error for a service that takes no request
// unsigned when the step has no credentials.
func requireCredentials(c config, service string) error {
	if c.anonymous() {
		return fmt.Errorf("%s needs credentials: give with.access_key_id and with.secret_access_key, or set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY", service)
	}
	return nil
}

// optionalString returns m[key] when it is a string, and an empty string
// when it is not given. name is how an error calls it.
func optionalString(m map[string]any, key, name string) (string, error) {
	switch v := m[key].(type) {
	case nil:
		return "", nil
	case string:
		return v, nil
	default:
		return "", fmt.Errorf("%s must be a string, not %T", name, v)
	}
}

// parseTimeout accepts a duration string such as "10s" or a number of
// seconds, as the http action does.
func parseTimeout(v any) (time.Duration, error) {
	switch t := v.(type) {
	case string:
		d, err := time.ParseDuration(t)
		if err != nil {
			return 0, fmt.Errorf("with.timeout: %w", err)
		}
		return d, nil
	case int:
		return time.Duration(t) * time.Second, nil
	case int64:
		return time.Duration(t) * time.Second, nil
	case float64:
		return time.Duration(t * float64(time.Second)), nil
	default:
		return 0, fmt.Errorf("with.timeout must be a duration or a number of seconds, not %T", v)
	}
}
