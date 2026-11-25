package main

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mozership/probe/sdk"
)

func apiServer(t *testing.T, service string, sent *[]*http.Request) string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		verify(t, r, body, "eu-west-1", service)
		*sent = append(*sent, r)
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		_, _ = w.Write([]byte(`{"echo":` + strings.TrimSpace(cmpOr(string(body), "null")) + `}`))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func cmpOr(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

func requestWith(endpoint string, request map[string]any) map[string]any {
	return map[string]any{
		"region":            "eu-west-1",
		"endpoint":          endpoint,
		"access_key_id":     testCreds.AccessKeyID,
		"secret_access_key": testCreds.SecretAccessKey,
		"request":           request,
	}
}

func TestRequest(t *testing.T) {
	var sent []*http.Request
	endpoint := apiServer(t, "lambda", &sent)

	ret, err := action().Run(requestWith(endpoint, map[string]any{
		"service": "lambda",
		"post":    "/2015-03-31/functions/f/invocations?Qualifier=live",
		"headers": map[string]any{"X-Amz-Invocation-Type": "RequestResponse"},
		"body":    map[string]any{"n": 1},
	}))
	if err != nil {
		t.Fatal(err)
	}
	res := ret["res"].(map[string]any)
	if res["code"] != 200 || res["body"] != `{"echo":{"n":1}}` || ret["status"] != 0 {
		t.Errorf("res = %v", res)
	}
	r := sent[0]
	if r.Method != http.MethodPost || r.URL.Path != "/2015-03-31/functions/f/invocations" || r.URL.Query().Get("Qualifier") != "live" {
		t.Errorf("sent %s %s", r.Method, r.URL)
	}
	if r.Header.Get("Content-Type") != "application/json" || r.Header.Get("X-Amz-Invocation-Type") != "RequestResponse" {
		t.Errorf("headers = %v", r.Header)
	}
	if req := ret["req"].(map[string]any); req["method"] != "POST" || req["service"] != "lambda" || req["signed"] != true {
		t.Errorf("req = %v", req)
	}

	// method and url, a URL in full, and a body given as text with its own
	// content type.
	if _, err := action().Run(requestWith("", map[string]any{
		"service": "lambda",
		"method":  "put",
		"url":     endpoint + "/x",
		"headers": map[string]any{"content-type": "text/plain"},
		"body":    "hello",
	})); err != nil {
		t.Fatal(err)
	}
	if r := sent[1]; r.Method != http.MethodPut || r.Header.Get("Content-Type") != "text/plain" {
		t.Errorf("sent %s with %v", r.Method, r.Header)
	}
}

func TestRequestErrorIsAResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"denied"}`))
	}))
	defer srv.Close()
	ret, err := action().Run(requestWith(srv.URL, map[string]any{"service": "execute-api", "get": "/"}))
	if err != nil {
		t.Fatal(err)
	}
	if res := ret["res"].(map[string]any); ret["status"] != 1 || res["code"] != 403 {
		t.Errorf("ret = %v", ret)
	}
}

func TestRequestParseErrors(t *testing.T) {
	tests := []struct {
		name    string
		request map[string]any
		region  any
		want    string
	}{
		{"no service", map[string]any{"get": "/"}, "eu-west-1", "with.request requires service"},
		{"no region", map[string]any{"service": "sqs", "get": "/"}, nil, "request needs a region"},
		{"no method", map[string]any{"service": "sqs", "url": "/"}, "eu-west-1", "with.request requires a method"},
		{"two methods", map[string]any{"service": "sqs", "get": "/", "post": "/"}, "eu-west-1", "not get and post together"},
		{"method and a shorthand", map[string]any{"service": "sqs", "get": "/", "method": "GET"}, "eu-west-1", "get or method, not both"},
		{"url and a shorthand", map[string]any{"service": "sqs", "get": "/", "url": "/"}, "eu-west-1", "url cannot be given with it"},
		{"no URL", map[string]any{"service": "sqs", "get": ""}, "eu-west-1", "requires the URL"},
		{"not a URL", map[string]any{"service": "sqs", "get": "ftp://x/"}, "eu-west-1", "needs an http or https URL"},
		{"authorization", map[string]any{"service": "sqs", "get": "/", "headers": map[string]any{"Authorization": "x"}}, "eu-west-1", "cannot have authorization"},
		{"unknown key", map[string]any{"service": "sqs", "get": "/", "query": "a=1"}, "eu-west-1", "not query"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sent []*http.Request
			endpoint := apiServer(t, "sqs", &sent)
			w := requestWith(endpoint, tt.request)
			if tt.region == nil {
				delete(w, "region")
			}
			_, err := action().Run(w)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err = %v, want one containing %q", err, tt.want)
			}
			if len(sent) != 0 {
				t.Error("a request was sent")
			}
		})
	}

	if _, err := action().Run(requestWith("", map[string]any{"service": "sqs", "get": "/"})); err == nil || !strings.Contains(err.Error(), "needs with.endpoint") {
		t.Errorf("a path without an endpoint error = %v", err)
	}
}

func TestRequestReadOnlyAndAllowHost(t *testing.T) {
	var sent []*http.Request
	endpoint := apiServer(t, "execute-api", &sent)
	host := strings.TrimPrefix(endpoint, "http://")

	_, _, err := action().RunStep(sdk.Call{With: requestWith(endpoint, map[string]any{"service": "execute-api", "post": "/"}), Guard: sdk.Guard{ReadOnly: true}})
	if refused := (*sdk.Refused)(nil); !errors.As(err, &refused) {
		t.Errorf("a POST under read-only error = %v, want a refusal", err)
	}
	if _, _, err := action().RunStep(sdk.Call{With: requestWith(endpoint, map[string]any{"service": "execute-api", "get": "/"}), Guard: sdk.Guard{ReadOnly: true}}); err != nil {
		t.Errorf("a GET under read-only error = %v", err)
	}

	_, _, err = action().RunStep(sdk.Call{With: requestWith(endpoint, map[string]any{"service": "execute-api", "get": "/"}), Guard: sdk.Guard{AllowHosts: []string{"api.example.com"}}})
	if refused := (*sdk.Refused)(nil); !errors.As(err, &refused) || !strings.Contains(err.Error(), host) {
		t.Errorf("a host not allowed error = %v, want a refusal naming %s", err, host)
	}
	if len(sent) != 1 {
		t.Errorf("%d requests were sent, want the GET alone", len(sent))
	}
}
