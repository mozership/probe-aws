package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mozership/probe-aws/internal/sigv4"
	"github.com/mozership/probe/sdk"
)

var testCreds = sigv4.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"}

// verify checks the signature of r as AWS does: it signs again the headers
// the request says it signed, at the time it says it was signed, and
// compares the two.
func verify(t *testing.T, r *http.Request, body []byte, region, service string) {
	t.Helper()
	auth := r.Header.Get("Authorization")
	_, rest, ok := strings.Cut(auth, "SignedHeaders=")
	if !ok {
		t.Errorf("Authorization = %q, want a signature", auth)
		return
	}
	signedHeaders, _, _ := strings.Cut(rest, ",")
	at, err := time.Parse("20060102T150405Z", r.Header.Get("X-Amz-Date"))
	if err != nil {
		t.Errorf("X-Amz-Date: %v", err)
		return
	}
	again, _ := http.NewRequest(r.Method, "http://"+r.Host+r.URL.RequestURI(), nil)
	for name := range strings.SplitSeq(signedHeaders, ";") {
		if name != "host" {
			again.Header[http.CanonicalHeaderKey(name)] = r.Header.Values(name)
		}
	}
	sigv4.Sign(again, testCreds, region, service, sigv4.PayloadHash(body), at)
	if got := again.Header.Get("Authorization"); got != auth {
		t.Errorf("Authorization = %q, want %q", auth, got)
	}
}

// queue stands in for SQS, speaking its JSON protocol. It checks the
// signature of each request, and keeps the calls it gets.
type queue struct {
	t        *testing.T
	mu       sync.Mutex
	calls    []sqsCall
	messages []map[string]any
}

type sqsCall struct {
	target string
	body   map[string]any
}

func newQueue(t *testing.T) (*queue, string) {
	q := &queue{t: t}
	srv := httptest.NewServer(q)
	t.Cleanup(srv.Close)
	return q, srv.URL
}

func (q *queue) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	verify(q.t, r, raw, "ap-northeast-1", "sqs")
	if r.Header.Get("Content-Type") != "application/x-amz-json-1.0" {
		q.t.Errorf("Content-Type = %q", r.Header.Get("Content-Type"))
	}
	var body map[string]any
	_ = json.Unmarshal(raw, &body)
	target := strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "AmazonSQS.")

	q.mu.Lock()
	defer q.mu.Unlock()
	q.calls = append(q.calls, sqsCall{target, body})
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	if !strings.HasSuffix(body["QueueUrl"].(string), "/jobs") {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"__type":"com.amazonaws.sqs#QueueDoesNotExist","message":"The specified queue does not exist."}`))
		return
	}
	switch target {
	case "ReceiveMessage":
		out := map[string]any{}
		if len(q.messages) > 0 {
			out["Messages"] = q.messages
			q.messages = nil
		}
		_ = json.NewEncoder(w).Encode(out)
	case "SendMessage":
		_, _ = w.Write([]byte(`{"MessageId":"m-1","MD5OfMessageBody":"abc"}`))
	case "DeleteMessage":
		_, _ = w.Write([]byte(`{}`))
	}
}

func (q *queue) got() []sqsCall {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]sqsCall(nil), q.calls...)
}

func sqsWith(endpoint string, sqs map[string]any) map[string]any {
	return map[string]any{
		"endpoint":          endpoint,
		"access_key_id":     testCreds.AccessKeyID,
		"secret_access_key": testCreds.SecretAccessKey,
		"sqs":               sqs,
	}
}

const queueURL = "https://sqs.ap-northeast-1.amazonaws.com/123456789012/jobs"

func TestSQSReceive(t *testing.T) {
	q, endpoint := newQueue(t)
	q.messages = []map[string]any{
		{"MessageId": "m-1", "ReceiptHandle": "h-1", "MD5OfBody": "x", "Body": `{"job": 42}`,
			"Attributes":        map[string]string{"ApproximateReceiveCount": "1"},
			"MessageAttributes": map[string]any{"kind": map[string]string{"DataType": "String", "StringValue": "export"}}},
		{"MessageId": "m-2", "ReceiptHandle": "h-2", "Body": "plain text"},
	}

	ret, err := action().Run(sqsWith(endpoint, map[string]any{"receive": queueURL, "wait": "20s", "max": 10, "visibility_timeout": 30}))
	if err != nil {
		t.Fatal(err)
	}
	res := ret["res"].(map[string]any)
	if res["code"] != 200 || res["count"] != 2 || ret["status"] != 0 {
		t.Fatalf("res = %v", res)
	}
	first := res["messages"].([]any)[0].(map[string]any)
	if first["body"].(map[string]any)["job"] != float64(42) || first["rawbody"] != `{"job": 42}` || first["receipt_handle"] != "h-1" {
		t.Errorf("first message = %v", first)
	}
	if first["attributes"].(map[string]any)["kind"] != "export" || first["system"].(map[string]any)["ApproximateReceiveCount"] != "1" {
		t.Errorf("first message attributes = %v", first)
	}
	if second := res["messages"].([]any)[1].(map[string]any); second["body"] != "plain text" {
		t.Errorf("second message = %v", second)
	}

	call := q.got()[0]
	if call.target != "ReceiveMessage" || call.body["QueueUrl"] != queueURL || call.body["WaitTimeSeconds"] != float64(20) || call.body["MaxNumberOfMessages"] != float64(10) || call.body["VisibilityTimeout"] != float64(30) {
		t.Errorf("call = %+v", call)
	}
	if req := ret["req"].(map[string]any); req["region"] != "ap-northeast-1" || req["url"] != endpoint+"/" {
		t.Errorf("req = %v", req)
	}
}

func TestSQSReceiveNothing(t *testing.T) {
	_, endpoint := newQueue(t)
	ret, err := action().Run(sqsWith(endpoint, map[string]any{"receive": queueURL}))
	if err != nil {
		t.Fatal(err)
	}
	if res := ret["res"].(map[string]any); res["count"] != 0 || len(res["messages"].([]any)) != 0 {
		t.Errorf("res = %v", res)
	}
}

func TestSQSReceiveAndDelete(t *testing.T) {
	q, endpoint := newQueue(t)
	q.messages = []map[string]any{{"MessageId": "m-1", "ReceiptHandle": "h-1", "Body": "a"}, {"MessageId": "m-2", "ReceiptHandle": "h-2", "Body": "b"}}
	ret, err := action().Run(sqsWith(endpoint, map[string]any{"receive": queueURL, "max": 2, "delete_received": true}))
	if err != nil {
		t.Fatal(err)
	}
	if res := ret["res"].(map[string]any); res["deleted"] != 2 {
		t.Errorf("res = %v", res)
	}
	calls := q.got()
	if len(calls) != 3 || calls[1].target != "DeleteMessage" || calls[1].body["ReceiptHandle"] != "h-1" || calls[2].body["ReceiptHandle"] != "h-2" {
		t.Errorf("calls = %+v", calls)
	}
}

func TestSQSSendAndDelete(t *testing.T) {
	q, endpoint := newQueue(t)
	ret, err := action().Run(sqsWith(endpoint, map[string]any{
		"send": queueURL, "body": map[string]any{"job": 42}, "delay": "5s",
		"group_id": "g", "deduplication_id": "d", "attributes": map[string]any{"kind": "export", "n": 1},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if res := ret["res"].(map[string]any); res["message_id"] != "m-1" || res["md5"] != "abc" {
		t.Errorf("res = %v", res)
	}
	call := q.got()[0]
	if call.target != "SendMessage" || call.body["MessageBody"] != `{"job":42}` || call.body["DelaySeconds"] != float64(5) || call.body["MessageGroupId"] != "g" || call.body["MessageDeduplicationId"] != "d" {
		t.Errorf("call = %+v", call)
	}
	if attrs := call.body["MessageAttributes"].(map[string]any); attrs["n"].(map[string]any)["StringValue"] != "1" {
		t.Errorf("attributes = %v", attrs)
	}

	if _, err := action().Run(sqsWith(endpoint, map[string]any{"delete": queueURL, "receipt_handle": "h-1"})); err != nil {
		t.Fatal(err)
	}
	if call := q.got()[1]; call.target != "DeleteMessage" || call.body["ReceiptHandle"] != "h-1" {
		t.Errorf("call = %+v", call)
	}
}

func TestSQSErrorIsAResult(t *testing.T) {
	_, endpoint := newQueue(t)
	ret, err := action().Run(sqsWith(endpoint, map[string]any{"receive": "https://sqs.ap-northeast-1.amazonaws.com/123456789012/missing"}))
	if err != nil {
		t.Fatal(err)
	}
	res := ret["res"].(map[string]any)
	if ret["status"] != 1 || res["code"] != 400 || res["error_code"] != "QueueDoesNotExist" || res["error_message"] != "The specified queue does not exist." || res["count"] != 0 {
		t.Errorf("ret = %v", ret)
	}
}

func TestSQSRegion(t *testing.T) {
	for _, tt := range []struct {
		with map[string]any
		env  map[string]string
		want string
	}{
		{map[string]any{"sqs": map[string]any{"receive": queueURL}}, nil, "ap-northeast-1"},
		{map[string]any{"sqs": map[string]any{"receive": "https://sqs.cn-north-1.amazonaws.com.cn/1/q"}}, nil, "cn-north-1"},
		{map[string]any{"region": "eu-west-1", "sqs": map[string]any{"receive": queueURL}}, nil, "eu-west-1"},
		{map[string]any{"sqs": map[string]any{"receive": "http://localhost:9324/000000000000/jobs"}}, map[string]string{"AWS_REGION": "us-west-2"}, "us-west-2"},
	} {
		cfg, _, given, err := parseConfig(tt.with, func(k string) string { return tt.env[k] })
		if err != nil {
			t.Fatal(err)
		}
		r, err := parseSQS(cfg, given)
		if err != nil {
			t.Fatal(err)
		}
		if r.region != tt.want {
			t.Errorf("region of %v = %q, want %q", tt.with, r.region, tt.want)
		}
	}

	cfg, _, given, _ := parseConfig(map[string]any{"sqs": map[string]any{"receive": "http://localhost:9324/000000000000/jobs"}}, func(string) string { return "" })
	if _, err := parseSQS(cfg, given); err == nil || !strings.Contains(err.Error(), "sqs needs a region") {
		t.Errorf("parseSQS without a region error = %v", err)
	}
}

func TestSQSParseErrors(t *testing.T) {
	tests := []struct {
		name string
		sqs  map[string]any
		want string
	}{
		{"no operation", map[string]any{"wait": "1s"}, "with.sqs requires exactly one of receive, send, delete"},
		{"two operations", map[string]any{"receive": queueURL, "send": queueURL}, "not receive and send together"},
		{"unknown key", map[string]any{"receive": queueURL, "waits": "1s"}, "not waits"},
		{"key of another operation", map[string]any{"receive": queueURL, "body": "x"}, "with.sqs.body is not for receive"},
		{"not a URL", map[string]any{"receive": "jobs"}, "with.sqs.receive must be the URL of a queue"},
		{"wait too long", map[string]any{"receive": queueURL, "wait": "21s"}, "with.sqs.wait must be whole seconds from 0s to 20s"},
		{"wait not whole", map[string]any{"receive": queueURL, "wait": "1.5s"}, "with.sqs.wait must be whole seconds"},
		{"max too many", map[string]any{"receive": queueURL, "max": 11}, "with.sqs.max must be a whole number from 1 to 10"},
		{"delete_received not a boolean", map[string]any{"receive": queueURL, "delete_received": "yes"}, "with.sqs.delete_received must be true or false"},
		{"send without a body", map[string]any{"send": queueURL}, "with.sqs.send requires body"},
		{"send an empty body", map[string]any{"send": queueURL, "body": ""}, "must not be empty"},
		{"delay too long", map[string]any{"send": queueURL, "body": "x", "delay": "16m"}, "with.sqs.delay must be whole seconds from 0s to 15m0s"},
		{"delete without a handle", map[string]any{"delete": queueURL}, "with.sqs.delete requires receipt_handle"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, endpoint := newQueue(t)
			_, err := action().Run(sqsWith(endpoint, tt.sqs))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err = %v, want one containing %q", err, tt.want)
			}
			if len(q.got()) != 0 {
				t.Error("a request was sent")
			}
		})
	}
}

func TestSQSNeedsCredentials(t *testing.T) {
	q, endpoint := newQueue(t)
	_, err := action().Run(map[string]any{"endpoint": endpoint, "sqs": map[string]any{"receive": queueURL}})
	if err == nil || !strings.Contains(err.Error(), "sqs needs credentials") {
		t.Errorf("err = %v", err)
	}
	if len(q.got()) != 0 {
		t.Error("a request was sent")
	}
}

func TestSQSReadOnly(t *testing.T) {
	q, endpoint := newQueue(t)
	guard := sdk.Guard{ReadOnly: true}
	for _, sqs := range []map[string]any{
		{"receive": queueURL},
		{"receive": queueURL, "visibility_timeout": 30},
		{"receive": queueURL, "visibility_timeout": 0, "delete_received": true},
		{"send": queueURL, "body": "x"},
		{"delete": queueURL, "receipt_handle": "h"},
	} {
		_, _, err := action().RunStep(sdk.Call{With: sqsWith(endpoint, sqs), Guard: guard})
		if refused := (*sdk.Refused)(nil); err == nil || !errors.As(err, &refused) {
			t.Errorf("RunStep(%v) under read-only error = %v, want a refusal", sqs, err)
		}
	}
	if len(q.got()) != 0 {
		t.Fatal("a refused step sent a request")
	}
	if _, _, err := action().RunStep(sdk.Call{With: sqsWith(endpoint, map[string]any{"receive": queueURL, "visibility_timeout": 0}), Guard: guard}); err != nil {
		t.Errorf("a receive that leaves the messages in the queue was refused: %v", err)
	}
}
