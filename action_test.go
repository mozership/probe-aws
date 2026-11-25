package main

import (
	"strings"
	"testing"

	"github.com/mozership/probe-aws/internal/sigv4"
)

func TestParseConfig(t *testing.T) {
	env := map[string]string{"AWS_ACCESS_KEY_ID": "id", "AWS_SECRET_ACCESS_KEY": "secret", "AWS_SESSION_TOKEN": "token", "AWS_DEFAULT_REGION": "eu-west-1"}
	cfg, service, given, err := parseConfig(map[string]any{"sqs": map[string]any{"receive": "x"}, "timeout": "5s"}, func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if service != "sqs" || given["receive"] != "x" || cfg.region != "eu-west-1" || cfg.timeout.String() != "5s" || cfg.creds != (sigv4.Credentials{AccessKeyID: "id", SecretAccessKey: "secret", SessionToken: "token"}) {
		t.Errorf("parseConfig = %+v, %s, %v", cfg, service, given)
	}

	env["AWS_REGION"] = "us-west-2"
	cfg, _, _, _ = parseConfig(map[string]any{"s3": map[string]any{}}, func(k string) string { return env[k] })
	if cfg.region != "us-west-2" {
		t.Errorf("region = %q, want AWS_REGION before AWS_DEFAULT_REGION", cfg.region)
	}

	// with wins over the environment, whole: its token is not taken along.
	cfg, _, _, _ = parseConfig(map[string]any{"s3": map[string]any{}, "region": "ap-south-1", "access_key_id": "a", "secret_access_key": "b"}, func(k string) string { return env[k] })
	if cfg.region != "ap-south-1" || cfg.creds != (sigv4.Credentials{AccessKeyID: "a", SecretAccessKey: "b"}) {
		t.Errorf("parseConfig = %+v", cfg)
	}
}

func TestParseConfigErrors(t *testing.T) {
	tests := []struct {
		name string
		with map[string]any
		want string
	}{
		{"no service", map[string]any{"region": "us-east-1"}, "requires exactly one of with.s3, with.sqs, with.request"},
		{"two services", map[string]any{"s3": map[string]any{}, "sqs": map[string]any{}}, "not s3 and sqs together"},
		{"service not an object", map[string]any{"sqs": "receive"}, "with.sqs must be an object"},
		{"unknown key", map[string]any{"s3": map[string]any{}, "bucket": "b"}, "not bucket"},
		{"region with a dot", map[string]any{"s3": map[string]any{}, "region": "us-east-1.example.com"}, "with.region must be the name of a region"},
		{"endpoint without a scheme", map[string]any{"s3": map[string]any{}, "endpoint": "localhost:9000"}, "with.endpoint must be an http or https URL"},
		{"one credential", map[string]any{"s3": map[string]any{}, "access_key_id": "a"}, "must be given together"},
		{"timeout", map[string]any{"s3": map[string]any{}, "timeout": "soon"}, "with.timeout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, err := parseConfig(tt.with, func(string) string { return "" })
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err = %v, want one containing %q", err, tt.want)
			}
		})
	}
}
