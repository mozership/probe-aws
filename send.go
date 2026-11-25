package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/mozership/probe-aws/internal/sigv4"
	"github.com/mozership/probe/sdk"
)

// signed is a request of a service other than S3, signed and sent once:
// a redirect is the answer, as the request would have to be signed again
// for wherever it leads.
type signed struct {
	method  string
	url     string
	headers map[string]string
	body    []byte
	service string
	region  string
	creds   sigv4.Credentials
}

// reply is what a service answered.
type reply struct {
	code    int
	status  string
	headers http.Header
	body    []byte
	rt      time.Duration
}

func (a *Action) clock() time.Time {
	if a.now != nil {
		return a.now()
	}
	return time.Now()
}

// send signs s and sends it within timeout, to a host the guard allows.
// what names the request in an error, as sqs receive.
func (a *Action) send(s signed, timeout time.Duration, guard sdk.Guard, what string) (*reply, error) {
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, s.method, s.url, bytes.NewReader(s.body))
	if err != nil {
		return nil, err
	}
	if len(s.body) == 0 {
		req.Body = http.NoBody
	}
	for k, v := range s.headers {
		req.Header.Set(k, v)
	}
	sigv4.Sign(req, s.creds, s.region, s.service, sigv4.PayloadHash(s.body), a.clock())
	req.Header.Set("User-Agent", "probe-aws/"+version)

	base := a.transport
	if base == nil {
		base = http.DefaultTransport
	}
	client := &http.Client{
		Transport:     &guarded{base: base, guard: guard},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		if refused := (*sdk.Refused)(nil); errors.As(err, &refused) {
			return nil, refused
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%s timed out after %s", what, timeout)
		}
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxAnswer))
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%s timed out after %s", what, timeout)
		}
		return nil, err
	}
	return &reply{code: resp.StatusCode, status: resp.Status, headers: resp.Header, body: body, rt: time.Since(start)}, nil
}

// text returns body as the result shows it: as it is when it is UTF-8
// text, and empty, with binary set, when it is not.
func text(body []byte) (s string, binary bool) {
	if !utf8.Valid(body) {
		return "", true
	}
	return string(body), false
}

// statusOf is the status of a step that got code: 0 for a 2xx, 1 for any
// other.
func statusOf(code int) int {
	if code >= 200 && code <= 299 {
		return 0
	}
	return 1
}
