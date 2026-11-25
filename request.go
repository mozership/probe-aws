package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/mozership/probe/sdk"
)

// requestMethods are the methods with.request takes, each also as a key
// that holds the URL, as get: https://... does.
var requestMethods = []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions}

// readMethods are the methods run under read-only.
var readMethods = []string{http.MethodGet, http.MethodHead, http.MethodOptions}

var requestParams = []string{"service", "method", "url", "headers", "body", "get", "head", "post", "put", "patch", "delete", "options"}

type apiRequest struct {
	service string
	method  string
	url     *url.URL
	headers map[string]string
	body    []byte
	written any
	region  string
}

// runRequest sends the request in with.request, signed for its service, to
// any API of AWS sent over HTTP, and returns what it answered.
func (a *Action) runRequest(cfg config, given map[string]any, guard sdk.Guard) (map[string]any, error) {
	r, err := parseAPIRequest(cfg, given)
	if err != nil {
		return nil, err
	}
	if guard.ReadOnly && !slices.Contains(readMethods, r.method) {
		return nil, sdk.Refuse("request %s may change what the service holds, and the run is read-only; only %s are run", r.method, strings.Join(readMethods, ", "))
	}
	if err := requireCredentials(cfg, "request"); err != nil {
		return nil, err
	}

	rep, err := a.send(signed{
		method:  r.method,
		url:     r.url.String(),
		headers: r.headers,
		body:    r.body,
		service: r.service,
		region:  r.region,
		creds:   cfg.creds,
	}, cfg.timeout, guard, "request "+r.method)
	if err != nil {
		return nil, err
	}

	body, binary := text(rep.body)
	return map[string]any{
		"req": map[string]any{
			"service": r.service,
			"region":  r.region,
			"method":  r.method,
			"url":     r.url.String(),
			"headers": r.headers,
			"body":    r.written,
			"signed":  true,
		},
		"res": map[string]any{
			"code":    rep.code,
			"status":  rep.status,
			"headers": flattenHeaders(rep.headers),
			"body":    body,
			"binary":  binary,
		},
		"rt":     rep.rt.String(),
		"status": statusOf(rep.code),
	}, nil
}

func parseAPIRequest(cfg config, with map[string]any) (*apiRequest, error) {
	for k := range with {
		if !slices.Contains(requestParams, k) {
			return nil, fmt.Errorf("with.request takes %s, not %s", strings.Join(requestParams, ", "), k)
		}
	}
	r := &apiRequest{headers: map[string]string{}}

	var err error
	if r.service, err = optionalString(with, "service", "with.request.service"); err != nil {
		return nil, err
	}
	if r.service == "" {
		return nil, errors.New("with.request requires service: the name of the service the endpoint signs for, such as sqs, execute-api or lambda")
	}
	r.region = cfg.region
	if r.region == "" {
		return nil, errors.New("request needs a region: give with.region, or set AWS_REGION")
	}

	method, raw, err := methodAndURL(with)
	if err != nil {
		return nil, err
	}
	r.method = method
	if r.url, err = resolveURL(cfg, raw); err != nil {
		return nil, err
	}

	if v, ok := with["headers"]; ok && v != nil {
		h, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("with.request.headers must be an object, not %T", v)
		}
		for k, v := range h {
			if strings.EqualFold(k, "authorization") {
				return nil, errors.New("with.request.headers cannot have authorization, which the signature is sent in")
			}
			r.headers[k] = fmt.Sprint(v)
		}
	}

	if body, ok := with["body"]; ok && body != nil {
		r.written = body
		if s, ok := body.(string); ok {
			r.body = []byte(s)
		} else {
			if r.body, err = json.Marshal(body); err != nil {
				return nil, fmt.Errorf("with.request.body cannot be sent as JSON: %w", err)
			}
			if !hasHeader(r.headers, "content-type") {
				r.headers["Content-Type"] = "application/json"
			}
		}
	}
	return r, nil
}

// methodAndURL reads the method and the URL: method and url, or one key
// named after the method that holds the URL.
func methodAndURL(with map[string]any) (string, string, error) {
	var named []string
	for _, m := range requestMethods {
		if _, ok := with[strings.ToLower(m)]; ok {
			named = append(named, strings.ToLower(m))
		}
	}
	if len(named) > 1 {
		return "", "", fmt.Errorf("with.request takes one method, not %s together", strings.Join(named, " and "))
	}
	if len(named) == 1 {
		if _, ok := with["method"]; ok {
			return "", "", fmt.Errorf("with.request takes %s or method, not both", named[0])
		}
		if _, ok := with["url"]; ok {
			return "", "", fmt.Errorf("with.request.%s holds the URL, so url cannot be given with it", named[0])
		}
		raw, err := optionalString(with, named[0], "with.request."+named[0])
		return strings.ToUpper(named[0]), raw, err
	}

	method, err := optionalString(with, "method", "with.request.method")
	if err != nil {
		return "", "", err
	}
	method = strings.ToUpper(method)
	if !slices.Contains(requestMethods, method) {
		return "", "", fmt.Errorf("with.request requires a method, as method or as a key such as get or post that holds the URL; %q is not one of %s", method, strings.Join(requestMethods, ", "))
	}
	raw, err := optionalString(with, "url", "with.request.url")
	return method, raw, err
}

// resolveURL reads the URL of the request: a URL in full, or a path, which
// is taken under with.endpoint.
func resolveURL(cfg config, raw string) (*url.URL, error) {
	if raw == "" {
		return nil, errors.New("with.request requires the URL to send to")
	}
	if strings.HasPrefix(raw, "/") {
		if cfg.endpoint == nil {
			return nil, fmt.Errorf("with.request has the path %s, which needs with.endpoint to be sent to", raw)
		}
		return url.Parse(cfg.endpoint.String() + raw)
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
		return nil, fmt.Errorf("with.request needs an http or https URL, not %q", raw)
	}
	return u, nil
}

func hasHeader(h map[string]string, name string) bool {
	return slices.ContainsFunc(slices.Collect(maps.Keys(h)), func(k string) bool { return strings.EqualFold(k, name) })
}
