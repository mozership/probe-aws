package main

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"slices"
	"strings"
)

// emptySHA256 is the digest of a request without a body.
const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// canonicalQuery writes a query the way a signature takes it: sorted by
// name and then by value, each percent-encoded.
func canonicalQuery(q url.Values) string {
	var pairs []string
	for name, values := range q {
		for _, v := range values {
			pairs = append(pairs, uriEncode(name, true)+"="+uriEncode(v, true))
		}
	}
	slices.Sort(pairs)
	return strings.Join(pairs, "&")
}

// uriEncode percent-encodes s as a signature expects: every byte but the
// unreserved ones, and a slash too when encodeSlash is set. A key is
// encoded with its slashes kept, as they separate the path.
func uriEncode(s string, encodeSlash bool) string {
	const upper = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(upper[c>>4])
			b.WriteByte(upper[c&15])
		}
	}
	return b.String()
}

func hexSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
