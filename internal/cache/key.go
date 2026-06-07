package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"strings"

	"github.com/vertex-lab/nagg/internal/capabilities"
)

// keyVersion lets us evolve the key format independently of the schema version.
// v2 introduced the stale-while-revalidate value envelope (timestamp-prefixed
// body); the bump retires the older plain-body entries with a clean all-miss.
const keyVersion = "v2"

// schemaPrefix embeds the GraphQL schema version so a schema bump automatically
// invalidates every cached entry (a new prefix yields all-misses).
func schemaPrefix() string {
	return keyVersion + ":" + capabilities.GraphQLSchemaVersion
}

// normalizeQuery collapses insignificant whitespace so two semantically
// identical GraphQL documents hash to the same key.
func normalizeQuery(query string) string {
	return strings.Join(strings.Fields(query), " ")
}

// canonicalJSON marshals v deterministically. encoding/json sorts map keys, and
// JSON-decoded GraphQL variables are map[string]any, so the bytes are stable for
// equivalent variable sets while preserving array order (which is significant).
func canonicalJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("null")
	}
	return b
}

// GraphQLKey derives the cache key for a GraphQL request. Because the viewer
// pubkey is passed as a variable, hashing the full variable set already
// segments personalized responses by viewer; the viewer suffix is appended only
// for observability and targeted inspection.
func GraphQLKey(query, operationName string, variables map[string]any, viewer string) string {
	h := sha256.New()
	h.Write([]byte(normalizeQuery(query)))
	h.Write([]byte{0})
	h.Write([]byte(operationName))
	h.Write([]byte{0})
	h.Write(canonicalJSON(variables))
	key := "nagg:gql:" + schemaPrefix() + ":" + hex.EncodeToString(h.Sum(nil))
	if viewer != "" {
		key += ":viewer=" + viewer
	}
	return key
}

// RESTKey derives the cache key for a REST app-view request. The viewer suffix
// is appended only for observability; the pubkey query param is already part of
// the hashed query string.
func RESTKey(method, path, rawQuery, viewer string) string {
	h := sha256.New()
	h.Write([]byte(method))
	h.Write([]byte{0})
	h.Write([]byte(path))
	h.Write([]byte{0})
	h.Write([]byte(normalizeRawQuery(rawQuery)))
	key := "nagg:rest:" + schemaPrefix() + ":" + hex.EncodeToString(h.Sum(nil))
	if viewer != "" {
		key += ":viewer=" + viewer
	}
	return key
}

// normalizeRawQuery sorts query parameters and drops the cache-control "refresh"
// flag so a forced refresh repopulates the same key a normal request reads.
func normalizeRawQuery(raw string) string {
	values, err := url.ParseQuery(raw)
	if err != nil {
		return raw
	}
	values.Del("refresh")
	return values.Encode() // url.Values.Encode sorts by key
}

// findViewer searches GraphQL variables for a viewer pubkey (best-effort, for
// the key suffix only).
func findViewer(variables map[string]any) string {
	for k, val := range variables {
		if strings.EqualFold(k, "viewer") || strings.EqualFold(k, "viewerPubkey") {
			if s, ok := val.(string); ok && isHex64(s) {
				return strings.ToLower(s)
			}
		}
		if nested, ok := val.(map[string]any); ok {
			if found := findViewer(nested); found != "" {
				return found
			}
		}
	}
	return ""
}

func isHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}
