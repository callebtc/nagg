package cache

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"
)

// maxBodyBytes caps how much of a GraphQL request body we buffer for keying.
const maxBodyBytes = 1 << 20 // 1 MiB

// revalidateTimeout bounds a background stale-while-revalidate recompute so a
// slow upstream cannot pin a goroutine indefinitely.
const revalidateTimeout = 30 * time.Second

// Envelope framing: a stored response is a one-byte version, an 8-byte
// big-endian unix-seconds "stored at" timestamp, then the raw response body.
// Carrying the timestamp lets the middleware tell a fresh entry from a stale one
// at read time, which is what makes stale-while-revalidate possible.
const (
	envVersion   = 1
	envHeaderLen = 1 + 8
)

func encodeEnvelope(now time.Time, body []byte) []byte {
	out := make([]byte, envHeaderLen+len(body))
	out[0] = envVersion
	binary.BigEndian.PutUint64(out[1:envHeaderLen], uint64(now.Unix()))
	copy(out[envHeaderLen:], body)
	return out
}

func decodeEnvelope(raw []byte) (storedAt time.Time, body []byte, ok bool) {
	if len(raw) < envHeaderLen || raw[0] != envVersion {
		return time.Time{}, nil, false
	}
	secs := binary.BigEndian.Uint64(raw[1:envHeaderLen])
	return time.Unix(int64(secs), 0), raw[envHeaderLen:], true
}

// capturedResult is the result of computing a response once, shared across
// concurrent identical requests via singleflight.
type capturedResult struct {
	status      int
	contentType string
	body        []byte
}

// capture is an http.ResponseWriter that records the handler's response instead
// of sending it, so the middleware can both cache and forward it.
type capture struct {
	header http.Header
	status int
	wrote  bool
	buf    bytes.Buffer
}

func newCapture() *capture { return &capture{header: make(http.Header), status: http.StatusOK} }

func (c *capture) Header() http.Header { return c.header }

func (c *capture) WriteHeader(status int) {
	if !c.wrote {
		c.status = status
		c.wrote = true
	}
}

func (c *capture) Write(b []byte) (int, error) { return c.buf.Write(b) }

// WrapGraphQL caches successful, error-free POST responses from a GraphQL
// handler. Identical requests within the fresh TTL return the cached bytes; once
// the fresh TTL lapses, a cached response is still served immediately (for up to
// staleFor) while it is revalidated in the background. A refresh hint forces that
// background revalidation instead of bypassing the cache, so pull-to-refresh
// stays fast.
func WrapGraphQL(next http.HandlerFunc, c Cache, defaultTTL, staleFor time.Duration) http.HandlerFunc {
	if c == nil || !c.Enabled() {
		return next
	}
	var group singleflight.Group
	cacheable := func(res capturedResult) bool {
		return res.status == http.StatusOK && !hasGraphQLErrors(res.body)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			next(w, r)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
		_ = r.Body.Close()
		if err != nil {
			r.Body = io.NopCloser(bytes.NewReader(body))
			next(w, r)
			return
		}

		var req struct {
			Query         string         `json:"query"`
			OperationName string         `json:"operationName"`
			Variables     map[string]any `json:"variables"`
		}
		if uerr := json.Unmarshal(body, &req); uerr != nil || req.Query == "" {
			r.Body = io.NopCloser(bytes.NewReader(body))
			next(w, r)
			return
		}

		key := GraphQLKey(req.Query, req.OperationName, req.Variables, findViewer(req.Variables))
		ttl, stale := graphqlCachePolicy(req.Query, defaultTTL, staleFor)
		compute := func(ctx context.Context) capturedResult {
			rec := newCapture()
			rr := r.Clone(ctx)
			rr.Body = io.NopCloser(bytes.NewReader(body))
			next(rec, rr)
			return capturedResult{status: rec.status, contentType: rec.header.Get("Content-Type"), body: rec.buf.Bytes()}
		}
		serveSWR(w, r, c, &group, key, ttl, stale, wantsRefresh(r), compute, cacheable)
	}
}

// WrapREST caches successful GET responses from an app-view handler with the same
// stale-while-revalidate behavior as WrapGraphQL.
func WrapREST(next http.HandlerFunc, c Cache, defaultTTL, staleFor time.Duration) http.HandlerFunc {
	if c == nil || !c.Enabled() {
		return next
	}
	var group singleflight.Group
	cacheable := func(res capturedResult) bool { return res.status == http.StatusOK }
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			next(w, r)
			return
		}
		viewer := ""
		if pk := r.URL.Query().Get("pubkey"); isHex64(pk) {
			viewer = strings.ToLower(pk)
		}
		key := RESTKey(r.Method, r.URL.Path, r.URL.RawQuery, viewer)
		ttl, stale := restCachePolicy(r.URL.Path, defaultTTL, staleFor)
		compute := func(ctx context.Context) capturedResult {
			rec := newCapture()
			next(rec, r.Clone(ctx))
			return capturedResult{status: rec.status, contentType: rec.header.Get("Content-Type"), body: rec.buf.Bytes()}
		}
		serveSWR(w, r, c, &group, key, ttl, stale, wantsRefresh(r), compute, cacheable)
	}
}

// serveSWR is the shared stale-while-revalidate core. A fresh cached entry is
// returned directly; a stale entry (or any entry when refresh is requested) is
// returned immediately and revalidated in the background; a missing entry is
// computed synchronously, collapsing concurrent identical misses via
// singleflight.
func serveSWR(
	w http.ResponseWriter,
	r *http.Request,
	c Cache,
	group *singleflight.Group,
	key string,
	freshTTL, staleFor time.Duration,
	refresh bool,
	compute func(ctx context.Context) capturedResult,
	cacheable func(capturedResult) bool,
) {
	hardTTL := freshTTL + staleFor
	now := time.Now()

	var cachedBody []byte
	var age time.Duration
	haveCached := false
	if raw, ok := c.Get(r.Context(), key); ok {
		if storedAt, decoded, decodedOK := decodeEnvelope(raw); decodedOK {
			cachedBody, age, haveCached = decoded, now.Sub(storedAt), true
		}
	}

	if haveCached && !refresh && age <= freshTTL {
		writeResponse(w, http.StatusOK, "application/json", cachedBody, "hit")
		return
	}

	if haveCached && staleFor > 0 {
		// Serve stale instantly and refresh in the background. This covers both an
		// expired entry and an explicit refresh, so neither blocks the caller.
		writeResponse(w, http.StatusOK, "application/json", cachedBody, "stale")
		go func() {
			_, _, _ = group.Do("revalidate:"+key, func() (any, error) {
				ctx, cancel := context.WithTimeout(context.Background(), revalidateTimeout)
				defer cancel()
				res := compute(ctx)
				if cacheable(res) {
					c.Set(ctx, key, encodeEnvelope(time.Now(), res.body), hardTTL)
				}
				return nil, nil
			})
		}()
		return
	}

	// Nothing cached (or stale serving disabled): compute synchronously,
	// collapsing concurrent identical misses into one computation.
	v, _, _ := group.Do(key, func() (any, error) {
		res := compute(r.Context())
		if cacheable(res) {
			c.Set(r.Context(), key, encodeEnvelope(time.Now(), res.body), hardTTL)
		}
		return res, nil
	})
	res := v.(capturedResult)
	writeResponse(w, res.status, res.contentType, res.body, "miss")
}

func writeResponse(w http.ResponseWriter, status int, contentType string, body []byte, cacheState string) {
	if contentType == "" {
		contentType = "application/json"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Nagg-Cache", cacheState)
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// wantsRefresh reports whether the caller asked to revalidate the cache, via
// ?refresh=1 or a Cache-Control: no-cache header. With stale-while-revalidate
// this triggers a background recompute rather than a blocking bypass.
func wantsRefresh(r *http.Request) bool {
	switch strings.ToLower(r.URL.Query().Get("refresh")) {
	case "1", "true", "yes":
		return true
	}
	return strings.Contains(strings.ToLower(r.Header.Get("Cache-Control")), "no-cache")
}

// hasGraphQLErrors reports whether a GraphQL response body carries a non-empty
// top-level errors array (or is unparseable), in which case it is not cached.
func hasGraphQLErrors(body []byte) bool {
	var resp struct {
		Errors []json.RawMessage `json:"errors"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return true
	}
	return len(resp.Errors) > 0
}

// A cache policy is a fresh TTL plus a stale-while-revalidate window. Within
// fresh, an entry is a direct hit. Past fresh but within stale, the entry is
// served instantly and revalidated in the background. A zero stale window means
// "never serve stale": once fresh lapses the response is recomputed before it is
// returned, so the caller always sees current data. That distinction is what
// lets real-time surfaces (DMs) stay live while tolerant surfaces (feed,
// profiles) stay fast.
//
// Rule of thumb by data freshness need:
//   - real-time (DMs): tiny fresh, zero stale — a refresh always recomputes, so
//     a new message is never hidden behind a stale read.
//   - timely (notifications): short fresh, short stale.
//   - tolerant (feed, threads): short fresh, generous stale.
//   - near-static (profiles, follows, service info): long fresh, long stale.

// graphqlCachePolicy picks (fresh, stale) based on the dominant root field.
func graphqlCachePolicy(query string, defFresh, defStale time.Duration) (fresh, stale time.Duration) {
	switch {
	case strings.Contains(query, "dmEnvelopes"), strings.Contains(query, "dmConversation"):
		return 5 * time.Second, 0
	case strings.Contains(query, "notifications"):
		return 15 * time.Second, 45 * time.Second
	case strings.Contains(query, "followStatus"):
		return 300 * time.Second, 30 * time.Minute
	case strings.Contains(query, "ownProfiles"), strings.Contains(query, "profileSearch"):
		return 120 * time.Second, 10 * time.Minute
	case strings.Contains(query, "serviceInfo"):
		return 600 * time.Second, time.Hour
	case strings.Contains(query, "rankedEvents"), strings.Contains(query, "events"):
		return 20 * time.Second, 5 * time.Minute
	default:
		return defFresh, defStale
	}
}

// restCachePolicy picks (fresh, stale) based on the app-view route path.
func restCachePolicy(path string, defFresh, defStale time.Duration) (fresh, stale time.Duration) {
	switch {
	case strings.Contains(path, "/dm/"):
		return 5 * time.Second, 0
	case strings.HasSuffix(path, "/profile"), strings.HasSuffix(path, "/profiles"),
		strings.HasSuffix(path, "/search"), strings.HasSuffix(path, "/recommended"):
		return 120 * time.Second, 10 * time.Minute
	case strings.HasSuffix(path, "/follows"):
		return 300 * time.Second, 30 * time.Minute
	case strings.HasSuffix(path, "/thread"):
		return 30 * time.Second, 5 * time.Minute
	case strings.Contains(path, "/feed"), strings.HasSuffix(path, "/notes/stats"),
		strings.HasSuffix(path, "/events"):
		return 20 * time.Second, 5 * time.Minute
	default:
		return defFresh, defStale
	}
}
