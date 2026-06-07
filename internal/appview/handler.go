package appview

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
	"github.com/vertex-lab/nagg/internal/cache"
	"github.com/vertex-lab/nagg/internal/capabilities"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/vertex"
)

type Store interface {
	FollowsFeed(context.Context, []string, int64, uint64, uint64) ([]chstore.EventView, error)
	QueryEvents(context.Context, chstore.EventQueryInput) ([]chstore.EventView, error)
	NoteStats(context.Context, []string) (map[string]chstore.NoteStats, error)
	LatestProfiles(context.Context, []string) (map[string]chstore.ProfileRow, error)
	FollowCounts(context.Context, string) (chstore.FollowCounts, error)
	ProfileFirstEventCreatedAt(context.Context, string) (*time.Time, error)
	CachedVertexProfile(context.Context, string) (vertex.ProfileResult, bool, error)
	SaveVertexProfile(context.Context, vertex.ProfileResult) error
	ThreadEvents(context.Context, string, int) (*chstore.EventView, []chstore.EventView, error)
	Notifications(context.Context, chstore.NotificationInput) ([]chstore.NotificationRow, error)
}

// RankedFeedProvider runs the shared ranked-feed ranking pipeline. It is
// satisfied by *graphqlapi.Ranker, which reuses the exact same ranking core as
// the GraphQL rankedEvents resolver. The REST handler decodes the request body
// into the same map shape the GraphQL `rankedEvents(input: ...)` field accepts
// and hands it to RankedEventViews, so both transports produce identical
// ranking for identical input. When nil, the ranked-feed route returns 503.
type RankedFeedProvider interface {
	RankedEventViews(context.Context, any) ([]chstore.EventView, error)
}

type Handler struct {
	store                     Store
	vertex                    VertexClient
	userBackfiller            UserFeedBackfiller
	eventBackfiller           EventBackfiller
	profileBackfiller         ProfileBackfiller
	engagementBackfiller      EngagementBackfiller
	threadBackfiller          ThreadBackfiller
	followBackfiller          FollowBackfiller
	dmEnvelopeBackfiller      DMEnvelopeBackfiller
	nip05Validator            *nip05Validator
	rateLimiter               *rateLimiter
	vertexProfileMinFollowers uint64
	viewerPubkey              string
	cache                     cache.Cache
	cacheTTL                  time.Duration
	cacheStaleFor             time.Duration
	ranker                    RankedFeedProvider
}

type VertexClient interface {
	Search(context.Context, vertex.SearchArgs) ([]vertex.SearchResult, bool, error)
	Recommended(context.Context, vertex.RecommendedArgs) ([]vertex.SearchResult, bool, error)
	ProfileRefresh(context.Context, string) (vertex.ProfileResult, error)
}

type Option func(*Handler)

const defaultVertexProfileMinFollowers uint64 = 500

func WithVertex(client VertexClient) Option {
	return func(h *Handler) {
		h.vertex = client
	}
}

func WithVertexProfileMinFollowers(minFollowers int) Option {
	return func(h *Handler) {
		if minFollowers < 0 {
			minFollowers = 0
		}
		h.vertexProfileMinFollowers = uint64(minFollowers)
	}
}

func WithViewerPubkey(pubkey string) Option {
	return func(h *Handler) {
		normalized, err := normalizePubkey(pubkey)
		if err == nil {
			h.viewerPubkey = normalized
		}
	}
}

func WithNIP05Validation(enabled bool) Option {
	return func(h *Handler) {
		h.nip05Validator = newNIP05Validator(enabled)
	}
}

// WithResponseCache enables the shared Redis response cache for the REST
// app-view routes. A disabled cache is a no-op.
func WithResponseCache(c cache.Cache, defaultTTL, staleFor time.Duration) Option {
	return func(h *Handler) {
		h.cache = c
		h.cacheTTL = defaultTTL
		h.cacheStaleFor = staleFor
	}
}

func WithRateLimit(limit int, window time.Duration) Option {
	return func(h *Handler) {
		h.rateLimiter = newRateLimiter(limit, window)
	}
}

// WithRankedFeed wires the shared ranked-feed ranking pipeline so the REST
// /nostr/feed/ranked route can serve the same ranking the GraphQL rankedEvents
// resolver produces. Without it, the route responds 503.
func WithRankedFeed(provider RankedFeedProvider) Option {
	return func(h *Handler) {
		h.ranker = provider
	}
}

func WithUserFeedBackfill(backfiller UserFeedBackfiller) Option {
	return func(h *Handler) {
		h.userBackfiller = backfiller
		h.setOptionalBackfillers(backfiller)
	}
}

func WithAppViewBackfill(backfiller AppViewBackfiller) Option {
	return func(h *Handler) {
		h.userBackfiller = backfiller
		h.eventBackfiller = backfiller
		h.profileBackfiller = backfiller
		h.engagementBackfiller = backfiller
		h.threadBackfiller = backfiller
		h.followBackfiller = backfiller
		if b, ok := any(backfiller).(DMEnvelopeBackfiller); ok {
			h.dmEnvelopeBackfiller = b
		}
	}
}

func New(store Store, opts ...Option) *Handler {
	h := &Handler{
		store:                     store,
		nip05Validator:            newNIP05Validator(true),
		rateLimiter:               newRateLimiter(120, time.Minute),
		vertexProfileMinFollowers: defaultVertexProfileMinFollowers,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

func (h *Handler) setOptionalBackfillers(backfiller any) {
	if b, ok := backfiller.(EventBackfiller); ok {
		h.eventBackfiller = b
	}
	if b, ok := backfiller.(ProfileBackfiller); ok {
		h.profileBackfiller = b
	}
	if b, ok := backfiller.(EngagementBackfiller); ok {
		h.engagementBackfiller = b
	}
	if b, ok := backfiller.(ThreadBackfiller); ok {
		h.threadBackfiller = b
	}
	if b, ok := backfiller.(FollowBackfiller); ok {
		h.followBackfiller = b
	}
	if b, ok := backfiller.(DMEnvelopeBackfiller); ok {
		h.dmEnvelopeBackfiller = b
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	routes := []struct {
		path    string
		handler http.HandlerFunc
	}{
		{"/nostr/capabilities", h.capabilities},
		{"/nostr/feed", h.feed},
		{"/nostr/feed/user", h.userFeed},
		{"/nostr/feed/ranked", h.rankedFeed},
		{"/nostr/notifications", h.notifications},
		{"/nostr/notes/stats", h.noteStats},
		{"/nostr/thread", h.thread},
		{"/nostr/follows", h.follows},
		{"/nostr/events", h.events},
		{"/nostr/dm/envelopes", h.dmEnvelopes},
		{"/nostr/profiles", h.profiles},
		{"/nostr/profile", h.profile},
		{"/nostr/search", h.search},
		{"/nostr/recommended", h.recommended},
	}
	for _, route := range routes {
		wrapped := h.withMiddleware(route.handler)
		mux.HandleFunc(route.path, wrapped)
		// Forward-looking versioned alias; both share the same cache + middleware.
		mux.HandleFunc("/v1"+route.path, wrapped)
	}
}

func (h *Handler) capabilities(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, capabilities.ServiceInfo())
}

func (h *Handler) withMiddleware(next http.HandlerFunc) http.HandlerFunc {
	// Capabilities headers and rate limiting always run; the response cache wraps
	// the underlying handler so hits still carry the standard headers.
	handler := next
	if h.cache != nil && h.cache.Enabled() {
		handler = cache.WrapREST(next, h.cache, h.cacheTTL, h.cacheStaleFor)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		capabilities.WriteHeaders(w)
		if !h.rateLimiter.allow(r) {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		handler(w, r.WithContext(ctx))
	}
}

type FeedEvent struct {
	ID        string     `json:"id"`
	Kind      int        `json:"kind"`
	PubKey    string     `json:"pubkey"`
	Content   string     `json:"content"`
	Tags      [][]string `json:"tags"`
	CreatedAt int64      `json:"created_at"`
}

type ProfileInfo struct {
	Name    string `json:"name"`
	Picture string `json:"picture,omitempty"`
}

type FeedItem struct {
	Type            string     `json:"type"`
	Event           *FeedEvent `json:"event,omitempty"`
	RepostEvent     *FeedEvent `json:"repostEvent,omitempty"`
	OriginalEvent   *FeedEvent `json:"originalEvent,omitempty"`
	OriginalEventID string     `json:"originalEventId,omitempty"`
	RootEvent       *FeedEvent `json:"rootEvent,omitempty"`
	RootEventID     string     `json:"rootEventId,omitempty"`
}

type FeedResponse struct {
	Items            []FeedItem                   `json:"items"`
	Metrics          map[string]chstore.NoteStats `json:"metrics"`
	Profiles         map[string]ProfileInfo       `json:"profiles"`
	Quoted           map[string]FeedEvent         `json:"quoted"`
	PaginationUntil  int64                        `json:"paginationUntil"`
	PaginationOffset int                          `json:"paginationOffset"`
}

// ThreadResponse is the flat REST app-view thread shape: a server-ranked flat
// list of descendant events under `root`, with the same enrichment side maps as
// the feed. It matches the canonical NaggThread so one client parser serves both
// transports.
type ThreadResponse struct {
	Root     FeedEvent                    `json:"root"`
	Events   []FeedEvent                  `json:"events"`
	Metrics  map[string]chstore.NoteStats `json:"metrics"`
	Profiles map[string]ProfileInfo       `json:"profiles"`
	Quoted   map[string]FeedEvent         `json:"quoted"`
}

type EnrichmentResponse struct {
	Metrics  map[string]chstore.NoteStats `json:"metrics"`
	Profiles map[string]ProfileInfo       `json:"profiles"`
	Quoted   map[string]FeedEvent         `json:"quoted"`
}

// PageInfo is the connection cursor envelope shared by the list app-view shapes
// (DM envelopes, notifications). It matches the GraphQL `pageInfo` shape so the
// nagg-ts client parses both transports with one schema. EndCursor is the
// `<RFC3339Nano>|<id>` cursor of the last (oldest) row, or null when empty.
type PageInfo struct {
	HasNextPage bool    `json:"hasNextPage"`
	EndCursor   *string `json:"endCursor"`
}

// eventEndCursor mirrors the GraphQL resolver's cursor format so the REST and
// GraphQL `pageInfo.endCursor` shapes are identical.
func eventEndCursor(events []chstore.EventView) *string {
	if len(events) == 0 {
		return nil
	}
	last := events[len(events)-1]
	cursor := last.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + last.ID
	return &cursor
}

type appViewHydration struct {
	Metrics  map[string]chstore.NoteStats
	Profiles map[string]ProfileInfo
	Quoted   map[string]FeedEvent
}

type resolvedRootEvent struct {
	ID       string
	Event    chstore.EventView
	HasEvent bool
}

var nostrEventURI = regexp.MustCompile(`nostr:(note1|nevent1)[a-z0-9]{1,512}`)

type ProfileFields struct {
	Name        *string `json:"name,omitempty"`
	DisplayName *string `json:"displayName,omitempty"`
	Picture     *string `json:"picture,omitempty"`
	Image       *string `json:"image,omitempty"`
	Banner      *string `json:"banner,omitempty"`
	About       *string `json:"about,omitempty"`
	NIP05       *string `json:"nip05,omitempty"`
	NIP05Valid  *bool   `json:"nip05Valid,omitempty"`
	Website     *string `json:"website,omitempty"`
	LUD16       *string `json:"lud16,omitempty"`
	LUD06       *string `json:"lud06,omitempty"`
}

type SearchResult struct {
	ProfileFields
	PubKey    string   `json:"pubkey"`
	Npub      string   `json:"npub"`
	Rank      *float64 `json:"rank,omitempty"`
	Score     *float64 `json:"score"`
	CreatedAt *int64   `json:"created_at,omitempty"`
}

type TopFollower struct {
	ProfileFields
	PubKey string   `json:"pubkey"`
	Npub   string   `json:"npub"`
	Rank   float64  `json:"rank"`
	Score  *float64 `json:"score"`
}

type ProfileResponse struct {
	ProfileFields
	PubKey       string        `json:"pubkey"`
	Npub         string        `json:"npub"`
	Rank         float64       `json:"rank"`
	Score        *float64      `json:"score"`
	Followers    uint64        `json:"followers"`
	Follows      uint64        `json:"follows"`
	CreatedAt    *int64        `json:"created_at"`
	Nodes        *int          `json:"nodes,omitempty"`
	TopFollowers []TopFollower `json:"topFollowers"`
	FromCache    bool          `json:"fromCache"`
}

func (h *Handler) feed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "GET or POST /nostr/feed only", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Spec       string `json:"spec"`
		Limit      uint64 `json:"limit"`
		Until      int64  `json:"until"`
		Offset     uint64 `json:"offset"`
		UserPubKey string `json:"user_pubkey"`
	}
	if r.Method == http.MethodPost {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		req.Limit = uint64(intParam(r, "limit", 30))
		req.Until = int64(intParam(r, "until", 0))
		req.Offset = uint64(intParam(r, "offset", 0))
	}

	authors := h.authorsFromFeedRequest(req.Spec, req.UserPubKey, r)
	if len(authors) == 0 {
		h.writeFeedResponse(w, r, nil)
		return
	}
	events, err := h.store.FollowsFeed(r.Context(), authors, req.Until, req.Limit, req.Offset)
	if err != nil {
		writeError(w, err)
		return
	}
	if h.shouldBackfillAuthoredFeed(events, authors, req.Until, req.Limit, req.Offset) {
		if h.tryBackfillUserFeeds(r.Context(), takeStrings(authors, 10), req.Limit) {
			events, err = h.store.FollowsFeed(r.Context(), authors, req.Until, req.Limit, req.Offset)
			if err != nil {
				writeError(w, err)
				return
			}
		}
	}
	h.writeFeedResponse(w, r, events)
}

func (h *Handler) authorsFromFeedRequest(spec, userPubkey string, r *http.Request) []string {
	if r.Method == http.MethodGet {
		authors := normalizePubkeys(csv(r.URL.Query().Get("pubkeys")))
		if len(authors) == 0 && h.viewerPubkey != "" {
			authors = []string{h.viewerPubkey}
		}
		return authors
	}
	var parsed struct {
		PubKey  string   `json:"pubkey"`
		PubKeys []string `json:"pubkeys"`
	}
	if err := json.Unmarshal([]byte(spec), &parsed); err != nil {
		return nil
	}
	if len(parsed.PubKeys) > 0 {
		return normalizePubkeys(parsed.PubKeys)
	}
	if parsed.PubKey != "" {
		if pubkey, err := normalizePubkey(parsed.PubKey); err == nil {
			return []string{pubkey}
		}
	}
	if userPubkey != "" {
		if pubkey, err := normalizePubkey(userPubkey); err == nil {
			return []string{pubkey}
		}
	}
	if h.viewerPubkey != "" {
		return []string{h.viewerPubkey}
	}
	return nil
}

func (h *Handler) userFeed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/feed/user only", http.StatusMethodNotAllowed)
		return
	}
	pubkey, err := h.viewerPubkeyOr(r.URL.Query().Get("pubkey"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	until := int64(intParam(r, "until", 0))
	limitParam := intParam(r, "limit", 50)
	if limitParam < 0 {
		limitParam = 0
	}
	offsetParam := intParam(r, "offset", 0)
	if offsetParam < 0 {
		offsetParam = 0
	}
	limit := uint64(limitParam)
	offset := uint64(offsetParam)
	events, err := h.store.FollowsFeed(
		r.Context(),
		[]string{pubkey},
		until,
		limit,
		offset,
	)
	if err != nil {
		writeError(w, err)
		return
	}
	if h.shouldBackfillUserFeed(events, until, limit, offset) {
		if h.tryBackfillUserFeed(r.Context(), pubkey, limit) {
			events, err = h.store.FollowsFeed(r.Context(), []string{pubkey}, until, limit, offset)
			if err != nil {
				writeError(w, err)
				return
			}
		}
	}
	h.writeFeedResponse(w, r, events)
}

func (h *Handler) shouldBackfillUserFeed(events []chstore.EventView, until int64, limit uint64, offset uint64) bool {
	if h.userBackfiller == nil || until != 0 || offset != 0 {
		return false
	}
	if limit == 0 || limit > 100 {
		limit = 50
	}
	return len(events) < int(limit)
}

func (h *Handler) shouldBackfillAuthoredFeed(events []chstore.EventView, authors []string, until int64, limit uint64, offset uint64) bool {
	if h.userBackfiller == nil || len(authors) == 0 || until != 0 || offset != 0 {
		return false
	}
	if limit == 0 || limit > 100 {
		limit = 30
	}
	return len(events) < int(limit)
}

func (h *Handler) writeFeedResponse(w http.ResponseWriter, r *http.Request, events []chstore.EventView) {
	response, err := h.feedResponse(r.Context(), events)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, response)
}

func (h *Handler) feedResponse(ctx context.Context, events []chstore.EventView) (FeedResponse, error) {
	originalIDs := make([]string, 0)
	for _, event := range events {
		if event.Kind == 6 || event.Kind == 16 {
			if id := firstEventTag(event); id != "" {
				originalIDs = append(originalIDs, id)
			}
		}
	}
	originals, err := h.eventsByID(ctx, originalIDs)
	if err != nil {
		return FeedResponse{}, err
	}

	rootSources := make([]chstore.EventView, 0, len(events)+len(originals))
	rootSources = append(rootSources, events...)
	for _, original := range originals {
		rootSources = append(rootSources, original)
	}
	roots, err := h.rootEvents(ctx, rootSources)
	if err != nil {
		return FeedResponse{}, err
	}

	items := make([]FeedItem, 0, len(events))
	hydrationEvents := make([]chstore.EventView, 0, len(events)+len(originals)+len(roots))
	var paginationUntil int64

	for _, event := range events {
		feedEvent := eventJSON(event)
		if paginationUntil == 0 || feedEvent.CreatedAt < paginationUntil {
			paginationUntil = feedEvent.CreatedAt
		}
		hydrationEvents = append(hydrationEvents, event)

		if event.Kind == 6 || event.Kind == 16 {
			originalID := firstEventTag(event)
			item := FeedItem{Type: "repost", RepostEvent: &feedEvent, OriginalEventID: originalID}
			if original, ok := originals[originalID]; ok {
				originalEvent := eventJSON(original)
				item.OriginalEvent = &originalEvent
				hydrationEvents = append(hydrationEvents, original)
				if root, ok := roots[original.ID]; ok {
					item.RootEventID = root.ID
					if root.HasEvent {
						rootEvent := eventJSON(root.Event)
						item.RootEvent = &rootEvent
						hydrationEvents = append(hydrationEvents, root.Event)
					}
				}
			}
			items = append(items, item)
			continue
		}

		item := FeedItem{Type: "note", Event: &feedEvent}
		if root, ok := roots[event.ID]; ok {
			item.RootEventID = root.ID
			if root.HasEvent {
				rootEvent := eventJSON(root.Event)
				item.RootEvent = &rootEvent
				hydrationEvents = append(hydrationEvents, root.Event)
			}
		}
		items = append(items, item)
	}

	hydration, err := h.hydrateAppViewEvents(ctx, hydrationEvents)
	if err != nil {
		return FeedResponse{}, err
	}

	return FeedResponse{
		Items:            items,
		Metrics:          hydration.Metrics,
		Profiles:         hydration.Profiles,
		Quoted:           hydration.Quoted,
		PaginationUntil:  paginationUntil,
		PaginationOffset: len(items),
	}, nil
}

// rankedFeed is the REST counterpart of the GraphQL rankedEvents resolver. It
// decodes the request body into the same input map the GraphQL
// `rankedEvents(input: ...)` field accepts (so Sovran feed recipes produce one
// shape for both transports), runs the shared ranking pipeline via the injected
// RankedFeedProvider, then enriches the ordered events into a FeedResponse using
// the same helpers as /nostr/feed. The ranking order is preserved verbatim.
func (h *Handler) rankedFeed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST /nostr/feed/ranked only", http.StatusMethodNotAllowed)
		return
	}
	if h.ranker == nil {
		http.Error(w, "ranked feed not configured", http.StatusServiceUnavailable)
		return
	}
	var input map[string]any
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	events, err := h.ranker.RankedEventViews(r.Context(), input)
	if err != nil {
		writeError(w, err)
		return
	}
	h.writeFeedResponse(w, r, events)
}

// NotificationRowJSON is the REST shape for a single notification: the event
// plus the ranking metadata the GraphQL notifications resolver exposes.
type NotificationRowJSON struct {
	Event            FeedEvent `json:"event"`
	Reason           string    `json:"reason"`
	ActorVertexScore float64   `json:"actorVertexScore"`
}

// NotificationConnection is the notification rows + cursor, matching the GraphQL
// notifications connection shape ({ nodes, pageInfo }) so one client schema
// parses both transports (the pageInfo synthesis moves server-side here).
type NotificationConnection struct {
	Nodes    []NotificationRowJSON `json:"nodes"`
	PageInfo PageInfo              `json:"pageInfo"`
}

// NotificationsResponse mirrors the feed enrichment (Metrics, Profiles, Quoted)
// so clients can render notification events with the same hydration as the feed.
type NotificationsResponse struct {
	Notifications NotificationConnection       `json:"notifications"`
	Metrics       map[string]chstore.NoteStats `json:"metrics"`
	Profiles      map[string]ProfileInfo       `json:"profiles"`
	Quoted        map[string]FeedEvent         `json:"quoted"`
}

// notifications is the REST counterpart of the GraphQL notifications resolver.
// It builds a chstore.NotificationInput from request params (mirroring the
// GraphQL input: viewer, tab, policy, replyScope, since, until, limit), calls
// store.Notifications, then enriches the notification events with the same
// Metrics/Profiles/Quoted hydration the feed uses.
func (h *Handler) notifications(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "GET or POST /nostr/notifications only", http.StatusMethodNotAllowed)
		return
	}
	input, err := h.parseNotificationRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rows, err := h.store.Notifications(r.Context(), input)
	if err != nil {
		writeError(w, err)
		return
	}

	events := make([]chstore.EventView, 0, len(rows))
	notifications := make([]NotificationRowJSON, 0, len(rows))
	for _, row := range rows {
		events = append(events, row.Event)
		notifications = append(notifications, NotificationRowJSON{
			Event:            eventJSON(row.Event),
			Reason:           row.Reason,
			ActorVertexScore: row.ActorVertexScore,
		})
	}

	hydration, err := h.hydrateAppViewEvents(r.Context(), events)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, NotificationsResponse{
		Notifications: NotificationConnection{
			Nodes:    notifications,
			PageInfo: PageInfo{HasNextPage: len(rows) >= int(input.Limit), EndCursor: eventEndCursor(events)},
		},
		Metrics:  hydration.Metrics,
		Profiles: hydration.Profiles,
		Quoted:   hydration.Quoted,
	})
}

// parseNotificationRequest builds a chstore.NotificationInput from the request,
// accepting both query params (GET) and a JSON body (POST). Defaults match the
// GraphQL parseNotificationInput: tab ALL, policy STRICT, replyScope THREAD,
// limit 50. The viewer pubkey falls back to the configured viewer.
func (h *Handler) parseNotificationRequest(r *http.Request) (chstore.NotificationInput, error) {
	input := chstore.NotificationInput{
		Tab:        "ALL",
		Policy:     "STRICT",
		ReplyScope: "THREAD",
		Limit:      50,
	}

	var raw struct {
		Viewer     string `json:"viewer"`
		Tab        string `json:"tab"`
		Policy     string `json:"policy"`
		ReplyScope string `json:"replyScope"`
		Since      int64  `json:"since"`
		Until      int64  `json:"until"`
		Limit      int    `json:"limit"`
	}
	if r.Method == http.MethodPost {
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			return input, err
		}
	} else {
		q := r.URL.Query()
		raw.Viewer = q.Get("viewer")
		raw.Tab = q.Get("tab")
		raw.Policy = q.Get("policy")
		raw.ReplyScope = q.Get("replyScope")
		raw.Since = int64(intParam(r, "since", 0))
		raw.Until = int64(intParam(r, "until", 0))
		raw.Limit = intParam(r, "limit", 0)
	}

	viewer, err := h.viewerPubkeyOr(raw.Viewer)
	if err != nil {
		return input, fmt.Errorf("notification viewer: %w", err)
	}
	input.Viewer = strings.ToLower(viewer)
	if tab := strings.ToUpper(strings.TrimSpace(raw.Tab)); tab == "ALL" || tab == "MENTIONS" {
		input.Tab = tab
	}
	if policy := strings.ToUpper(strings.TrimSpace(raw.Policy)); policy == "RELAXED" || policy == "MODERATE" || policy == "STRICT" {
		input.Policy = policy
	}
	if replyScope := strings.ToUpper(strings.TrimSpace(raw.ReplyScope)); replyScope == "DIRECT" || replyScope == "THREAD" {
		input.ReplyScope = replyScope
	}
	input.Since = raw.Since
	input.Until = raw.Until
	if raw.Limit > 0 {
		input.Limit = uint64(raw.Limit)
	}
	return input, nil
}

func (h *Handler) noteStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST /nostr/notes/stats only", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		IDs []string `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.IDs) > 100 {
		http.Error(w, "ids max 100", http.StatusBadRequest)
		return
	}
	ids := normalizeHexIDs(req.IDs)
	h.tryBackfillEngagement(r.Context(), ids)
	stats, err := h.store.NoteStats(r.Context(), ids)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, stats)
}

func (h *Handler) thread(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/thread only", http.StatusMethodNotAllowed)
		return
	}
	id, err := normalizeEventID(r.URL.Query().Get("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	limit := intParam(r, "limit", 1000)
	root, events, err := h.store.ThreadEvents(r.Context(), id, limit)
	if errors.Is(err, sql.ErrNoRows) {
		if h.tryBackfillThread(r.Context(), id, limit) {
			root, events, err = h.store.ThreadEvents(r.Context(), id, limit)
		}
	} else if err == nil && h.shouldBackfillThread(events, limit) {
		if h.tryBackfillThread(r.Context(), id, limit) {
			root, events, err = h.store.ThreadEvents(r.Context(), id, limit)
		}
	}
	if err != nil {
		writeError(w, err)
		return
	}
	allEvents := append([]chstore.EventView{*root}, events...)
	hydration, err := h.hydrateAppViewEvents(r.Context(), allEvents)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, ThreadResponse{
		Root:     eventJSON(*root),
		Events:   eventsJSON(events),
		Metrics:  hydration.Metrics,
		Profiles: hydration.Profiles,
		Quoted:   hydration.Quoted,
	})
}

func (h *Handler) follows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/follows only", http.StatusMethodNotAllowed)
		return
	}
	pubkey, err := h.viewerPubkeyOr(r.URL.Query().Get("pubkey"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	h.tryBackfillFollows(r.Context(), pubkey)
	counts, err := h.store.FollowCounts(r.Context(), pubkey)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, map[string]any{
		"pubkey":    pubkey,
		"follows":   counts.Follows,
		"followers": counts.Followers,
	})
}

func (h *Handler) events(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/events only", http.StatusMethodNotAllowed)
		return
	}
	eventsByID, err := h.eventsByID(r.Context(), normalizeHexIDs(csv(r.URL.Query().Get("ids"))))
	if err != nil {
		writeError(w, err)
		return
	}
	events := make([]chstore.EventView, 0, len(eventsByID))
	quoted := make(map[string]FeedEvent, len(eventsByID))
	for id, event := range eventsByID {
		events = append(events, event)
		quoted[id] = eventJSON(event)
	}
	hydration, err := h.hydrateAppViewEvents(r.Context(), events)
	if err != nil {
		writeError(w, err)
		return
	}
	for id, event := range hydration.Quoted {
		quoted[id] = event
	}
	writeJSON(w, EnrichmentResponse{Metrics: hydration.Metrics, Profiles: hydration.Profiles, Quoted: quoted})
}

// DmConnection is the REST app-view DM list. It matches the GraphQL dmEnvelopes
// connection shape ({ nodes, pageInfo }) so the nagg-ts client parses both
// transports with one schema and no per-transport normalize.
type DmConnection struct {
	Nodes    []chstore.EventView `json:"nodes"`
	PageInfo PageInfo            `json:"pageInfo"`
}

// DmEnvelopesResponse wraps the connection under `dmEnvelopes` to byte-match the
// GraphQL `data.dmEnvelopes` shape for the DM/contacts page.
type DmEnvelopesResponse struct {
	DmEnvelopes DmConnection `json:"dmEnvelopes"`
}

// dmEnvelopes is the REST app-view counterpart of the GraphQL dmEnvelopes
// resolver, purpose-built for the contacts/DM page: it returns the viewer's
// recent DM envelopes (gift wraps by default), paginated by `until`. nagg never
// decrypts — the client decrypts and buckets by counterparty. Unions
// author=viewer with p-tag=viewer (the one shape the generic events query can't
// OR), dedupes by id, orders createdAt DESC, truncates to limit.
func (h *Handler) dmEnvelopes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "GET or POST /nostr/dm/envelopes only", http.StatusMethodNotAllowed)
		return
	}
	viewer, err := h.viewerPubkeyOr(r.URL.Query().Get("viewer"))
	if err != nil {
		writeError(w, err)
		return
	}
	kinds := parseDmKinds(r.URL.Query().Get("kinds"))
	limit := clampDmLimit(intParam(r, "limit", 50))
	until := int64(intParam(r, "until", 0))

	h.tryBackfillDMEnvelopes(r.Context(), viewer, kinds, until, uint64(limit))
	authored, err := h.store.QueryEvents(r.Context(), chstore.EventQueryInput{
		PubKeys: []string{viewer}, Kinds: kinds, Until: until, Limit: uint64(limit),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	received, err := h.store.QueryEvents(r.Context(), chstore.EventQueryInput{
		Tags: []chstore.TagFilter{{Key: "p", Value: viewer}}, Kinds: kinds, Until: until, Limit: uint64(limit),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	merged := mergeDmEnvelopes(limit, authored, received)
	writeJSON(w, DmEnvelopesResponse{DmEnvelopes: DmConnection{
		Nodes:    merged,
		PageInfo: PageInfo{HasNextPage: len(merged) >= limit, EndCursor: eventEndCursor(merged)},
	}})
}

// parseDmKinds reads a CSV `kinds` param, defaulting to NIP-04 legacy DMs
// (kind 4) and NIP-17 gift wraps (kind 1059) — matching the GraphQL dmKinds
// default so both transports surface the same conversations.
func parseDmKinds(raw string) []int {
	values := csv(raw)
	kinds := make([]int, 0, len(values))
	for _, v := range values {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			kinds = append(kinds, n)
		}
	}
	if len(kinds) == 0 {
		return []int{4, 1059}
	}
	return kinds
}

func clampDmLimit(n int) int {
	if n <= 0 {
		return 50
	}
	if n > 200 {
		return 200
	}
	return n
}

// mergeDmEnvelopes merges, dedupes by id, orders createdAt DESC (id DESC
// tiebreak), and truncates to limit — mirroring the GraphQL resolver.
func mergeDmEnvelopes(limit int, lists ...[]chstore.EventView) []chstore.EventView {
	seen := make(map[string]struct{})
	merged := make([]chstore.EventView, 0)
	for _, list := range lists {
		for _, ev := range list {
			if _, ok := seen[ev.ID]; ok {
				continue
			}
			seen[ev.ID] = struct{}{}
			merged = append(merged, ev)
		}
	}
	sort.SliceStable(merged, func(i, j int) bool {
		if merged[i].CreatedAt.Equal(merged[j].CreatedAt) {
			return merged[i].ID > merged[j].ID
		}
		return merged[i].CreatedAt.After(merged[j].CreatedAt)
	})
	if limit > 0 && len(merged) > limit {
		merged = merged[:limit]
	}
	return merged
}

func (h *Handler) profiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/profiles only", http.StatusMethodNotAllowed)
		return
	}
	profiles, err := h.profileInfos(r.Context(), normalizePubkeys(csv(r.URL.Query().Get("pubkeys"))))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, EnrichmentResponse{
		Metrics:  map[string]chstore.NoteStats{},
		Profiles: profiles,
		Quoted:   map[string]FeedEvent{},
	})
}

func (h *Handler) profile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/profile only", http.StatusMethodNotAllowed)
		return
	}
	pubkey, err := h.viewerPubkeyOr(r.URL.Query().Get("pubkey"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	h.tryBackfillProfileSummary(ctx, pubkey)
	profiles, err := h.store.LatestProfiles(ctx, []string{pubkey})
	if err != nil {
		writeError(w, err)
		return
	}
	counts, err := h.store.FollowCounts(ctx, pubkey)
	if err != nil {
		writeError(w, err)
		return
	}
	profile := profiles[pubkey]
	createdAt, err := h.localProfileCreatedAt(ctx, pubkey, profile)
	if err != nil {
		writeError(w, err)
		return
	}
	var dvmProfile vertex.ProfileResult
	var fromCache bool
	dvmProfile, fromCache = h.vertexProfile(ctx, pubkey, counts.Followers)
	topFollowers, err := h.enrichTopFollowers(ctx, dvmProfile.TopFollowers)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, ProfileResponse{
		ProfileFields: h.profileFields(ctx, pubkey, profile).fields,
		PubKey:        pubkey,
		Npub:          vertex.Npub(pubkey),
		Rank:          dvmProfile.Rank,
		Score:         dvmProfile.Score,
		Followers:     counts.Followers,
		Follows:       counts.Follows,
		CreatedAt:     createdAt,
		Nodes:         dvmProfile.Nodes,
		TopFollowers:  topFollowers,
		FromCache:     fromCache,
	})
}

func (h *Handler) search(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/search only", http.StatusMethodNotAllowed)
		return
	}
	if h.vertex == nil {
		http.Error(w, "Vertex DVM proxy not configured", http.StatusServiceUnavailable)
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("query"))
	if len(query) < 3 {
		http.Error(w, "query must be at least 3 characters", http.StatusBadRequest)
		return
	}
	limit := intParam(r, "limit", 5)
	sortKey := r.URL.Query().Get("sort")
	results, fromCache, err := h.vertex.Search(r.Context(), vertex.SearchArgs{
		Query: query,
		Limit: limit,
		Sort:  sortKey,
	})
	if err != nil {
		writeVertexError(w, err)
		return
	}
	enriched, err := h.enrichSearchResults(r.Context(), results)
	if err != nil {
		writeError(w, err)
		return
	}
	if sortKey == "" {
		sortKey = "globalPagerank"
	}
	writeJSON(w, map[string]any{
		"query":     query,
		"limit":     limitClamp(limit, 5),
		"sort":      sortKey,
		"results":   enriched,
		"fromCache": fromCache,
	})
}

func (h *Handler) recommended(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/recommended only", http.StatusMethodNotAllowed)
		return
	}
	if h.vertex == nil {
		http.Error(w, "Vertex DVM proxy not configured", http.StatusServiceUnavailable)
		return
	}
	source := strings.TrimSpace(r.URL.Query().Get("source"))
	limit := intParam(r, "limit", 5)
	sortKey := r.URL.Query().Get("sort")
	if sortKey == "" {
		sortKey = "globalPagerank"
	}
	results, fromCache, err := h.vertex.Recommended(r.Context(), vertex.RecommendedArgs{
		Source: source,
		Limit:  limit,
		Sort:   sortKey,
	})
	if err != nil {
		writeVertexError(w, err)
		return
	}
	enriched, err := h.enrichSearchResults(r.Context(), results)
	if err != nil {
		writeError(w, err)
		return
	}
	if source == "" {
		source = "default"
	}
	writeJSON(w, map[string]any{
		"source":    source,
		"limit":     limitClamp(limit, 5),
		"sort":      sortKey,
		"results":   enriched,
		"fromCache": fromCache,
	})
}

func (h *Handler) eventsByID(ctx context.Context, ids []string) (map[string]chstore.EventView, error) {
	ids = normalizeHexIDs(ids)
	out := make(map[string]chstore.EventView, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	events, err := h.store.QueryEvents(ctx, chstore.EventQueryInput{IDs: ids, Limit: uint64(len(ids))})
	if err != nil {
		return nil, err
	}
	for _, event := range events {
		out[event.ID] = event
	}
	missing := missingIDs(ids, out)
	if len(missing) > 0 && h.tryBackfillEvents(ctx, missing) {
		events, err = h.store.QueryEvents(ctx, chstore.EventQueryInput{IDs: missing, Limit: uint64(len(missing))})
		if err != nil {
			return nil, err
		}
		for _, event := range events {
			out[event.ID] = event
		}
	}
	return out, nil
}

func (h *Handler) profileInfos(ctx context.Context, pubkeys []string) (map[string]ProfileInfo, error) {
	pubkeys = normalizePubkeys(pubkeys)
	rows, err := h.store.LatestProfiles(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	if missing := missingProfiles(pubkeys, rows); len(missing) > 0 && h.tryBackfillProfiles(ctx, missing) {
		refreshed, err := h.store.LatestProfiles(ctx, missing)
		if err != nil {
			return nil, err
		}
		for pubkey, row := range refreshed {
			rows[pubkey] = row
		}
	}
	out := make(map[string]ProfileInfo, len(rows))
	for pubkey, row := range rows {
		name := row.DisplayName
		if name == "" {
			name = row.Name
		}
		if name == "" && row.Picture == "" {
			continue
		}
		out[pubkey] = ProfileInfo{Name: name, Picture: row.Picture}
	}
	return out, nil
}

func (h *Handler) hydrateAppViewEvents(ctx context.Context, events []chstore.EventView) (appViewHydration, error) {
	metricIDs := make([]string, 0, len(events))
	pubkeys := make([]string, 0, len(events))
	quotedIDs := make([]string, 0)
	seenMetricIDs := map[string]struct{}{}
	seenPubkeys := map[string]struct{}{}
	seenQuotedIDs := map[string]struct{}{}

	for _, event := range events {
		metricIDs = appendUniqueString(metricIDs, seenMetricIDs, event.ID)
		pubkeys = appendUniqueString(pubkeys, seenPubkeys, event.PubKey)
		for _, id := range quotedEventIDs(event) {
			quotedIDs = appendUniqueString(quotedIDs, seenQuotedIDs, id)
		}
	}

	quotedEvents, err := h.eventsByID(ctx, quotedIDs)
	if err != nil {
		return appViewHydration{}, err
	}
	quoted := make(map[string]FeedEvent, len(quotedEvents))
	for id, event := range quotedEvents {
		quoted[id] = eventJSON(event)
		metricIDs = appendUniqueString(metricIDs, seenMetricIDs, id)
		pubkeys = appendUniqueString(pubkeys, seenPubkeys, event.PubKey)
	}

	h.tryBackfillEnrichment(ctx, metricIDs, pubkeys)
	metrics, err := h.store.NoteStats(ctx, metricIDs)
	if err != nil {
		return appViewHydration{}, err
	}
	profiles, err := h.profileInfos(ctx, pubkeys)
	if err != nil {
		return appViewHydration{}, err
	}
	return appViewHydration{Metrics: metrics, Profiles: profiles, Quoted: quoted}, nil
}

func (h *Handler) rootEvents(ctx context.Context, events []chstore.EventView) (map[string]resolvedRootEvent, error) {
	candidates := make(map[string]string, len(events))
	paths := make(map[string]map[string]struct{}, len(events))
	pending := make([]string, 0, len(events))
	seenPending := map[string]struct{}{}

	for _, event := range events {
		rootID := rootEventID(event)
		if rootID == "" {
			continue
		}
		candidates[event.ID] = rootID
		paths[event.ID] = map[string]struct{}{rootID: {}}
		pending = appendUniqueString(pending, seenPending, rootID)
	}
	if len(candidates) == 0 {
		return map[string]resolvedRootEvent{}, nil
	}

	fetched := make(map[string]chstore.EventView, len(pending))
	for depth := 0; depth < 8 && len(pending) > 0; depth++ {
		batch := pending
		pending = nil
		seenPending = map[string]struct{}{}

		toFetch := make([]string, 0, len(batch))
		seenFetch := map[string]struct{}{}
		for _, id := range batch {
			if _, ok := fetched[id]; ok {
				continue
			}
			toFetch = appendUniqueString(toFetch, seenFetch, id)
		}
		if len(toFetch) == 0 {
			break
		}

		eventsByID, err := h.eventsByID(ctx, toFetch)
		if err != nil {
			return nil, err
		}
		for id, event := range eventsByID {
			fetched[id] = event
		}

		for _, source := range events {
			currentID := candidates[source.ID]
			if currentID == "" {
				continue
			}
			current, ok := fetched[currentID]
			if !ok {
				continue
			}
			nextID := rootEventID(current)
			if nextID == "" || nextID == source.ID || nextID == currentID {
				continue
			}
			path := paths[source.ID]
			if _, seen := path[nextID]; seen {
				continue
			}
			path[nextID] = struct{}{}
			candidates[source.ID] = nextID
			pending = appendUniqueString(pending, seenPending, nextID)
		}
	}

	out := make(map[string]resolvedRootEvent, len(candidates))
	for sourceID, rootID := range candidates {
		root := resolvedRootEvent{ID: rootID}
		if event, ok := fetched[rootID]; ok {
			root.Event = event
			root.HasEvent = true
		}
		out[sourceID] = root
	}
	return out, nil
}

func appendUniqueString(values []string, seen map[string]struct{}, value string) []string {
	if value == "" {
		return values
	}
	if _, ok := seen[value]; ok {
		return values
	}
	seen[value] = struct{}{}
	return append(values, value)
}

func (h *Handler) shouldBackfillThread(events []chstore.EventView, limit int) bool {
	if h.threadBackfiller == nil {
		return false
	}
	if limit <= 0 || limit > 2000 {
		limit = 1000
	}
	return len(events)+1 < limit
}

func (h *Handler) tryBackfillUserFeed(ctx context.Context, pubkey string, limit uint64) bool {
	if h.userBackfiller == nil {
		return false
	}
	if hydrator, ok := h.userBackfiller.(UserFeedHydrator); ok {
		completed, err := hydrator.HydrateUserFeed(ctx, pubkey, limit)
		if err != nil {
			slog.Warn("user feed hydration failed", "pubkey", pubkey, "error", err)
			return false
		}
		return completed
	}
	if err := h.userBackfiller.BackfillUserFeed(ctx, pubkey, limit); err != nil {
		slog.Warn("user feed backfill failed", "pubkey", pubkey, "error", err)
		return false
	}
	return true
}

func (h *Handler) tryBackfillUserFeeds(ctx context.Context, pubkeys []string, limit uint64) bool {
	if h.userBackfiller == nil || len(pubkeys) == 0 {
		return false
	}
	if hydrator, ok := h.userBackfiller.(UserFeedsHydrator); ok {
		completed, err := hydrator.HydrateUserFeeds(ctx, pubkeys, limit)
		if err != nil {
			slog.Warn("user feeds hydration failed", "pubkeys", len(pubkeys), "error", err)
			return false
		}
		return completed
	}
	completed := true
	for _, pubkey := range pubkeys {
		if !h.tryBackfillUserFeed(ctx, pubkey, limit) {
			completed = false
		}
	}
	return completed
}

func (h *Handler) tryBackfillEvents(ctx context.Context, ids []string) bool {
	if h.eventBackfiller == nil || len(ids) == 0 {
		return false
	}
	if hydrator, ok := h.eventBackfiller.(EventHydrator); ok {
		completed, err := hydrator.HydrateEvents(ctx, ids)
		if err != nil {
			slog.Warn("event hydration failed", "ids", len(ids), "error", err)
			return false
		}
		return completed
	}
	if err := h.eventBackfiller.BackfillEvents(ctx, ids); err != nil {
		slog.Warn("event backfill failed", "ids", len(ids), "error", err)
		return false
	}
	return true
}

func (h *Handler) tryBackfillProfiles(ctx context.Context, pubkeys []string) bool {
	if h.profileBackfiller == nil || len(pubkeys) == 0 {
		return false
	}
	if hydrator, ok := h.profileBackfiller.(ProfileHydrator); ok {
		completed, err := hydrator.HydrateProfiles(ctx, pubkeys)
		if err != nil {
			slog.Warn("profile hydration failed", "pubkeys", len(pubkeys), "error", err)
			return false
		}
		return completed
	}
	if err := h.profileBackfiller.BackfillProfiles(ctx, pubkeys); err != nil {
		slog.Warn("profile backfill failed", "pubkeys", len(pubkeys), "error", err)
		return false
	}
	return true
}

func (h *Handler) tryBackfillEngagement(ctx context.Context, ids []string) bool {
	if h.engagementBackfiller == nil || len(ids) == 0 {
		return false
	}
	if hydrator, ok := h.engagementBackfiller.(EngagementHydrator); ok {
		completed, err := hydrator.HydrateEngagement(ctx, ids)
		if err != nil {
			slog.Warn("engagement hydration failed", "ids", len(ids), "error", err)
			return false
		}
		return completed
	}
	if err := h.engagementBackfiller.BackfillEngagement(ctx, ids); err != nil {
		slog.Warn("engagement backfill failed", "ids", len(ids), "error", err)
		return false
	}
	return true
}

func (h *Handler) tryBackfillThread(ctx context.Context, id string, limit int) bool {
	if h.threadBackfiller == nil {
		return false
	}
	if hydrator, ok := h.threadBackfiller.(ThreadHydrator); ok {
		completed, err := hydrator.HydrateThread(ctx, id, limit)
		if err != nil {
			slog.Warn("thread hydration failed", "id", id, "error", err)
			return false
		}
		return completed
	}
	if err := h.threadBackfiller.BackfillThread(ctx, id, limit); err != nil {
		slog.Warn("thread backfill failed", "id", id, "error", err)
		return false
	}
	return true
}

func (h *Handler) tryBackfillFollows(ctx context.Context, pubkey string) bool {
	if h.followBackfiller == nil {
		return false
	}
	if hydrator, ok := h.followBackfiller.(FollowHydrator); ok {
		completed, err := hydrator.HydrateFollows(ctx, pubkey)
		if err != nil {
			slog.Warn("follow graph hydration failed", "pubkey", pubkey, "error", err)
			return false
		}
		return completed
	}
	if err := h.followBackfiller.BackfillFollows(ctx, pubkey); err != nil {
		slog.Warn("follow graph backfill failed", "pubkey", pubkey, "error", err)
		return false
	}
	return true
}

func (h *Handler) tryBackfillDMEnvelopes(ctx context.Context, pubkey string, kinds []int, until int64, limit uint64) bool {
	if h.dmEnvelopeBackfiller == nil || pubkey == "" {
		return false
	}
	if hydrator, ok := h.dmEnvelopeBackfiller.(DMEnvelopeHydrator); ok {
		completed, err := hydrator.HydrateDMEnvelopes(ctx, pubkey, kinds, until, limit)
		if err != nil {
			slog.Warn("dm envelope hydration failed", "pubkey", pubkey, "error", err)
			return false
		}
		return completed
	}
	if err := h.dmEnvelopeBackfiller.BackfillDMEnvelopes(ctx, pubkey, kinds, until, limit); err != nil {
		slog.Warn("dm envelope backfill failed", "pubkey", pubkey, "error", err)
		return false
	}
	return true
}

func (h *Handler) tryBackfillEnrichment(ctx context.Context, ids []string, pubkeys []string) bool {
	type result struct {
		completed bool
	}
	tasks := 0
	results := make(chan result, 2)
	if h.engagementBackfiller != nil && len(ids) > 0 {
		tasks++
		go func() {
			results <- result{completed: h.tryBackfillEngagement(ctx, ids)}
		}()
	}
	if h.profileBackfiller != nil && len(pubkeys) > 0 {
		tasks++
		go func() {
			results <- result{completed: h.tryBackfillProfiles(ctx, pubkeys)}
		}()
	}
	completed := true
	for i := 0; i < tasks; i++ {
		if !(<-results).completed {
			completed = false
		}
	}
	return completed
}

func (h *Handler) tryBackfillProfileSummary(ctx context.Context, pubkey string) bool {
	type result struct {
		completed bool
	}
	tasks := 0
	results := make(chan result, 2)
	if h.profileBackfiller != nil {
		tasks++
		go func() {
			results <- result{completed: h.tryBackfillProfiles(ctx, []string{pubkey})}
		}()
	}
	if h.followBackfiller != nil {
		tasks++
		go func() {
			results <- result{completed: h.tryBackfillFollows(ctx, pubkey)}
		}()
	}
	completed := true
	for i := 0; i < tasks; i++ {
		if !(<-results).completed {
			completed = false
		}
	}
	return completed
}

func missingIDs(ids []string, found map[string]chstore.EventView) []string {
	out := make([]string, 0)
	for _, id := range ids {
		if _, ok := found[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

func missingProfiles(pubkeys []string, rows map[string]chstore.ProfileRow) []string {
	out := make([]string, 0)
	for _, pubkey := range pubkeys {
		if row, ok := rows[pubkey]; !ok || row.EventID == "" {
			out = append(out, pubkey)
		}
	}
	return out
}

func takeStrings(values []string, limit int) []string {
	if limit <= 0 || len(values) <= limit {
		return values
	}
	return values[:limit]
}

type profileFieldsResult struct {
	fields   ProfileFields
	conflict bool
}

func (h *Handler) enrichSearchResults(ctx context.Context, rows []vertex.SearchResult) ([]SearchResult, error) {
	pubkeys := make([]string, 0, len(rows))
	for _, row := range rows {
		pubkeys = append(pubkeys, row.PubKey)
	}
	h.tryBackfillProfiles(ctx, pubkeys)
	profiles, err := h.store.LatestProfiles(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	out := make([]SearchResult, 0, len(rows))
	for _, row := range rows {
		fields := h.profileFields(ctx, row.PubKey, profiles[row.PubKey])
		if fields.conflict {
			continue
		}
		createdAt := unixPtr(profiles[row.PubKey].CreatedAt)
		out = append(out, SearchResult{
			ProfileFields: fields.fields,
			PubKey:        row.PubKey,
			Npub:          row.Npub,
			Rank:          row.Rank,
			Score:         row.Score,
			CreatedAt:     createdAt,
		})
	}
	return out, nil
}

func (h *Handler) vertexProfile(ctx context.Context, pubkey string, followers uint64) (vertex.ProfileResult, bool) {
	provider := vertex.NewScoreProvider(h.store, h.vertex, h.vertexProfileMinFollowers)
	profile, ok, err := provider.AuthorProfileWithFollowers(ctx, pubkey, followers)
	if err != nil {
		slog.Warn("vertex profile cache read failed", "pubkey", pubkey, "error", err)
		return vertex.ProfileResult{}, false
	}
	return profile, ok
}

func (h *Handler) localProfileCreatedAt(ctx context.Context, pubkey string, localProfile chstore.ProfileRow) (*int64, error) {
	firstEventAt, err := h.store.ProfileFirstEventCreatedAt(ctx, pubkey)
	if err != nil {
		return nil, err
	}
	if firstEventAt != nil {
		return unixPtr(*firstEventAt), nil
	}
	return unixPtr(localProfile.CreatedAt), nil
}

func (h *Handler) enrichTopFollowers(ctx context.Context, followers []vertex.TopFollower) ([]TopFollower, error) {
	pubkeys := make([]string, 0, len(followers))
	for _, follower := range followers {
		pubkeys = append(pubkeys, follower.PubKey)
	}
	h.tryBackfillProfiles(ctx, pubkeys)
	profiles, err := h.store.LatestProfiles(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	out := make([]TopFollower, 0, len(followers))
	for _, follower := range followers {
		fields := h.profileFields(ctx, follower.PubKey, profiles[follower.PubKey])
		if fields.conflict {
			continue
		}
		out = append(out, TopFollower{
			ProfileFields: fields.fields,
			PubKey:        follower.PubKey,
			Npub:          follower.Npub,
			Rank:          follower.Rank,
			Score:         follower.Score,
		})
	}
	return out, nil
}

func (h *Handler) profileFields(ctx context.Context, pubkey string, row chstore.ProfileRow) profileFieldsResult {
	fields := ProfileFields{
		Name:        stringPtr(row.Name),
		DisplayName: stringPtr(row.DisplayName),
		Picture:     stringPtr(row.Picture),
		Image:       stringPtr(row.Picture),
		Banner:      stringPtr(row.Banner),
		About:       stringPtr(row.About),
		Website:     stringPtr(row.Website),
		LUD16:       stringPtr(row.LUD16),
		LUD06:       stringPtr(row.LUD06),
	}
	if row.NIP05 == "" {
		return profileFieldsResult{fields: fields}
	}
	if h.nip05Validator == nil || !h.nip05Validator.enabled {
		fields.NIP05 = stringPtr(row.NIP05)
		return profileFieldsResult{fields: fields}
	}
	status := h.nip05Validator.validate(ctx, row.NIP05, pubkey)
	fields.NIP05Valid = &status.valid
	if status.conflict {
		return profileFieldsResult{fields: fields, conflict: true}
	}
	fields.NIP05 = stringPtr(row.NIP05)
	return profileFieldsResult{fields: fields}
}

func eventJSON(event chstore.EventView) FeedEvent {
	return FeedEvent{
		ID:        event.ID,
		Kind:      event.Kind,
		PubKey:    event.PubKey,
		Content:   event.Content,
		Tags:      event.Tags,
		CreatedAt: event.CreatedAt.Unix(),
	}
}

func eventsJSON(events []chstore.EventView) []FeedEvent {
	out := make([]FeedEvent, 0, len(events))
	for _, event := range events {
		out = append(out, eventJSON(event))
	}
	return out
}

func firstEventTag(event chstore.EventView) string {
	for _, tag := range event.Tags {
		if len(tag) >= 2 && tag[0] == "e" && len(tag[1]) == 64 {
			return tag[1]
		}
	}
	return ""
}

func rootEventID(event chstore.EventView) string {
	rootID := markedEventTag(event, "root")
	if rootID == "" {
		rootID = firstPositionalEventTag(event)
	}
	if rootID == "" || rootID == event.ID {
		return ""
	}
	return rootID
}

func markedEventTag(event chstore.EventView, marker string) string {
	for _, tag := range event.Tags {
		if len(tag) < 4 || tag[0] != "e" || tag[3] != marker {
			continue
		}
		if id := cleanEventID(tag[1]); id != "" {
			return id
		}
	}
	return ""
}

func firstPositionalEventTag(event chstore.EventView) string {
	for _, tag := range event.Tags {
		if len(tag) < 2 || tag[0] != "e" {
			continue
		}
		marker := ""
		if len(tag) >= 4 {
			marker = tag[3]
		}
		if marker == "mention" {
			continue
		}
		if marker != "" && marker != "root" && marker != "reply" {
			continue
		}
		if id := cleanEventID(tag[1]); id != "" {
			return id
		}
	}
	return ""
}

func quotedEventIDs(event chstore.EventView) []string {
	matches := nostrEventURI.FindAllString(event.Content, -1)
	out := make([]string, 0, len(matches))
	seen := map[string]struct{}{}
	for _, match := range matches {
		id, err := normalizeEventID(strings.TrimPrefix(match, "nostr:"))
		if err != nil {
			continue
		}
		out = appendUniqueString(out, seen, id)
	}
	for _, tag := range event.Tags {
		if len(tag) < 2 || tag[0] != "q" {
			continue
		}
		out = appendUniqueString(out, seen, cleanEventID(tag[1]))
	}
	return out
}

func cleanEventID(value string) string {
	id := strings.ToLower(strings.TrimSpace(value))
	if nostr.IsValid32ByteHex(id) {
		return id
	}
	return ""
}

func normalizePubkey(input string) (string, error) {
	input = strings.TrimSpace(input)
	if nostr.IsValid32ByteHex(input) {
		return input, nil
	}
	prefix, value, err := nip19.Decode(input)
	if err != nil {
		return "", err
	}
	if prefix == "npub" {
		return value.(string), nil
	}
	return "", fmt.Errorf("unsupported pubkey prefix %q", prefix)
}

func (h *Handler) viewerPubkeyOr(input string) (string, error) {
	if strings.TrimSpace(input) == "" && h.viewerPubkey != "" {
		return h.viewerPubkey, nil
	}
	return normalizePubkey(input)
}

func normalizeEventID(input string) (string, error) {
	input = strings.TrimSpace(input)
	if nostr.IsValid32ByteHex(input) {
		return input, nil
	}
	prefix, value, err := nip19.Decode(input)
	if err != nil {
		return "", err
	}
	switch prefix {
	case "note":
		return value.(string), nil
	case "nevent":
		return value.(nostr.EventPointer).ID, nil
	default:
		return "", fmt.Errorf("unsupported event prefix %q", prefix)
	}
}

func normalizePubkeys(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		pubkey, err := normalizePubkey(value)
		if err == nil {
			out = append(out, pubkey)
		}
	}
	return out
}

func normalizeHexIDs(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		id, err := normalizeEventID(value)
		if err != nil {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func csv(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func intParam(r *http.Request, key string, fallback int) int {
	value := strings.TrimSpace(r.URL.Query().Get(key))
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return n
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		status = http.StatusGatewayTimeout
	}
	http.Error(w, err.Error(), status)
}

func writeVertexError(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	if errors.Is(err, vertex.ErrUnavailable) {
		status = http.StatusServiceUnavailable
	} else if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "timed out") {
		status = http.StatusGatewayTimeout
	}
	http.Error(w, err.Error(), status)
}

func stringPtr(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func unixPtr(value time.Time) *int64 {
	if value.IsZero() {
		return nil
	}
	seconds := value.Unix()
	return &seconds
}

func limitClamp(value int, fallback int) int {
	if value <= 0 {
		return fallback
	}
	if value > 100 {
		return 100
	}
	return value
}
