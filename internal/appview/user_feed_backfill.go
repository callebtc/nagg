package appview

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nbd-wtf/go-nostr"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/relayquery"
)

type UserFeedBackfiller interface {
	BackfillUserFeed(context.Context, string, uint64) error
}

type UserFeedHydrator interface {
	HydrateUserFeed(context.Context, string, uint64) (bool, error)
}

type UserFeedsHydrator interface {
	HydrateUserFeeds(context.Context, []string, uint64) (bool, error)
}

type EventBackfiller interface {
	BackfillEvents(context.Context, []string) error
}

type EventHydrator interface {
	HydrateEvents(context.Context, []string) (bool, error)
}

type ProfileBackfiller interface {
	BackfillProfiles(context.Context, []string) error
}

type ProfileHydrator interface {
	HydrateProfiles(context.Context, []string) (bool, error)
}

type EngagementBackfiller interface {
	BackfillEngagement(context.Context, []string) error
}

type EngagementHydrator interface {
	HydrateEngagement(context.Context, []string) (bool, error)
}

type ThreadBackfiller interface {
	BackfillThread(context.Context, string, int) error
}

type ThreadHydrator interface {
	HydrateThread(context.Context, string, int) (bool, error)
}

type FollowBackfiller interface {
	BackfillFollows(context.Context, string) error
}

type FollowHydrator interface {
	HydrateFollows(context.Context, string) (bool, error)
}

type DMEnvelopeBackfiller interface {
	BackfillDMEnvelopes(context.Context, string, []int, int64, uint64) error
}

type DMEnvelopeHydrator interface {
	HydrateDMEnvelopes(context.Context, string, []int, int64, uint64) (bool, error)
}

type RelayEventBackfiller interface {
	BackfillRelayEvents(context.Context, chstore.EventQueryInput, string) error
}

type RelayEventHydrator interface {
	HydrateRelayEvents(context.Context, chstore.EventQueryInput, string) (bool, error)
}

type AppViewBackfiller interface {
	UserFeedBackfiller
	EventBackfiller
	ProfileBackfiller
	EngagementBackfiller
	ThreadBackfiller
	FollowBackfiller
}

type eventInserter interface {
	InsertEvents(context.Context, []chstore.EventRecord) error
}

type UserFeedBackfillConfig struct {
	Relays          []string
	ReadLimit       int64
	Cooldown        time.Duration
	Timeout         time.Duration
	Wait            time.Duration
	AuthorLimit     int
	EngagementLimit int
	ThreadLimit     int
	FollowLimit     int
	DMLimit         int
	DMBackfillPages int
	GraphQLLimit    int
}

type RelayUserFeedBackfiller struct {
	store eventInserter
	query relayquery.Client
	cfg   UserFeedBackfillConfig

	mu       sync.Mutex
	attempts map[string]time.Time
	jobs     map[string]*hydrationJob
}

func NewRelayUserFeedBackfiller(store eventInserter, cfg UserFeedBackfillConfig) *RelayUserFeedBackfiller {
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 5 * time.Minute
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.AuthorLimit <= 0 {
		cfg.AuthorLimit = 100
	}
	if cfg.EngagementLimit <= 0 {
		cfg.EngagementLimit = 1000
	}
	if cfg.ThreadLimit <= 0 {
		cfg.ThreadLimit = 1000
	}
	if cfg.FollowLimit <= 0 {
		cfg.FollowLimit = 1000
	}
	if cfg.DMLimit <= 0 {
		cfg.DMLimit = 200
	}
	if cfg.DMBackfillPages <= 0 {
		cfg.DMBackfillPages = 2
	}
	if cfg.GraphQLLimit <= 0 {
		cfg.GraphQLLimit = 100
	}
	return &RelayUserFeedBackfiller{
		store: store,
		query: relayquery.Client{
			Relays:    cfg.Relays,
			ReadLimit: cfg.ReadLimit,
			Health:    relayquery.NewRelayHealth(),
		},
		cfg:      cfg,
		attempts: map[string]time.Time{},
		jobs:     map[string]*hydrationJob{},
	}
}

func (b *RelayUserFeedBackfiller) HydrateUserFeed(ctx context.Context, pubkey string, limit uint64) (bool, error) {
	return b.HydrateUserFeeds(ctx, []string{pubkey}, limit)
}

func (b *RelayUserFeedBackfiller) HydrateUserFeeds(ctx context.Context, pubkeys []string, limit uint64) (bool, error) {
	pubkeys = validPubkeys(pubkeys)
	if b == nil || b.store == nil || len(pubkeys) == 0 {
		return true, nil
	}
	jobs := make([]*hydrationJob, 0, len(pubkeys))
	for _, pubkey := range pubkeys {
		pubkey := pubkey
		jobs = append(jobs, b.scheduleJob(ctx, "feed:"+pubkey, func(jobCtx context.Context) error {
			return b.BackfillUserFeed(jobCtx, pubkey, limit)
		}))
	}
	return b.waitJobs(ctx, jobs)
}

func (b *RelayUserFeedBackfiller) HydrateEvents(ctx context.Context, ids []string) (bool, error) {
	ids = validHexIDs(ids)
	if b == nil || b.store == nil || len(ids) == 0 {
		return true, nil
	}
	return b.waitJobs(ctx, []*hydrationJob{b.scheduleJob(ctx, hydrationBatchKey("events", ids), func(jobCtx context.Context) error {
		return b.BackfillEvents(jobCtx, ids)
	})})
}

func (b *RelayUserFeedBackfiller) HydrateProfiles(ctx context.Context, pubkeys []string) (bool, error) {
	pubkeys = validPubkeys(pubkeys)
	if b == nil || b.store == nil || len(pubkeys) == 0 {
		return true, nil
	}
	return b.waitJobs(ctx, []*hydrationJob{b.scheduleJob(ctx, hydrationBatchKey("profiles", pubkeys), func(jobCtx context.Context) error {
		return b.BackfillProfiles(jobCtx, pubkeys)
	})})
}

func (b *RelayUserFeedBackfiller) HydrateEngagement(ctx context.Context, ids []string) (bool, error) {
	ids = validHexIDs(ids)
	if b == nil || b.store == nil || len(ids) == 0 {
		return true, nil
	}
	return b.waitJobs(ctx, []*hydrationJob{b.scheduleJob(ctx, hydrationBatchKey("engagement", ids), func(jobCtx context.Context) error {
		return b.BackfillEngagement(jobCtx, ids)
	})})
}

func (b *RelayUserFeedBackfiller) HydrateThread(ctx context.Context, id string, limit int) (bool, error) {
	ids := validHexIDs([]string{id})
	if b == nil || b.store == nil || len(ids) != 1 {
		return true, nil
	}
	id = ids[0]
	return b.waitJobs(ctx, []*hydrationJob{b.scheduleJob(ctx, "thread:"+id, func(jobCtx context.Context) error {
		return b.BackfillThread(jobCtx, id, limit)
	})})
}

func (b *RelayUserFeedBackfiller) HydrateFollows(ctx context.Context, pubkey string) (bool, error) {
	pubkeys := validPubkeys([]string{pubkey})
	if b == nil || b.store == nil || len(pubkeys) != 1 {
		return true, nil
	}
	pubkey = pubkeys[0]
	return b.waitJobs(ctx, []*hydrationJob{b.scheduleJob(ctx, "follows:"+pubkey, func(jobCtx context.Context) error {
		return b.BackfillFollows(jobCtx, pubkey)
	})})
}

func (b *RelayUserFeedBackfiller) HydrateDMEnvelopes(ctx context.Context, pubkey string, kinds []int, until int64, limit uint64) (bool, error) {
	pubkeys := validPubkeys([]string{pubkey})
	if b == nil || b.store == nil || len(pubkeys) != 1 {
		return true, nil
	}
	pubkey = pubkeys[0]
	kinds = dmRelayKinds(kinds)
	if len(kinds) == 0 {
		return true, nil
	}
	key := "dm:" + pubkey + ":" + strconv.FormatInt(until, 10) + ":" + kindSignature(kinds)
	return b.waitJobs(ctx, []*hydrationJob{b.scheduleJob(ctx, key, func(jobCtx context.Context) error {
		return b.BackfillDMEnvelopes(jobCtx, pubkey, kinds, until, limit)
	})})
}

func (b *RelayUserFeedBackfiller) HydrateRelayEvents(ctx context.Context, input chstore.EventQueryInput, label string) (bool, error) {
	if b == nil || b.store == nil {
		return true, nil
	}
	filter, ok := b.relayFilterFromEventQuery(input)
	if !ok {
		return true, nil
	}
	key := "relay-query:" + strings.TrimSpace(label) + ":" + relayFilterSignature(filter)
	return b.waitJobs(ctx, []*hydrationJob{b.scheduleJob(ctx, key, func(jobCtx context.Context) error {
		return b.backfillRelayEventsWithFilter(jobCtx, input, label, filter, key)
	})})
}

func (b *RelayUserFeedBackfiller) BackfillUserFeed(ctx context.Context, pubkey string, limit uint64) error {
	if b == nil || b.store == nil || pubkey == "" || !b.shouldAttempt("feed:"+pubkey) {
		return nil
	}
	queryCtx, baseCtx, cancelQuery, timeout := b.queryContext(ctx)
	defer cancelQuery()

	collector := newEventCollector()
	authorLimit := maxInt(b.cfg.AuthorLimit, int(limit))
	authorEvents, err := b.query.Query(queryCtx, map[string]any{
		"authors": []string{pubkey},
		"kinds":   []int{0, 1, 6, 16},
		"limit":   authorLimit,
	}, timeout)
	if err != nil {
		return err
	}
	collector.add(authorEvents)

	b.queryEventsByID(queryCtx, collector, b.attemptValues("events", targetEventIDs(collector.records)), timeout)
	b.queryEngagement(queryCtx, collector, b.attemptValues("engagement", targetEventIDs(collector.records)), timeout)
	b.queryProfiles(queryCtx, collector, b.attemptValues("profiles", profilePubkeys(collector.records)), timeout)
	return b.insertCollected(baseCtx, "user feed backfill inserted", collector.records, "pubkey", pubkey)
}

func (b *RelayUserFeedBackfiller) BackfillEvents(ctx context.Context, ids []string) error {
	if b == nil || b.store == nil {
		return nil
	}
	ids = b.attemptValues("events", validHexIDs(ids))
	if len(ids) == 0 {
		return nil
	}
	queryCtx, baseCtx, cancelQuery, timeout := b.queryContext(ctx)
	defer cancelQuery()

	collector := newEventCollector()
	b.queryEventsByID(queryCtx, collector, ids, timeout)
	b.queryProfiles(queryCtx, collector, profilePubkeys(collector.records), timeout)
	return b.insertCollected(baseCtx, "event backfill inserted", collector.records, "ids", len(ids))
}

func (b *RelayUserFeedBackfiller) BackfillProfiles(ctx context.Context, pubkeys []string) error {
	if b == nil || b.store == nil {
		return nil
	}
	pubkeys = b.attemptValues("profiles", validPubkeys(pubkeys))
	if len(pubkeys) == 0 {
		return nil
	}
	queryCtx, baseCtx, cancelQuery, timeout := b.queryContext(ctx)
	defer cancelQuery()

	collector := newEventCollector()
	b.queryProfiles(queryCtx, collector, pubkeys, timeout)
	return b.insertCollected(baseCtx, "profile backfill inserted", collector.records, "pubkeys", len(pubkeys))
}

func (b *RelayUserFeedBackfiller) BackfillEngagement(ctx context.Context, ids []string) error {
	if b == nil || b.store == nil {
		return nil
	}
	ids = b.attemptValues("engagement", validHexIDs(ids))
	if len(ids) == 0 {
		return nil
	}
	queryCtx, baseCtx, cancelQuery, timeout := b.queryContext(ctx)
	defer cancelQuery()

	collector := newEventCollector()
	b.queryEngagement(queryCtx, collector, ids, timeout)
	b.queryProfiles(queryCtx, collector, profilePubkeys(collector.records), timeout)
	return b.insertCollected(baseCtx, "engagement backfill inserted", collector.records, "ids", len(ids))
}

func (b *RelayUserFeedBackfiller) BackfillThread(ctx context.Context, id string, limit int) error {
	ids := validHexIDs([]string{id})
	if len(ids) != 1 {
		return nil
	}
	id = ids[0]
	if b == nil || b.store == nil || !b.shouldAttempt("thread:"+id) {
		return nil
	}
	queryCtx, baseCtx, cancelQuery, timeout := b.queryContext(ctx)
	defer cancelQuery()

	collector := newEventCollector()
	b.queryEventsByID(queryCtx, collector, b.attemptValues("events", []string{id}), timeout)
	b.queryThreadReferences(queryCtx, collector, id, threadLimit(limit, b.cfg.ThreadLimit), timeout)
	threadIDs := targetEventIDs(collector.records)
	if len(threadIDs) == 0 {
		threadIDs = []string{id}
	}
	b.queryEngagement(queryCtx, collector, b.attemptValues("engagement", threadIDs), timeout)
	b.queryProfiles(queryCtx, collector, b.attemptValues("profiles", profilePubkeys(collector.records)), timeout)
	return b.insertCollected(baseCtx, "thread backfill inserted", collector.records, "root", id)
}

func (b *RelayUserFeedBackfiller) BackfillFollows(ctx context.Context, pubkey string) error {
	pubkeys := validPubkeys([]string{pubkey})
	if len(pubkeys) != 1 {
		return nil
	}
	pubkey = pubkeys[0]
	if b == nil || b.store == nil || !b.shouldAttempt("follows:"+pubkey) {
		return nil
	}
	queryCtx, baseCtx, cancelQuery, timeout := b.queryContext(ctx)
	defer cancelQuery()

	collector := newEventCollector()
	contactLists, err := b.query.Query(queryCtx, map[string]any{
		"authors": []string{pubkey},
		"kinds":   []int{3},
		"limit":   3,
	}, timeout)
	if err != nil {
		slog.Debug("follow contact-list backfill failed", "pubkey", pubkey, "error", err)
	}
	collector.add(contactLists)

	followers, err := b.query.Query(queryCtx, map[string]any{
		"#p":    []string{pubkey},
		"kinds": []int{3},
		"limit": b.cfg.FollowLimit,
	}, timeout)
	if err != nil {
		slog.Debug("follower backfill failed", "pubkey", pubkey, "error", err)
	}
	collector.add(followers)
	b.queryProfiles(queryCtx, collector, b.attemptValues("profiles", profilePubkeys(collector.records)), timeout)
	return b.insertCollected(baseCtx, "follow graph backfill inserted", collector.records, "pubkey", pubkey)
}

func (b *RelayUserFeedBackfiller) BackfillDMEnvelopes(ctx context.Context, pubkey string, kinds []int, until int64, limit uint64) error {
	pubkeys := validPubkeys([]string{pubkey})
	if len(pubkeys) != 1 {
		return nil
	}
	pubkey = pubkeys[0]
	kinds = dmRelayKinds(kinds)
	if b == nil || b.store == nil || len(kinds) == 0 {
		return nil
	}
	attemptKey := "dm:" + pubkey + ":" + strconv.FormatInt(until, 10) + ":" + kindSignature(kinds)
	if !b.shouldAttempt(attemptKey) {
		return nil
	}

	queryCtx, baseCtx, cancelQuery, timeout := b.queryContext(ctx)
	defer cancelQuery()

	collector := newEventCollector()
	inboxRelays := b.queryDMInboxRelays(queryCtx, collector, pubkey, timeout)
	query := b.query
	query.Relays = uniqueStrings(append(append([]string(nil), query.Relays...), inboxRelays...))

	pageLimit := dmBackfillLimit(limit, b.cfg.DMLimit)
	cursor := until
	for page := 0; page < b.cfg.DMBackfillPages; page++ {
		if queryCtx.Err() != nil {
			break
		}
		pageEvents := b.queryDMEnvelopePage(queryCtx, query, pubkey, kinds, cursor, pageLimit, timeout)
		if len(pageEvents) == 0 {
			break
		}
		collector.add(pageEvents)
		oldest := oldestRelayEventCreatedAt(pageEvents)
		if oldest <= 0 || len(pageEvents) < pageLimit {
			break
		}
		cursor = oldest
	}
	return b.insertCollected(baseCtx, "dm envelope backfill inserted", collector.records, "pubkey", pubkey, "kinds", kindSignature(kinds))
}

func (b *RelayUserFeedBackfiller) BackfillRelayEvents(ctx context.Context, input chstore.EventQueryInput, label string) error {
	if b == nil || b.store == nil {
		return nil
	}
	filter, ok := b.relayFilterFromEventQuery(input)
	if !ok {
		return nil
	}
	key := "relay-query:" + strings.TrimSpace(label) + ":" + relayFilterSignature(filter)
	return b.backfillRelayEventsWithFilter(ctx, input, label, filter, key)
}

func (b *RelayUserFeedBackfiller) backfillRelayEventsWithFilter(ctx context.Context, _ chstore.EventQueryInput, label string, filter map[string]any, attemptKey string) error {
	if !b.shouldAttempt(attemptKey) {
		return nil
	}
	queryCtx, baseCtx, cancelQuery, timeout := b.queryContext(ctx)
	defer cancelQuery()

	events, err := b.query.Query(queryCtx, filter, timeout)
	if err != nil {
		return err
	}
	collector := newEventCollector()
	collector.add(events)
	return b.insertCollected(baseCtx, "graphql relay hydration inserted", collector.records, "label", strings.TrimSpace(label))
}

func (b *RelayUserFeedBackfiller) queryContext(ctx context.Context) (context.Context, context.Context, context.CancelFunc, time.Duration) {
	timeout := b.cfg.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	baseCtx := context.WithoutCancel(ctx)
	queryCtx, cancelQuery := context.WithTimeout(baseCtx, timeout)
	return queryCtx, baseCtx, cancelQuery, timeout
}

func (b *RelayUserFeedBackfiller) queryEventsByID(ctx context.Context, collector *eventCollector, ids []string, timeout time.Duration) {
	for _, batch := range chunks(validHexIDs(ids), 80) {
		if ctx.Err() != nil {
			return
		}
		events, err := b.query.Query(ctx, map[string]any{
			"ids":   batch,
			"limit": len(batch) * 2,
		}, timeout)
		if err != nil {
			slog.Debug("event id backfill failed", "ids", len(batch), "error", err)
		}
		collector.add(events)
	}
}

func (b *RelayUserFeedBackfiller) queryEngagement(ctx context.Context, collector *eventCollector, ids []string, timeout time.Duration) {
	for _, batch := range chunks(validHexIDs(ids), 80) {
		if ctx.Err() != nil {
			return
		}
		events, err := b.query.Query(ctx, map[string]any{
			"#e":    batch,
			"kinds": []int{1, 6, 7, 16, 9735},
			"limit": b.cfg.EngagementLimit,
		}, timeout)
		if err != nil {
			slog.Debug("engagement backfill failed", "ids", len(batch), "error", err)
		}
		collector.add(events)
	}
}

func (b *RelayUserFeedBackfiller) queryDMInboxRelays(ctx context.Context, collector *eventCollector, pubkey string, timeout time.Duration) []string {
	events, err := b.query.Query(ctx, map[string]any{
		"authors": []string{pubkey},
		"kinds":   []int{10050},
		"limit":   3,
	}, timeout)
	if err != nil {
		slog.Debug("dm inbox relay discovery failed", "pubkey", pubkey, "error", err)
		return nil
	}
	collector.add(events)
	return dmInboxRelays(events)
}

func (b *RelayUserFeedBackfiller) queryDMEnvelopePage(ctx context.Context, query relayquery.Client, pubkey string, kinds []int, until int64, limit int, timeout time.Duration) []relayquery.Event {
	var out []relayquery.Event
	if containsInt(kinds, 1059) || containsInt(kinds, 21059) {
		wrapKinds := intersectKinds(kinds, []int{1059, 21059})
		filter := map[string]any{
			"#p":    []string{pubkey},
			"kinds": wrapKinds,
			"limit": limit,
		}
		if until > 0 {
			filter["until"] = until
		}
		events, err := query.Query(ctx, filter, timeout)
		if err != nil {
			slog.Debug("dm gift-wrap backfill failed", "pubkey", pubkey, "error", err)
		}
		out = append(out, events...)
	}
	if containsInt(kinds, 4) {
		receivedFilter := map[string]any{
			"#p":    []string{pubkey},
			"kinds": []int{4},
			"limit": limit,
		}
		if until > 0 {
			receivedFilter["until"] = until
		}
		received, err := query.Query(ctx, receivedFilter, timeout)
		if err != nil {
			slog.Debug("nip04 received dm backfill failed", "pubkey", pubkey, "error", err)
		}
		out = append(out, received...)

		sentFilter := map[string]any{
			"authors": []string{pubkey},
			"kinds":   []int{4},
			"limit":   limit,
		}
		if until > 0 {
			sentFilter["until"] = until
		}
		sent, err := query.Query(ctx, sentFilter, timeout)
		if err != nil {
			slog.Debug("nip04 sent dm backfill failed", "pubkey", pubkey, "error", err)
		}
		out = append(out, sent...)
	}
	return out
}

func (b *RelayUserFeedBackfiller) queryProfiles(ctx context.Context, collector *eventCollector, pubkeys []string, timeout time.Duration) {
	for _, batch := range chunks(validPubkeys(pubkeys), 80) {
		if ctx.Err() != nil {
			return
		}
		profiles, err := b.query.Query(ctx, map[string]any{
			"authors": batch,
			"kinds":   []int{0},
			"limit":   len(batch) * 3,
		}, timeout)
		if err != nil {
			slog.Debug("profile backfill failed", "pubkeys", len(batch), "error", err)
		}
		collector.add(profiles)
	}
}

func (b *RelayUserFeedBackfiller) queryThreadReferences(ctx context.Context, collector *eventCollector, rootID string, limit int, timeout time.Duration) {
	visited := map[string]struct{}{}
	frontier := []string{rootID}
	for depth := 0; depth < 8 && len(frontier) > 0 && len(collector.records) < limit; depth++ {
		batch := takeUnvisited(visited, frontier, 80)
		if len(batch) == 0 || ctx.Err() != nil {
			return
		}
		remaining := limit - len(collector.records)
		if remaining <= 0 {
			return
		}
		events, err := b.query.Query(ctx, map[string]any{
			"#e":    batch,
			"kinds": []int{1, 1111},
			"limit": min(maxInt(remaining, 100), 500),
		}, timeout)
		if err != nil {
			slog.Debug("thread reference backfill failed", "depth", depth, "ids", len(batch), "error", err)
		}

		newIDs := collector.add(events)
		frontier = frontier[:0]
		frontier = append(frontier, newIDs...)
	}
}

func (b *RelayUserFeedBackfiller) insertCollected(ctx context.Context, message string, records map[string]chstore.EventRecord, attrs ...any) error {
	if len(records) == 0 {
		return nil
	}
	out := make([]chstore.EventRecord, 0, len(records))
	for _, record := range records {
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Event.CreatedAt < out[j].Event.CreatedAt
	})
	insertCtx, cancelInsert := context.WithTimeout(ctx, 10*time.Second)
	defer cancelInsert()
	if err := b.store.InsertEvents(insertCtx, out); err != nil {
		return err
	}
	attrs = append(attrs, "events", len(out))
	slog.Info(message, attrs...)
	return nil
}

func (b *RelayUserFeedBackfiller) shouldAttempt(key string) bool {
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if last, ok := b.attempts[key]; ok && now.Sub(last) < b.cfg.Cooldown {
		return false
	}
	b.attempts[key] = now
	return true
}

func (b *RelayUserFeedBackfiller) attemptValues(prefix string, values []string) []string {
	now := time.Now()
	out := make([]string, 0, len(values))
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, value := range values {
		key := prefix + ":" + value
		if last, ok := b.attempts[key]; ok && now.Sub(last) < b.cfg.Cooldown {
			continue
		}
		b.attempts[key] = now
		out = append(out, value)
	}
	return out
}

type hydrationJob struct {
	done chan struct{}
	err  error
}

func (b *RelayUserFeedBackfiller) scheduleJob(ctx context.Context, key string, work func(context.Context) error) *hydrationJob {
	if key == "" {
		key = "unknown"
	}
	b.mu.Lock()
	if b.jobs == nil {
		b.jobs = map[string]*hydrationJob{}
	}
	if job, ok := b.jobs[key]; ok {
		b.mu.Unlock()
		return job
	}
	job := &hydrationJob{done: make(chan struct{})}
	b.jobs[key] = job
	b.mu.Unlock()

	go func() {
		job.err = work(context.WithoutCancel(ctx))
		close(job.done)

		b.mu.Lock()
		if b.jobs[key] == job {
			delete(b.jobs, key)
		}
		b.mu.Unlock()
	}()
	return job
}

func (b *RelayUserFeedBackfiller) waitJobs(ctx context.Context, jobs []*hydrationJob) (bool, error) {
	if len(jobs) == 0 {
		return true, nil
	}
	wait := b.cfg.Wait
	if wait <= 0 {
		return false, nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()

	var firstErr error
	for _, job := range jobs {
		select {
		case <-job.done:
			if job.err != nil && firstErr == nil {
				firstErr = job.err
			}
		case <-timer.C:
			return false, nil
		case <-ctx.Done():
			return false, nil
		}
	}
	return true, firstErr
}

type eventCollector struct {
	records map[string]chstore.EventRecord
}

func newEventCollector() *eventCollector {
	return &eventCollector{records: map[string]chstore.EventRecord{}}
}

func (c *eventCollector) add(events []relayquery.Event) []string {
	seen := time.Now().UTC()
	newIDs := make([]string, 0, len(events))
	for _, event := range events {
		if event.Event == nil {
			continue
		}
		if _, ok := c.records[event.Event.ID]; ok {
			continue
		}
		c.records[event.Event.ID] = chstore.EventRecord{
			Event: event.Event,
			Relay: event.Relay,
			Seen:  seen,
		}
		newIDs = append(newIDs, event.Event.ID)
	}
	return newIDs
}

func targetEventIDs(records map[string]chstore.EventRecord) []string {
	seen := map[string]struct{}{}
	for _, record := range records {
		event := record.Event
		if event == nil {
			continue
		}
		switch event.Kind {
		case 1, 1111:
			seen[event.ID] = struct{}{}
		case 6, 16:
			if id := firstHexTag(event.Tags, "e"); id != "" {
				seen[id] = struct{}{}
			}
		}
	}
	return sortedKeys(seen)
}

func profilePubkeys(records map[string]chstore.EventRecord) []string {
	seen := map[string]struct{}{}
	for _, record := range records {
		event := record.Event
		if event == nil {
			continue
		}
		if nostr.IsValidPublicKey(event.PubKey) {
			seen[event.PubKey] = struct{}{}
		}
		for _, tag := range event.Tags {
			if len(tag) >= 2 && tag[0] == "p" && nostr.IsValidPublicKey(tag[1]) {
				seen[tag[1]] = struct{}{}
			}
		}
	}
	return sortedKeys(seen)
}

func firstHexTag(tags nostr.Tags, key string) string {
	for _, tag := range tags {
		if len(tag) >= 2 && tag[0] == key && len(tag[1]) == 64 {
			return tag[1]
		}
	}
	return ""
}

func validHexIDs(values []string) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if !nostr.IsValid32ByteHex(value) {
			continue
		}
		seen[value] = struct{}{}
	}
	return sortedKeys(seen)
}

func validPubkeys(values []string) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if !nostr.IsValidPublicKey(value) {
			continue
		}
		seen[value] = struct{}{}
	}
	return sortedKeys(seen)
}

func (b *RelayUserFeedBackfiller) relayFilterFromEventQuery(input chstore.EventQueryInput) (map[string]any, bool) {
	if input.Empty {
		return nil, false
	}
	filter := map[string]any{}
	safe := false
	if ids := validHexIDs(input.IDs); len(ids) > 0 {
		filter["ids"] = ids
		safe = true
	}
	if pubkeys := validPubkeys(input.PubKeys); len(pubkeys) > 0 {
		filter["authors"] = pubkeys
		safe = true
	}
	if kinds := relayKinds(input.Kinds); len(kinds) > 0 {
		filter["kinds"] = kinds
		safe = true
	}
	for key, values := range relayTagFilters(input.Tags) {
		filter["#"+key] = values
		safe = true
	}
	if input.Since > 0 {
		filter["since"] = input.Since
		safe = true
	}
	if input.Until > 0 {
		filter["until"] = input.Until
		safe = true
	}
	if !safe {
		return nil, false
	}
	filter["limit"] = b.graphqlRelayLimit(input)
	return filter, true
}

func (b *RelayUserFeedBackfiller) graphqlRelayLimit(input chstore.EventQueryInput) int {
	capLimit := b.cfg.GraphQLLimit
	if capLimit <= 0 {
		capLimit = 100
	}
	limit := input.Limit
	if limit == 0 {
		limit = 50
	}
	if input.Offset > 0 && input.Limit > 0 {
		if input.Offset > ^uint64(0)-limit {
			limit = uint64(capLimit)
		} else {
			limit += input.Offset
		}
	}
	if ids := uint64(len(input.IDs)); ids > limit {
		limit = ids
	}
	if limit > uint64(capLimit) {
		limit = uint64(capLimit)
	}
	if limit <= 0 {
		return 50
	}
	return int(limit)
}

func relayKinds(kinds []int) []int {
	seen := map[int]struct{}{}
	for _, kind := range kinds {
		if kind < 0 {
			continue
		}
		seen[kind] = struct{}{}
	}
	out := make([]int, 0, len(seen))
	for kind := range seen {
		out = append(out, kind)
	}
	sort.Ints(out)
	return out
}

func relayTagFilters(tags []chstore.TagFilter) map[string][]string {
	out := map[string][]string{}
	for _, tag := range tags {
		if strings.EqualFold(strings.TrimSpace(tag.Dataset), "DERIVED_TAGS") {
			continue
		}
		key := strings.TrimSpace(tag.Key)
		if len(key) != 1 {
			continue
		}
		values := relayTagValues(tag)
		if len(values) == 0 {
			continue
		}
		out[key] = uniqueStrings(append(out[key], values...))
	}
	return out
}

func relayTagValues(tag chstore.TagFilter) []string {
	values := make([]string, 0, 1+len(tag.Values))
	if value := strings.TrimSpace(tag.Value); value != "" {
		values = append(values, value)
	}
	for _, value := range tag.Values {
		if value := strings.TrimSpace(value); value != "" {
			values = append(values, value)
		}
	}
	return uniqueStrings(values)
}

func relayFilterSignature(filter map[string]any) string {
	parts := make([]string, 0, len(filter))
	for key, value := range filter {
		switch v := value.(type) {
		case []string:
			values := append([]string(nil), v...)
			sort.Strings(values)
			parts = append(parts, key+"="+strings.Join(values, ","))
		case []int:
			values := append([]int(nil), v...)
			sort.Ints(values)
			intParts := make([]string, 0, len(values))
			for _, n := range values {
				intParts = append(intParts, strconv.Itoa(n))
			}
			parts = append(parts, key+"="+strings.Join(intParts, ","))
		default:
			parts = append(parts, key+"="+fmt.Sprint(value))
		}
	}
	sort.Strings(parts)
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:8])
}

func uniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		seen[value] = struct{}{}
	}
	return sortedKeys(seen)
}

func dmRelayKinds(kinds []int) []int {
	if len(kinds) == 0 {
		return []int{4, 1059}
	}
	seen := map[int]struct{}{}
	for _, kind := range kinds {
		switch kind {
		case 4, 1059, 21059:
			seen[kind] = struct{}{}
		}
	}
	out := make([]int, 0, len(seen))
	for kind := range seen {
		out = append(out, kind)
	}
	sort.Ints(out)
	return out
}

func dmBackfillLimit(requested uint64, configured int) int {
	limit := int(requested)
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	if configured > 0 {
		limit = min(limit, configured)
	}
	if limit <= 0 {
		return 200
	}
	return limit
}

func dmInboxRelays(events []relayquery.Event) []string {
	seen := map[string]struct{}{}
	for _, wrapped := range events {
		event := wrapped.Event
		if event == nil || event.Kind != 10050 {
			continue
		}
		for _, tag := range event.Tags {
			if len(tag) < 2 || tag[0] != "relay" {
				continue
			}
			relay := normalizeRelayURL(tag[1])
			if relay == "" {
				continue
			}
			seen[relay] = struct{}{}
		}
	}
	return sortedKeys(seen)
}

func normalizeRelayURL(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return ""
	}
	if parsed.Scheme != "wss" && parsed.Scheme != "ws" {
		return ""
	}
	if parsed.Host == "" {
		return ""
	}
	parsed.Fragment = ""
	return parsed.String()
}

func kindSignature(kinds []int) string {
	kinds = append([]int(nil), kinds...)
	sort.Ints(kinds)
	parts := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		parts = append(parts, strconv.Itoa(kind))
	}
	return strings.Join(parts, ",")
}

func containsInt(values []int, target int) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func intersectKinds(values []int, allowed []int) []int {
	allowedSet := map[int]struct{}{}
	for _, kind := range allowed {
		allowedSet[kind] = struct{}{}
	}
	seen := map[int]struct{}{}
	for _, kind := range values {
		if _, ok := allowedSet[kind]; ok {
			seen[kind] = struct{}{}
		}
	}
	out := make([]int, 0, len(seen))
	for kind := range seen {
		out = append(out, kind)
	}
	sort.Ints(out)
	return out
}

func oldestRelayEventCreatedAt(events []relayquery.Event) int64 {
	var oldest int64
	for _, wrapped := range events {
		if wrapped.Event == nil {
			continue
		}
		createdAt := wrapped.Event.CreatedAt.Time().Unix()
		if createdAt <= 0 {
			continue
		}
		if oldest == 0 || createdAt < oldest {
			oldest = createdAt
		}
	}
	return oldest
}

func chunks[T any](items []T, size int) [][]T {
	if len(items) == 0 {
		return nil
	}
	if size <= 0 {
		size = len(items)
	}
	out := make([][]T, 0, (len(items)+size-1)/size)
	for len(items) > 0 {
		n := size
		if len(items) < n {
			n = len(items)
		}
		out = append(out, items[:n])
		items = items[n:]
	}
	return out
}

func sortedKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func hydrationBatchKey(prefix string, values []string) string {
	values = append([]string(nil), values...)
	sort.Strings(values)
	if len(values) == 0 {
		return prefix
	}
	if len(values) == 1 {
		return prefix + ":" + values[0]
	}
	sum := sha256.Sum256([]byte(strings.Join(values, ",")))
	return prefix + ":" + hex.EncodeToString(sum[:8])
}

func takeUnvisited(visited map[string]struct{}, ids []string, max int) []string {
	out := make([]string, 0, max)
	for _, id := range ids {
		if _, ok := visited[id]; ok {
			continue
		}
		visited[id] = struct{}{}
		out = append(out, id)
		if len(out) == max {
			break
		}
	}
	return out
}

func threadLimit(requested int, configured int) int {
	if requested <= 0 || requested > 2000 {
		requested = 1000
	}
	if configured <= 0 {
		return requested
	}
	return min(requested, configured)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
