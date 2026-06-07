package config

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/enrich"
	"github.com/vertex-lab/nagg/internal/firehose"
	"github.com/vertex-lab/nagg/internal/ingest"
)

type Config struct {
	ClickHouse chstore.Config
	API        APIConfig
	Firehose   firehose.Config
	Ingest     ingest.Config
	Vertex     VertexConfig
	OnDemand   OnDemandConfig
	Viewer     ViewerConfig
	Enrich     EnrichConfig
	Cache      CacheConfig

	// RunIngester / RunEnricher let the API process host the firehose ingester
	// and the enrichment runner in-process (alongside the HTTP server + Vertex
	// syncer), so a single `nagg` service can do everything against one
	// ClickHouse + Redis. Default on. Set false to split those workers back out
	// into the standalone cmd/ingester / cmd/enricher binaries (e.g. to scale the
	// API horizontally without N duplicate firehose consumers).
	RunIngester bool
	RunEnricher bool
}

type APIConfig struct {
	GraphQLTimeout time.Duration
}

// CacheConfig configures the optional shared Redis response cache. When URL is
// empty the cache is disabled and every request is computed as before.
type CacheConfig struct {
	URL        string
	DefaultTTL time.Duration
	// StaleFor is how long past a key's fresh TTL a cached response may still be
	// served while it is revalidated in the background (stale-while-revalidate).
	// A non-positive value disables stale serving, making every expiry a full
	// recompute.
	StaleFor time.Duration
}

type VertexConfig struct {
	PrivateKey          string
	Relay               string
	ValidateNIP05       bool
	ProfileMinFollowers int
	RankMinFollowers    int
	SyncBatch           int
}

type ViewerConfig struct {
	PubKey string
}

type EnrichConfig struct {
	Tasks        []string
	BatchSize    int
	PollInterval time.Duration
	ModelVersion string
}

type OnDemandConfig struct {
	UserFeed                 bool
	GraphQLHydration         bool
	Cooldown                 time.Duration
	Timeout                  time.Duration
	Wait                     time.Duration
	AuthorLimit              int
	EngagementLimit          int
	ThreadLimit              int
	FollowLimit              int
	DMLimit                  int
	DMBackfillPages          int
	GraphQLLimit             int
	GraphQLMaxJobsPerRequest int
}

func Load() (Config, error) {
	onDemandUserFeed := parseBool(env("NAGG_ON_DEMAND_USER_FEED", "false"))
	cfg := Config{
		ClickHouse: chstore.Config{
			Addr:         env("NAGG_CLICKHOUSE_ADDR", "127.0.0.1:9000"),
			Database:     env("NAGG_CLICKHOUSE_DATABASE", "default"),
			Username:     env("NAGG_CLICKHOUSE_USERNAME", "default"),
			Password:     os.Getenv("NAGG_CLICKHOUSE_PASSWORD"),
			MaxOpenConns: parseInt(env("NAGG_CLICKHOUSE_MAX_OPEN_CONNS", "30")),
			MaxIdleConns: parseInt(env("NAGG_CLICKHOUSE_MAX_IDLE_CONNS", "10")),
		},
		API: APIConfig{
			GraphQLTimeout: parseDuration(env("NAGG_GRAPHQL_TIMEOUT", "30s")),
		},
		Firehose: firehose.Config{
			Relays:        splitCSV(env("NAGG_RELAYS", "wss://relay.damus.io,wss://nos.lol,wss://relay.snort.social")),
			Kinds:         parseKinds(env("NAGG_KINDS", "0,1,3,4,6,7,16,443,444,445,1059,1063,9735,10050,10051,30078,38000")),
			Since:         parseDurationPtr(env("NAGG_SINCE", "24h")),
			RelayRetry:    parseDuration(env("NAGG_RELAY_RETRY", "30s")),
			SeenCacheSize: parseInt(env("NAGG_SEEN_CACHE_SIZE", "200000")),
			ReadLimit:     parseInt64(env("NAGG_RELAY_READ_LIMIT_BYTES", "2097152")),
			SubID:         env("NAGG_SUB_ID", "nagg-firehose"),
		},
		Ingest: ingest.Config{
			BatchSize:     parseInt(env("NAGG_BATCH_SIZE", "1000")),
			FlushInterval: parseDuration(env("NAGG_FLUSH_INTERVAL", "5s")),
			QueueSize:     parseInt(env("NAGG_QUEUE_SIZE", "10000")),
			VerifyEvents:  parseBool(env("NAGG_VERIFY_EVENTS", "true")),
		},
		Vertex: VertexConfig{
			PrivateKey:          os.Getenv("NAGG_VERTEX_PRIVATE_KEY"),
			Relay:               env("NAGG_VERTEX_RELAY", "wss://relay.vertexlab.io"),
			ValidateNIP05:       parseBool(env("NAGG_NIP05_VALIDATE", "true")),
			ProfileMinFollowers: parseInt(env("NAGG_VERTEX_PROFILE_MIN_FOLLOWERS", "500")),
			RankMinFollowers:    parseInt(env("NAGG_VERTEX_RANK_MIN_FOLLOWERS", "500")),
			SyncBatch:           parseInt(env("NAGG_VERTEX_SYNC_BATCH", "200")),
		},
		OnDemand: OnDemandConfig{
			UserFeed:                 onDemandUserFeed,
			GraphQLHydration:         parseBool(env("NAGG_ON_DEMAND_GRAPHQL_HYDRATION", strconv.FormatBool(onDemandUserFeed))),
			Cooldown:                 parseDuration(env("NAGG_ON_DEMAND_COOLDOWN", "5m")),
			Timeout:                  parseDuration(env("NAGG_ON_DEMAND_TIMEOUT", "5s")),
			Wait:                     parseDuration(env("NAGG_ON_DEMAND_WAIT", "0s")),
			AuthorLimit:              parseInt(env("NAGG_ON_DEMAND_AUTHOR_LIMIT", "100")),
			EngagementLimit:          parseInt(env("NAGG_ON_DEMAND_ENGAGEMENT_LIMIT", "1000")),
			ThreadLimit:              parseInt(env("NAGG_ON_DEMAND_THREAD_LIMIT", "1000")),
			FollowLimit:              parseInt(env("NAGG_ON_DEMAND_FOLLOW_LIMIT", "1000")),
			DMLimit:                  parseInt(env("NAGG_ON_DEMAND_DM_LIMIT", "200")),
			DMBackfillPages:          parseInt(env("NAGG_ON_DEMAND_DM_BACKFILL_PAGES", "2")),
			GraphQLLimit:             parseInt(env("NAGG_ON_DEMAND_GRAPHQL_LIMIT", "100")),
			GraphQLMaxJobsPerRequest: parseInt(env("NAGG_ON_DEMAND_GRAPHQL_MAX_JOBS_PER_REQUEST", "4")),
		},
		Viewer: ViewerConfig{
			PubKey: strings.ToLower(strings.TrimSpace(os.Getenv("NAGG_VIEWER_PUBKEY"))),
		},
		Enrich: EnrichConfig{
			Tasks:        supportedEnrichTasks(splitCSV(env("NAGG_ENRICH_TASKS", "quality"))),
			BatchSize:    parseInt(env("NAGG_ENRICH_BATCH_SIZE", "256")),
			PollInterval: parseDuration(env("NAGG_ENRICH_POLL_INTERVAL", "30s")),
			ModelVersion: env("NAGG_ENRICH_MODEL_VERSION", "local-skeleton-v1"),
		},
		Cache: CacheConfig{
			URL:        strings.TrimSpace(os.Getenv("NAGG_REDIS_URL")),
			DefaultTTL: parseDuration(env("NAGG_CACHE_DEFAULT_TTL", "30s")),
			StaleFor:   parseDuration(env("NAGG_CACHE_STALE_FOR", "5m")),
		},
		RunIngester: parseBool(env("NAGG_RUN_INGESTER", "true")),
		RunEnricher: parseBool(env("NAGG_RUN_ENRICHER", "true")),
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	if _, _, err := net.SplitHostPort(c.ClickHouse.Addr); err != nil {
		return fmt.Errorf("NAGG_CLICKHOUSE_ADDR: %w", err)
	}
	if len(c.Firehose.Relays) == 0 {
		return errors.New("NAGG_RELAYS must contain at least one relay URL")
	}
	if c.Ingest.BatchSize < 1 {
		return errors.New("NAGG_BATCH_SIZE must be positive")
	}
	if c.Ingest.FlushInterval <= 0 {
		return errors.New("NAGG_FLUSH_INTERVAL must be positive")
	}
	if c.API.GraphQLTimeout <= 0 {
		return errors.New("NAGG_GRAPHQL_TIMEOUT must be positive")
	}
	if c.Vertex.PrivateKey != "" {
		if len(c.Vertex.PrivateKey) != 64 {
			return errors.New("NAGG_VERTEX_PRIVATE_KEY must be 64 hex characters")
		}
		if _, err := hex.DecodeString(c.Vertex.PrivateKey); err != nil {
			return fmt.Errorf("NAGG_VERTEX_PRIVATE_KEY: %w", err)
		}
		relayURL, err := url.Parse(c.Vertex.Relay)
		if err != nil {
			return fmt.Errorf("NAGG_VERTEX_RELAY: %w", err)
		}
		if relayURL.Scheme != "wss" && relayURL.Scheme != "ws" {
			return errors.New("NAGG_VERTEX_RELAY must use ws or wss")
		}
	}
	if c.Vertex.ProfileMinFollowers < 0 {
		return errors.New("NAGG_VERTEX_PROFILE_MIN_FOLLOWERS must be non-negative")
	}
	if c.Vertex.RankMinFollowers < 0 {
		return errors.New("NAGG_VERTEX_RANK_MIN_FOLLOWERS must be non-negative")
	}
	if c.Vertex.SyncBatch < 1 {
		return errors.New("NAGG_VERTEX_SYNC_BATCH must be positive")
	}
	if c.Viewer.PubKey != "" {
		if len(c.Viewer.PubKey) != 64 {
			return errors.New("NAGG_VIEWER_PUBKEY must be 64 hex characters")
		}
		if _, err := hex.DecodeString(c.Viewer.PubKey); err != nil {
			return fmt.Errorf("NAGG_VIEWER_PUBKEY: %w", err)
		}
	}
	if c.Enrich.BatchSize < 1 {
		return errors.New("NAGG_ENRICH_BATCH_SIZE must be positive")
	}
	if c.Enrich.PollInterval <= 0 {
		return errors.New("NAGG_ENRICH_POLL_INTERVAL must be positive")
	}
	if strings.TrimSpace(c.Enrich.ModelVersion) == "" {
		return errors.New("NAGG_ENRICH_MODEL_VERSION must be non-empty")
	}
	// NAGG_ENRICH_TASKS is NOT rejected here: unsupported tasks are dropped at
	// load (supportedEnrichTasks). Now that the API hosts the enricher in-process,
	// a stale env value must not crash the whole service — it just runs the
	// supported subset.
	// NAGG_REDIS_URL is intentionally not validated here: the cache is
	// best-effort, so an empty or malformed URL just disables it (see cache.New)
	// rather than failing the process.
	return nil
}

func validEnrichTask(task string) bool {
	return enrich.SupportedTask(task)
}

// supportedEnrichTasks drops any tasks this build no longer supports (e.g. a
// stale NAGG_ENRICH_TASKS env left over from a previous version with trending /
// topics / ML tasks). Dropping instead of erroring keeps the API — which now
// hosts the enricher in-process — from crashing on a stale value; unsupported
// tasks are simply ignored.
func supportedEnrichTasks(tasks []string) []string {
	out := make([]string, 0, len(tasks))
	for _, task := range tasks {
		if validEnrichTask(task) {
			out = append(out, task)
		}
	}
	return out
}

func env(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return out
}

func parseKinds(value string) []int {
	var kinds []int
	for _, part := range splitCSV(value) {
		if start, end, ok := strings.Cut(part, "-"); ok {
			a, errA := strconv.Atoi(strings.TrimSpace(start))
			b, errB := strconv.Atoi(strings.TrimSpace(end))
			if errA == nil && errB == nil && a <= b {
				for k := a; k <= b; k++ {
					kinds = append(kinds, k)
				}
			}
			continue
		}
		if k, err := strconv.Atoi(part); err == nil {
			kinds = append(kinds, k)
		}
	}
	return kinds
}

func parseDuration(value string) time.Duration {
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0
	}
	return d
}

func parseDurationPtr(value string) *time.Duration {
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		return nil
	}
	return &d
}

func parseInt(value string) int {
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return n
}

func parseInt64(value string) int64 {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func parseBool(value string) bool {
	v, err := strconv.ParseBool(value)
	return err == nil && v
}
