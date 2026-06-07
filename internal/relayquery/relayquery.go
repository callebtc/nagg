package relayquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nbd-wtf/go-nostr"
)

type Event struct {
	Relay string
	Event *nostr.Event
}

type Client struct {
	Relays          []string
	ReadLimit       int64
	DialTimeout     time.Duration
	ReadIdleTimeout time.Duration
	// Health, when set, circuit-breaks relays that recently failed so a dead
	// relay is not re-dialed on every request. A nil Health disables breaking.
	Health *RelayHealth
}

func (c Client) Query(ctx context.Context, filter map[string]any, timeout time.Duration) ([]Event, error) {
	relays := c.healthyRelays(uniqueRelays(c.Relays))
	if len(relays) == 0 {
		return nil, nil
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	qctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	subID := fmt.Sprintf("nagg-demand-%d", time.Now().UnixNano())

	type relayResult struct {
		relay  string
		events []Event
		err    error
	}
	// Buffered to len(relays) so a straggler goroutine never blocks on send after
	// we have already returned on timeout; it drains here and exits.
	results := make(chan relayResult, len(relays))
	for _, relay := range relays {
		relay := relay
		go func() {
			events, err := c.queryRelay(qctx, relay, subID, filter)
			results <- relayResult{relay: relay, events: events, err: err}
		}()
	}

	var out []Event
	var errs []error
	for done := 0; done < len(relays); {
		select {
		case res := <-results:
			done++
			if res.err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", res.relay, res.err))
				c.Health.recordFailure(res.relay, time.Now())
				slog.Warn("on-demand relay query failed", "relay", res.relay, "error", res.err)
				continue
			}
			c.Health.recordSuccess(res.relay)
			out = append(out, res.events...)
			if len(res.events) > 0 {
				slog.Info("on-demand relay query returned events", "relay", res.relay, "events", len(res.events))
			}
		case <-qctx.Done():
			// The deadline elapsed: stop waiting on the remaining relays (usually
			// the slow or dead ones) and return what the responsive relays gave.
			return finishQuery(out, errs)
		}
	}
	return finishQuery(out, errs)
}

func finishQuery(out []Event, errs []error) ([]Event, error) {
	if len(out) > 0 {
		return out, nil
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return nil, nil
}

// healthyRelays drops relays currently in circuit-breaker backoff. If every
// relay is backed off (e.g. a transient network outage tripped them all), it
// probes the full set so the breaker can recover instead of going dark.
func (c Client) healthyRelays(relays []string) []string {
	if c.Health == nil || len(relays) == 0 {
		return relays
	}
	now := time.Now()
	out := make([]string, 0, len(relays))
	for _, relay := range relays {
		if c.Health.available(relay, now) {
			out = append(out, relay)
		}
	}
	if len(out) == 0 {
		return relays
	}
	return out
}

func (c Client) queryRelay(ctx context.Context, relay string, subID string, filter map[string]any) ([]Event, error) {
	dialTimeout := c.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = 5 * time.Second
	}
	idleTimeout := c.ReadIdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = 2 * time.Second
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: dialTimeout,
		Proxy:            http.ProxyFromEnvironment,
	}
	conn, _, err := dialer.DialContext(ctx, relay, http.Header{})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if c.ReadLimit > 0 {
		conn.SetReadLimit(c.ReadLimit)
	} else {
		conn.SetReadLimit(8 << 20)
	}

	if err := conn.WriteJSON([]any{"REQ", subID, filter}); err != nil {
		return nil, fmt.Errorf("send REQ: %w", err)
	}
	defer conn.WriteJSON([]any{"CLOSE", subID})

	var out []Event
	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return out, nil
			}
			return out, ctx.Err()
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(idleTimeout))
		_, data, err := conn.ReadMessage()
		if err != nil {
			if strings.Contains(err.Error(), "i/o timeout") {
				return out, nil
			}
			if websocket.IsCloseError(err, websocket.CloseNoStatusReceived, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return out, nil
			}
			if len(out) > 0 {
				return out, nil
			}
			return out, err
		}

		event, eose, err := parseRelayMessage(data)
		if err != nil {
			continue
		}
		if event != nil && validateEvent(event) == nil {
			out = append(out, Event{Relay: relay, Event: event})
		}
		if eose {
			return out, nil
		}
	}
}

func parseRelayMessage(data []byte) (*nostr.Event, bool, error) {
	var frame []json.RawMessage
	if err := json.Unmarshal(data, &frame); err != nil {
		return nil, false, err
	}
	if len(frame) == 0 {
		return nil, false, nil
	}
	var typ string
	if err := json.Unmarshal(frame[0], &typ); err != nil {
		return nil, false, err
	}
	switch typ {
	case "EVENT":
		if len(frame) < 3 {
			return nil, false, nil
		}
		var event nostr.Event
		if err := json.Unmarshal(frame[2], &event); err != nil {
			return nil, false, err
		}
		return &event, false, nil
	case "EOSE":
		return nil, true, nil
	default:
		return nil, false, nil
	}
}

func validateEvent(event *nostr.Event) error {
	if len(event.ID) != 64 || len(event.PubKey) != 64 || len(event.Sig) != 128 {
		return fmt.Errorf("invalid event shape")
	}
	if !event.CheckID() {
		return fmt.Errorf("invalid id")
	}
	ok, err := event.CheckSignature()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("invalid signature")
	}
	return nil
}

func uniqueRelays(relays []string) []string {
	out := make([]string, 0, len(relays))
	seen := map[string]struct{}{}
	for _, relay := range relays {
		relay = strings.TrimSpace(relay)
		if relay == "" {
			continue
		}
		if _, ok := seen[relay]; ok {
			continue
		}
		seen[relay] = struct{}{}
		out = append(out, relay)
	}
	return out
}
