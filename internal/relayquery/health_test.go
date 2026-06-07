package relayquery

import (
	"testing"
	"time"
)

func TestRelayHealthBacksOffAndRecovers(t *testing.T) {
	h := NewRelayHealth()
	now := time.Now()
	const relay = "wss://dead.example"

	if !h.available(relay, now) {
		t.Fatal("relay should start available")
	}

	h.recordFailure(relay, now)
	if h.available(relay, now) {
		t.Fatal("relay should be in backoff immediately after a failure")
	}
	// Still backed off within the base window, available once it elapses.
	if h.available(relay, now.Add(defaultBaseBackoff-time.Second)) {
		t.Fatal("relay should still be backed off before base window elapses")
	}
	if !h.available(relay, now.Add(defaultBaseBackoff+time.Second)) {
		t.Fatal("relay should be re-probable after the base window")
	}

	// A success clears the breaker entirely.
	h.recordSuccess(relay)
	if !h.available(relay, now) {
		t.Fatal("relay should be available again after a success")
	}
}

func TestRelayHealthBackoffGrowsAndCaps(t *testing.T) {
	h := NewRelayHealth()
	now := time.Now()
	const relay = "wss://flaky.example"

	for i := 0; i < 50; i++ {
		h.recordFailure(relay, now)
	}
	// After many failures the window must be clamped to maxBackoff, never longer.
	if h.available(relay, now.Add(defaultMaxBackoff+time.Second)) {
		// past the cap → available, good
	} else {
		t.Fatal("relay should be available once maxBackoff elapses")
	}
	if h.available(relay, now.Add(defaultMaxBackoff-time.Second)) {
		t.Fatal("relay should still be backed off just before maxBackoff")
	}
}

func TestHealthyRelaysFallsBackWhenAllBackedOff(t *testing.T) {
	h := NewRelayHealth()
	now := time.Now()
	relays := []string{"wss://a", "wss://b"}
	for _, r := range relays {
		h.recordFailure(r, now)
	}
	c := Client{Health: h}
	// All relays are backed off, so we probe the full set rather than going dark.
	if got := c.healthyRelays(relays); len(got) != len(relays) {
		t.Fatalf("expected fallback to full relay set, got %d", len(got))
	}
}
