package relayquery

import (
	"sync"
	"time"
)

// Default circuit-breaker bounds: a relay that fails is skipped for baseBackoff,
// doubling per consecutive failure up to maxBackoff. This keeps a permanently
// dead relay from being re-dialed on every request while still re-probing it
// periodically so a recovered relay comes back.
const (
	defaultBaseBackoff = 30 * time.Second
	defaultMaxBackoff  = 5 * time.Minute
)

// RelayHealth tracks per-relay failure state so dead relays can be skipped. It is
// safe for concurrent use and is shared across Client value copies via a pointer.
type RelayHealth struct {
	mu          sync.Mutex
	failures    map[string]*failEntry
	baseBackoff time.Duration
	maxBackoff  time.Duration
}

type failEntry struct {
	until  time.Time
	streak int
}

func NewRelayHealth() *RelayHealth {
	return &RelayHealth{
		failures:    map[string]*failEntry{},
		baseBackoff: defaultBaseBackoff,
		maxBackoff:  defaultMaxBackoff,
	}
}

// available reports whether relay may be dialed now (not in backoff).
func (h *RelayHealth) available(relay string, now time.Time) bool {
	if h == nil {
		return true
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.failures[relay]
	return !ok || now.After(e.until)
}

// recordSuccess clears any backoff so a recovered relay is fully trusted again.
func (h *RelayHealth) recordSuccess(relay string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	delete(h.failures, relay)
	h.mu.Unlock()
}

// recordFailure extends the relay's backoff window with exponential growth.
func (h *RelayHealth) recordFailure(relay string, now time.Time) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	e := h.failures[relay]
	if e == nil {
		e = &failEntry{}
		h.failures[relay] = e
	}
	e.streak++
	// Cap the shift so the exponential never overflows the Duration.
	shift := e.streak - 1
	if shift > 16 {
		shift = 16
	}
	backoff := h.baseBackoff << uint(shift)
	if backoff <= 0 || backoff > h.maxBackoff {
		backoff = h.maxBackoff
	}
	e.until = now.Add(backoff)
}
