// Package server is the DeadHorse server side: the in-memory GCRA engine
// and the DHP/1 TextServer built on top of it. A pure client of a remote
// DeadHorse fleet never needs to import this package -- see the client
// subpackage instead.
package server

import (
	"context"
	"time"

	"github.com/spiridonov/deadhorse"
)

// InMemoryThrottler is a leaky-bucket (GCRA) rate limiter over a striped,
// self-expiring in-memory store. It holds no configuration of its own --
// every call carries its own capacity and rate -- and no key survives longer
// than about 1-2 GC intervals past its last touch. Throttle never returns a
// non-nil error: the GCRA check itself can't fail (a nonsensical limit just
// fails closed, see gcraCheck), so ResponseEntry.Err is always nil too.
type InMemoryThrottler struct {
	store *store
}

var _ deadhorse.Throttler = &InMemoryThrottler{}

// NewInMemoryThrottler starts an InMemoryThrottler with the given number of
// concurrency stripes and GC interval; zero/negative values fall back to
// sensible defaults. Call Close when done to stop its GC goroutine.
func NewInMemoryThrottler(numStripes int, gcInterval time.Duration) *InMemoryThrottler {
	return &InMemoryThrottler{store: newStore(numStripes, gcInterval)}
}

func (t *InMemoryThrottler) Throttle(ctx context.Context, entries []deadhorse.RequestEntry) ([]deadhorse.ResponseEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	now := time.Now().UnixNano()
	result := make([]deadhorse.ResponseEntry, len(entries))
	for i, e := range entries {
		result[i].Key = e.Key

		b := t.store.stripeFor(e.Key).getOrCreate(e.Key)
		cost := deadhorse.EffectiveCost(e.Cost)

		b.mu.Lock()
		throttled, remaining, retryAfter, admittedTAT := gcraCheck(b.tat, now, e.Limit.Capacity, e.Limit.EmissionInterval, cost)
		if !e.Peek && !throttled {
			b.tat = admittedTAT
		}
		b.mu.Unlock()

		result[i].Throttled = throttled
		result[i].Remaining = remaining
		result[i].RetryAfter = retryAfter
	}
	return result, nil
}

// Close stops the background GC goroutine. It does not otherwise release
// memory held by the store -- the process is expected to exit shortly after.
func (t *InMemoryThrottler) Close() {
	t.store.close()
}

func (t *InMemoryThrottler) keyCountEstimate() int {
	return t.store.keyCountEstimate()
}
