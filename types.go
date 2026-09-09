// Package deadhorse defines the shared vocabulary between a DeadHorse
// server and its clients: the request/response shapes and the Throttler
// interface they're built around. It has no dependency beyond the standard
// library and no opinion on how a Throttle call is actually carried out --
// see the server subpackage for the in-memory engine and DHP/1 server, and
// the client subpackage for the sharded network client.
package deadhorse

import (
	"context"
	"time"
)

// Limit describes a leaky bucket's shape: how large it is and how quickly
// it drains. Callers typically define one Limit per rate-limited operation
// (e.g. "100 writes/sec for this service") and reuse it across every
// RequestEntry for that operation, rather than recomputing it per request.
type Limit struct {
	Capacity         int64         // bucket size, in cost units
	EmissionInterval time.Duration // time to leak one cost unit
}

// RequestEntry is one leaky-bucket check: consume (or peek at) Cost units
// from the bucket identified by Key, shaped by Limit.
type RequestEntry struct {
	Key   string
	Limit Limit
	// Cost is how many units this request consumes. Zero means "not
	// specified," which defaults to 1 -- see EffectiveCost.
	Cost int64
	// Peek, if true, reports what would happen without consuming from the
	// bucket. The zero value (false) is the safe default: an entry that
	// forgets to set this still actually enforces its limit, rather than
	// silently never doing anything.
	Peek bool
}

// ResponseEntry is the outcome of one RequestEntry, always returned in the
// same order and at the same index as its request.
type ResponseEntry struct {
	Key       string
	Throttled bool
	// Remaining is the bucket's current headroom in cost units, clamped to
	// [0, Limit.Capacity].
	Remaining int64
	// RetryAfter is how long until this exact request would have fit. Zero
	// when not throttled.
	RetryAfter time.Duration
	// Err is non-nil if this entry couldn't be evaluated normally -- a
	// validation problem (e.g. a key a wire client can't encode) or an
	// operational failure (e.g. the shard that owns this key was
	// unreachable). Throttled still holds a sensible value even when Err is
	// set -- whatever the caller's fail-open/fail-closed policy decided --
	// so code that only reads Throttled works the same whether or not it
	// checks Err.
	Err error
}

// EffectiveCost normalizes a request's cost: zero/negative means "not
// specified," which defaults to 1. Every Throttler implementation and the
// DHP/1 wire client apply this so an omitted cost means the same thing
// everywhere.
func EffectiveCost(cost int64) int64 {
	if cost <= 0 {
		return 1
	}
	return cost
}

// Throttler evaluates a batch of leaky-bucket checks. Implementations
// always return a result slice the same length as entries, in the same
// order, even when the returned error is non-nil -- a call-level error
// means something went wrong for some subset of entries, not that nothing
// can be reported; see ResponseEntry.Err for which ones and why.
type Throttler interface {
	Throttle(ctx context.Context, entries []RequestEntry) ([]ResponseEntry, error)
}
