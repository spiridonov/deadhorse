package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/spiridonov/deadhorse"
)

// throttle1 runs a single entry through th and returns its one result,
// failing the test immediately on an unexpected call-level error -- a
// terser stand-in for the pre-context `th.Throttle([]RequestEntry{e})[0]`
// shape used throughout these tests.
func throttle1(t *testing.T, th deadhorse.Throttler, e deadhorse.RequestEntry) deadhorse.ResponseEntry {
	t.Helper()
	got, err := th.Throttle(context.Background(), []deadhorse.RequestEntry{e})
	require.NoError(t, err)
	require.Len(t, got, 1)
	return got[0]
}

func TestInMemoryThrottlerBurstThenThrottle(t *testing.T) {
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	entry := deadhorse.RequestEntry{Key: "org:acme:writes", Limit: deadhorse.Limit{Capacity: 2, EmissionInterval: time.Hour}}

	r1 := throttle1(t, th, entry)
	require.False(t, r1.Throttled, "1st request into a capacity-2 bucket should be admitted")
	r2 := throttle1(t, th, entry)
	require.False(t, r2.Throttled, "2nd request into a capacity-2 bucket should be admitted")
	r3 := throttle1(t, th, entry)
	require.True(t, r3.Throttled, "3rd request into a capacity-2 bucket should be throttled")
	assert.Positive(t, r3.RetryAfter)
	assert.NoError(t, r3.Err)
}

func TestInMemoryThrottlerWorkedExample(t *testing.T) {
	// Mirrors the reference scenario: capacity=100, emission_interval=10ms
	// (100 req/s sustained, burst of 100). All 101 requests arrive in a
	// single batch so they share one "now" -- exactly one must be throttled
	// (the 101st), with a retryAfter of exactly one emission interval.
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	const n = 101
	entries := make([]deadhorse.RequestEntry, n)
	for i := range entries {
		entries[i] = deadhorse.RequestEntry{Key: "org:acme:writes", Limit: deadhorse.Limit{Capacity: 100, EmissionInterval: 10 * time.Millisecond}}
	}

	results, err := th.Throttle(context.Background(), entries)
	require.NoError(t, err)
	throttledCount := 0
	var lastRetryAfter time.Duration
	for _, r := range results {
		if r.Throttled {
			throttledCount++
			lastRetryAfter = r.RetryAfter
		}
	}
	require.Equal(t, 1, throttledCount, "expected exactly 1 throttled request (the 101st)")
	assert.Equal(t, 10*time.Millisecond, lastRetryAfter, "one emission interval")
}

func TestInMemoryThrottlerPeekDoesNotConsume(t *testing.T) {
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	peekEntry := deadhorse.RequestEntry{Key: "peek-key", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: time.Hour}, Peek: true}
	realEntry := peekEntry
	realEntry.Peek = false

	// Many peeks in a row must never consume the bucket's one unit of
	// capacity.
	for i := 0; i < 5; i++ {
		require.Falsef(t, throttle1(t, th, peekEntry).Throttled,
			"peek %d: a fresh capacity-1 bucket should never appear throttled under peek", i)
	}

	r1 := throttle1(t, th, realEntry)
	require.False(t, r1.Throttled, "first real call after only peeks should still be admitted")
	r2 := throttle1(t, th, realEntry)
	require.True(t, r2.Throttled, "second back-to-back real call on a capacity-1 bucket should be throttled")
}

func TestInMemoryThrottlerDefaultIsRealNotPeek(t *testing.T) {
	// The zero value of Peek is false, which must mean "enforce the limit
	// for real" -- an entry that forgets to set Peek should not silently
	// become a no-op rate limiter.
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	entry := deadhorse.RequestEntry{Key: "default-mode", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: time.Hour}}
	r1 := throttle1(t, th, entry)
	require.False(t, r1.Throttled, "first request into an empty capacity-1 bucket should be admitted")
	r2 := throttle1(t, th, entry)
	require.True(t, r2.Throttled, "second request should be throttled, proving the first one actually consumed the bucket")
}

func TestInMemoryThrottlerUnsetCostDefaultsToOne(t *testing.T) {
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	entry := deadhorse.RequestEntry{Key: "cost-default", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: time.Hour}}
	// entry.Cost is left at its zero value on purpose, matching every
	// caller written before RequestEntry.Cost existed.
	r1 := throttle1(t, th, entry)
	require.False(t, r1.Throttled, "unset Cost should behave as Cost=1 and be admitted into an empty capacity-1 bucket")
	r2 := throttle1(t, th, entry)
	require.True(t, r2.Throttled, "a second unset-Cost request should exhaust a capacity-1 bucket, same as Cost=1 would")
}

func TestInMemoryThrottlerExplicitCostConsumesMultipleUnits(t *testing.T) {
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	spend := deadhorse.RequestEntry{Key: "cost-key", Limit: deadhorse.Limit{Capacity: 5, EmissionInterval: time.Hour}, Cost: 3}
	r1 := throttle1(t, th, spend)
	require.False(t, r1.Throttled, "a cost-3 request into a capacity-5 bucket should be admitted")
	assert.EqualValues(t, 5, r1.Remaining, "Remaining reflects headroom before this request's own cost")

	peek := spend
	peek.Peek = true
	peek.Cost = 0
	after := throttle1(t, th, peek)
	assert.EqualValues(t, 2, after.Remaining, "after spending 3 of 5 units, remaining headroom should be 2")
}

func TestInMemoryThrottlerKeysAreIndependent(t *testing.T) {
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	entries := []deadhorse.RequestEntry{
		{Key: "tenant-a", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: time.Hour}},
		{Key: "tenant-b", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: time.Hour}},
	}
	// Exhaust tenant-a's bucket; tenant-b must be unaffected.
	throttle1(t, th, entries[0])
	results, err := th.Throttle(context.Background(), entries)
	require.NoError(t, err)
	assert.True(t, results[0].Throttled, "tenant-a's bucket should already be exhausted")
	assert.False(t, results[1].Throttled, "tenant-b should be unaffected by tenant-a's usage")
}

func TestInMemoryThrottlerZeroEmissionIntervalFailsClosed(t *testing.T) {
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	entry := deadhorse.RequestEntry{Key: "bad-config", Limit: deadhorse.Limit{Capacity: 10, EmissionInterval: 0}}
	r := throttle1(t, th, entry)
	assert.True(t, r.Throttled, "a zero emission interval must fail closed, not panic or admit")
	assert.NoError(t, r.Err, "a nonsensical limit fails closed, it doesn't error -- InMemoryThrottler never sets Err")
}

func TestInMemoryThrottlerGCResetsIdleKeys(t *testing.T) {
	const gcInterval = 20 * time.Millisecond
	th := NewInMemoryThrottler(4, gcInterval)
	defer th.Close()

	entry := deadhorse.RequestEntry{Key: "idle-tenant", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: time.Hour}}

	require.False(t, throttle1(t, th, entry).Throttled, "first touch should be admitted")
	require.True(t, throttle1(t, th, entry).Throttled, "second touch on a capacity-1, 1h-refill bucket should be throttled before any GC")

	time.Sleep(8 * gcInterval) // comfortably more than the 1-2 interval expiry window

	assert.False(t, throttle1(t, th, entry).Throttled, "after several idle GC intervals the key should have reset to a fresh bucket")
}

func TestInMemoryThrottlerKeyCountEstimate(t *testing.T) {
	th := NewInMemoryThrottler(4, time.Hour)
	defer th.Close()

	assert.Zero(t, th.keyCountEstimate())
	throttle1(t, th, deadhorse.RequestEntry{Key: "k1", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: 1}, Peek: true})
	throttle1(t, th, deadhorse.RequestEntry{Key: "k2", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: 1}, Peek: true})
	assert.Equal(t, 2, th.keyCountEstimate())
}

func TestInMemoryThrottlerRespectsCancelledContext(t *testing.T) {
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	entries := []deadhorse.RequestEntry{
		{Key: "k1", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: time.Hour}},
		{Key: "k2", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: time.Hour}},
	}
	results, err := th.Throttle(ctx, entries)
	assert.ErrorIs(t, err, context.Canceled)

	// The Throttler interface promises a result slice the same length as
	// entries, in the same order, even when the call-level error is
	// non-nil -- a caller that indexes results[i] regardless of err must
	// not see a nil slice here.
	require.Len(t, results, len(entries))
	for i, r := range results {
		assert.Equalf(t, entries[i].Key, r.Key, "result %d", i)
		assert.Truef(t, r.Throttled, "result %d: an unevaluated entry should fail closed", i)
		assert.ErrorIsf(t, r.Err, context.Canceled, "result %d", i)
	}
}
