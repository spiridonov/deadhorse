package server

import (
	"context"
	"fmt"
	"math/rand"
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
func throttle1(t *testing.T, th Throttler, e deadhorse.RequestEntry) deadhorse.ResponseEntry {
	t.Helper()
	got, err := th.Throttle(context.Background(), []deadhorse.RequestEntry{e})
	require.NoError(t, err)
	require.Len(t, got, 1)
	return got[0]
}

func TestInMemoryThrottlerBurstThenThrottle(t *testing.T) {
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	entry := deadhorse.RequestEntry{Key: "org:acme:writes", Limit: deadhorse.Limit{Capacity: 2, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}}

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
	// Mirrors the reference scenario: capacity=100, rate=1 unit/10ms
	// (100 req/s sustained, burst of 100). Each request is its own Throttle
	// call (its own line, its own transaction) -- unlike the batch-of-101
	// this test used before all-or-none grouping existed, sending all 101
	// together in one call would now make the 101st drag the other 100
	// down with it (see TestInMemoryThrottlerRealGroupIsAllOrNone) rather
	// than demonstrating "burst of 100, then throttled" at all.
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	entry := deadhorse.RequestEntry{Key: "org:acme:writes", Limit: deadhorse.Limit{Capacity: 100, Rate: deadhorse.Rate{Units: 1, Period: 10 * time.Millisecond}}}

	const n = 101
	throttledCount := 0
	var lastRetryAfter time.Duration
	for i := 0; i < n; i++ {
		r := throttle1(t, th, entry)
		if r.Throttled {
			throttledCount++
			lastRetryAfter = r.RetryAfter
		}
	}
	require.Equal(t, 1, throttledCount, "expected exactly 1 throttled request (the 101st)")
	// Roughly one leak period: unlike the single shared "now" a
	// batch-of-101 gave this test before, 101 separate calls each take
	// their own real time.Now(), so a little of the period has already
	// drained by the time the 101st call happens.
	assert.InDelta(t, 10*time.Millisecond, lastRetryAfter, float64(5*time.Millisecond), "roughly one leak period")
	assert.Positive(t, lastRetryAfter)
}

func TestInMemoryThrottlerPeekDoesNotConsume(t *testing.T) {
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	peekEntry := deadhorse.RequestEntry{Key: "peek-key", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}, Peek: true}
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

	entry := deadhorse.RequestEntry{Key: "default-mode", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}}
	r1 := throttle1(t, th, entry)
	require.False(t, r1.Throttled, "first request into an empty capacity-1 bucket should be admitted")
	r2 := throttle1(t, th, entry)
	require.True(t, r2.Throttled, "second request should be throttled, proving the first one actually consumed the bucket")
}

func TestInMemoryThrottlerUnsetCostDefaultsToOne(t *testing.T) {
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	entry := deadhorse.RequestEntry{Key: "cost-default", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}}
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

	spend := deadhorse.RequestEntry{Key: "cost-key", Limit: deadhorse.Limit{Capacity: 5, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}, Cost: 3}
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
		{Key: "tenant-a", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}},
		{Key: "tenant-b", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}},
	}
	// Exhaust tenant-a's bucket with its own call; tenant-b, checked with a
	// separate call of its own, must be unaffected. Two different keys
	// checked in two different calls are exactly as independent as they
	// were before all-or-none grouping existed -- that grouping only ever
	// applies to entries sharing one call (see
	// TestInMemoryThrottlerRealGroupIsAllOrNone for what happens when they
	// share one).
	throttle1(t, th, entries[0])
	require.True(t, throttle1(t, th, entries[0]).Throttled, "tenant-a's bucket should already be exhausted")
	assert.False(t, throttle1(t, th, entries[1]).Throttled, "tenant-b should be unaffected by tenant-a's usage")
}

func TestInMemoryThrottlerRealGroupIsAllOrNone(t *testing.T) {
	// The README's org/user worked example: two different keys, checked
	// together in one call. Before all-or-none grouping, one passing and
	// one failing would report exactly that (one throttled, one not).
	// Now, since they share one call/line, a single denial denies the
	// whole group -- see Throttler and ResponseEntry.Throttled.
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	// Exhaust tenant-a on its own first, so it's the one that fails.
	tenantA := deadhorse.RequestEntry{Key: "tenant-a", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}}
	tenantB := deadhorse.RequestEntry{Key: "tenant-b", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}}
	require.False(t, throttle1(t, th, tenantA).Throttled, "warm-up call should exhaust tenant-a's one unit of capacity")

	results, err := th.Throttle(context.Background(), []deadhorse.RequestEntry{tenantA, tenantB})
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.True(t, results[0].Throttled, "tenant-a's own bucket is exhausted")
	assert.True(t, results[1].Throttled, "tenant-b must also be denied: the group is all-or-none")

	// Because the group was denied, tenant-b's capacity must not have been
	// spent -- nothing in a failed group gets committed.
	assert.False(t, throttle1(t, th, tenantB).Throttled, "tenant-b's own bucket should still be fresh, since the earlier group never committed")
}

func TestInMemoryThrottlerRealGroupCommitsOnlyWhenAllAdmit(t *testing.T) {
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	tenantA := deadhorse.RequestEntry{Key: "tenant-a-ok", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}}
	tenantB := deadhorse.RequestEntry{Key: "tenant-b-ok", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}}

	results, err := th.Throttle(context.Background(), []deadhorse.RequestEntry{tenantA, tenantB})
	require.NoError(t, err)
	assert.False(t, results[0].Throttled)
	assert.False(t, results[1].Throttled)

	// Both entries actually consumed their bucket now that the group as a
	// whole admitted.
	assert.True(t, throttle1(t, th, tenantA).Throttled, "tenant-a's unit should have been spent by the admitted group")
	assert.True(t, throttle1(t, th, tenantB).Throttled, "tenant-b's unit should have been spent by the admitted group")
}

func TestInMemoryThrottlerPeekIndependentOfRealGroup(t *testing.T) {
	// A Peek entry sharing a call with a failing Real group must be
	// completely unaffected by it: it neither gates nor is gated (see
	// RequestEntry.Peek).
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	real := deadhorse.RequestEntry{Key: "real-key", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}}
	require.False(t, throttle1(t, th, real).Throttled, "warm-up call should exhaust real-key's one unit")

	peek := deadhorse.RequestEntry{Key: "peek-key", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}, Peek: true}
	results, err := th.Throttle(context.Background(), []deadhorse.RequestEntry{real, peek})
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.True(t, results[0].Throttled, "real-key's group fails on its own exhausted bucket")
	assert.False(t, results[1].Throttled, "the Peek entry must be unaffected by the failing Real group sharing its call")

	// And the Peek entry must not have consumed peek-key's capacity either,
	// same as always.
	assert.False(t, throttle1(t, th, deadhorse.RequestEntry{Key: "peek-key", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}}).Throttled)
}

func TestInMemoryThrottlerRealGroupChainsRepeatedKey(t *testing.T) {
	// Two Real entries for the *same* key in one call must chain the way
	// two sequential calls would: the second sees the first's provisional
	// effect. Capacity 2 admits both; capacity 1 would deny the whole group
	// (the second entry alone exceeds it), which is covered by
	// TestInMemoryThrottlerRealGroupIsAllOrNone-style denial and not
	// repeated here.
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	entry := deadhorse.RequestEntry{Key: "repeated-key", Limit: deadhorse.Limit{Capacity: 2, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}}
	results, err := th.Throttle(context.Background(), []deadhorse.RequestEntry{entry, entry})
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.False(t, results[0].Throttled)
	assert.False(t, results[1].Throttled)
	assert.EqualValues(t, 2, results[0].Remaining, "first entry sees the bucket before either consumed anything")
	assert.EqualValues(t, 1, results[1].Remaining, "second entry sees the first entry's provisional consumption")

	// Both units should now be spent.
	assert.True(t, throttle1(t, th, entry).Throttled)
}

func TestInMemoryThrottlerRealGroupNoDeadlockWithOverlappingKeys(t *testing.T) {
	// Many concurrent calls, each touching a random subset of a small key
	// space in a random order -- if evaluateRealGroup didn't lock every
	// touched key in a fixed (sorted) order, two calls could deadlock by
	// acquiring the same two keys in opposite orders. The test's real
	// assertion is that it finishes at all, within the deadline.
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	keys := make([]string, 8)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	limit := deadhorse.Limit{Capacity: 1_000_000, Rate: deadhorse.Rate{Units: 1, Period: time.Nanosecond}}

	const goroutines = 50
	const callsPerGoroutine = 50
	done := make(chan struct{}, goroutines)
	for g := 0; g < goroutines; g++ {
		go func(seed int) {
			defer func() { done <- struct{}{} }()
			rng := rand.New(rand.NewSource(int64(seed)))
			for c := 0; c < callsPerGoroutine; c++ {
				n := 1 + rng.Intn(len(keys))
				order := rng.Perm(len(keys))[:n]
				entries := make([]deadhorse.RequestEntry, n)
				for i, idx := range order {
					entries[i] = deadhorse.RequestEntry{Key: keys[idx], Limit: limit}
				}
				_, err := th.Throttle(context.Background(), entries)
				assert.NoError(t, err)
			}
		}(g)
	}

	deadline := time.After(10 * time.Second)
	for g := 0; g < goroutines; g++ {
		select {
		case <-done:
		case <-deadline:
			t.Fatal("timed out -- likely deadlock in evaluateRealGroup's multi-key locking")
		}
	}
}

func TestInMemoryThrottlerZeroPeriodFailsClosed(t *testing.T) {
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	entry := deadhorse.RequestEntry{Key: "bad-config", Limit: deadhorse.Limit{Capacity: 10, Rate: deadhorse.Rate{Units: 1, Period: 0}}}
	r := throttle1(t, th, entry)
	assert.True(t, r.Throttled, "a zero leak period must fail closed, not panic or admit")
	assert.NoError(t, r.Err, "a nonsensical limit fails closed, it doesn't error -- InMemoryThrottler never sets Err")
}

func TestInMemoryThrottlerGCResetsIdleKeys(t *testing.T) {
	const gcInterval = 20 * time.Millisecond
	th := NewInMemoryThrottler(4, gcInterval)
	defer th.Close()

	entry := deadhorse.RequestEntry{Key: "idle-tenant", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}}

	require.False(t, throttle1(t, th, entry).Throttled, "first touch should be admitted")
	require.True(t, throttle1(t, th, entry).Throttled, "second touch on a capacity-1, 1h-refill bucket should be throttled before any GC")

	time.Sleep(8 * gcInterval) // comfortably more than the 1-2 interval expiry window

	assert.False(t, throttle1(t, th, entry).Throttled, "after several idle GC intervals the key should have reset to a fresh bucket")
}

func TestInMemoryThrottlerKeyCountEstimate(t *testing.T) {
	th := NewInMemoryThrottler(4, time.Hour)
	defer th.Close()

	assert.Zero(t, th.keyCountEstimate())
	throttle1(t, th, deadhorse.RequestEntry{Key: "k1", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: 1}}, Peek: true})
	throttle1(t, th, deadhorse.RequestEntry{Key: "k2", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: 1}}, Peek: true})
	assert.Equal(t, 2, th.keyCountEstimate())
}

func TestInMemoryThrottlerRespectsCancelledContext(t *testing.T) {
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	entries := []deadhorse.RequestEntry{
		{Key: "k1", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}},
		{Key: "k2", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}},
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
