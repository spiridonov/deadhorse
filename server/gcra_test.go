package server

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCeilDiv(t *testing.T) {
	cases := []struct {
		a, b, want int64
	}{
		{0, 5, 0},
		{-3, 5, 0},
		{1, 5, 1},
		{4, 5, 1},
		{5, 5, 1},
		{6, 5, 2},
		{10, 5, 2},
		{11, 5, 3},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, ceilDiv(c.a, c.b), "ceilDiv(%d, %d)", c.a, c.b)
	}
}

func TestGcraCheckFreshKeyIsFullyCompliant(t *testing.T) {
	// tat=0 means "never seen"; it must be treated exactly like a key whose
	// bucket is already empty at time now, not like an ancient, expired debt.
	throttled, remaining, retryAfterNs, admittedTAT := gcraCheck(0, 1000, 10, 1000, 1)
	require.False(t, throttled, "fresh key should not be throttled")
	assert.EqualValues(t, 10, remaining, "fresh key should report a full bucket")
	assert.Zero(t, retryAfterNs, "retryAfterNs should be 0 when not throttled")
	assert.EqualValues(t, 1000+1000, admittedTAT)
}

func TestGcraCheckBurstThenThrottle(t *testing.T) {
	// capacity=1, emissionInterval=1000ns: a single unit of burst. The first
	// request at now=0 must be admitted; a second request at the same
	// instant must not be (the bucket has no room until the first unit
	// leaks out).
	const capacity, emissionInterval = 1, 1000

	throttled, _, _, tat := gcraCheck(0, 0, capacity, emissionInterval, 1)
	require.False(t, throttled, "first request into an empty capacity-1 bucket should be admitted")

	throttled, remaining, retryAfterNs, _ := gcraCheck(tat, 0, capacity, emissionInterval, 1)
	require.True(t, throttled, "second back-to-back request into a capacity-1 bucket should be throttled")
	assert.Zero(t, remaining, "bucket should be full")
	assert.EqualValues(t, emissionInterval, retryAfterNs, "should be exactly one leak interval away")
}

func TestGcraCheckRetryAfterAccountsForCost(t *testing.T) {
	// A bucket that's already full asked for 3 units at once (still within
	// capacity, so genuinely satisfiable once enough has drained) needs 3
	// emission intervals of room, so retryAfterNs should scale with cost.
	const capacity, emissionInterval = 5, 1000

	_, _, _, tat := gcraCheck(0, 0, capacity, emissionInterval, capacity) // fill the bucket completely
	throttled, _, retryAfterNs, _ := gcraCheck(tat, 0, capacity, emissionInterval, 3)
	require.True(t, throttled)
	assert.EqualValues(t, 3*emissionInterval, retryAfterNs)
}

func TestGcraCheckCostExceedingCapacityIsPermanentlyUnsatisfiable(t *testing.T) {
	// A single request whose cost exceeds the bucket's capacity can never
	// be admitted, no matter how long the caller waits: RetryAfter must
	// signal "don't bother retrying" (0), not a positive-looking value
	// that never actually resolves.
	const capacity, emissionInterval = 1, 1000

	throttled, _, retryAfterNs, tat := gcraCheck(0, 0, capacity, emissionInterval, 5)
	require.True(t, throttled)
	assert.Zero(t, retryAfterNs, "an unsatisfiable request must not report a misleading RetryAfter")

	// Waiting an arbitrary amount of time and retrying changes nothing:
	// still throttled, still RetryAfter=0.
	throttled, _, retryAfterNs, _ = gcraCheck(tat, 1_000_000, capacity, emissionInterval, 5)
	require.True(t, throttled)
	assert.Zero(t, retryAfterNs)
}

func TestGcraCheckRemainingReflectsCurrentLevelNotThisRequest(t *testing.T) {
	// remaining is the bucket's headroom before this request's own cost is
	// applied -- so a request that itself gets admitted still reports the
	// pre-request remaining, not post-consumption.
	const capacity, emissionInterval = 5, 1000

	throttled, remaining, _, _ := gcraCheck(0, 0, capacity, emissionInterval, 3)
	require.False(t, throttled, "bucket was empty, cost 3 <= capacity 5")
	assert.EqualValues(t, capacity, remaining, "bucket was empty before this request")
}

func TestGcraCheckLeaksOverTime(t *testing.T) {
	// A bucket filled to capacity at t=0 should have exactly one unit of
	// room by the time one emissionInterval has passed.
	const capacity, emissionInterval = 3, 1000

	tat := int64(0)
	for i := 0; i < int(capacity); i++ {
		var throttled bool
		throttled, _, _, tat = gcraCheck(tat, 0, capacity, emissionInterval, 1)
		require.Falsef(t, throttled, "request %d should have been admitted while filling the bucket", i)
	}

	throttled, _, _, _ := gcraCheck(tat, 0, capacity, emissionInterval, 1)
	require.True(t, throttled, "bucket should be exactly full immediately after filling it")

	throttled, remaining, _, _ := gcraCheck(tat, emissionInterval, capacity, emissionInterval, 1)
	require.False(t, throttled, "one emissionInterval later, one unit should have leaked out")
	assert.EqualValues(t, 1, remaining, "only one unit has leaked")
}

func TestGcraCheckZeroOrNegativeEmissionIntervalFailsClosed(t *testing.T) {
	// A zero or negative leak rate is nonsensical; gcraCheck must refuse the
	// request rather than divide by zero.
	for _, ei := range []time.Duration{0, -1, -1000} {
		throttled, remaining, retryAfterNs, admittedTAT := gcraCheck(0, 1000, 10, ei, 1)
		assert.Truef(t, throttled, "emissionInterval=%d: expected fail-closed (throttled)", ei)
		assert.Zerof(t, remaining, "emissionInterval=%d", ei)
		assert.Zerof(t, retryAfterNs, "emissionInterval=%d", ei)
		assert.Zerof(t, admittedTAT, "emissionInterval=%d: tat should be left unchanged", ei)
	}
}

func TestGcraCheckOverflowingCostFailsClosedInsteadOfWrapping(t *testing.T) {
	// cost*emissionInterval overflows int64 here (2^62 * 4 wraps to 0 mod
	// 2^64) -- if gcraCheck let that wrap silently through, the request
	// would look like it cost nothing and get admitted for free instead of
	// being throttled. Both capacity and cost arrive over the wire with no
	// upper bound (see textserver.parseEntry), so this is reachable from a
	// crafted request, not just a theoretical edge case.
	const capacity, emissionInterval = 1, 4
	const hugeCost = int64(1) << 62

	throttled, remaining, retryAfterNs, admittedTAT := gcraCheck(0, 1000, capacity, emissionInterval, hugeCost)
	assert.True(t, throttled, "an overflowing cost must fail closed, not bypass throttling")
	assert.Zero(t, remaining)
	assert.Zero(t, retryAfterNs)
	assert.EqualValues(t, 0, admittedTAT, "TAT must be left unchanged, not silently advanced")
}

func TestGcraCheckOverflowingCapacityFailsClosed(t *testing.T) {
	// capacity*emissionInterval overflows int64 here for the same reason as
	// the cost case above.
	const emissionInterval = 1 << 40
	const hugeCapacity = int64(1) << 40

	throttled, remaining, retryAfterNs, admittedTAT := gcraCheck(0, 1000, hugeCapacity, emissionInterval, 1)
	assert.True(t, throttled, "an overflowing capacity*emissionInterval must fail closed")
	assert.Zero(t, remaining)
	assert.Zero(t, retryAfterNs)
	assert.EqualValues(t, 0, admittedTAT)
}

func TestGcraCheckNegativeCapacityOrCostFailsClosed(t *testing.T) {
	// Neither should reach gcraCheck in practice (textserver.parseEntry and
	// EffectiveCost both reject/normalize negatives upstream), but gcraCheck
	// itself must still refuse rather than let a negative operand skew the
	// arithmetic, same as it does for a non-sane emissionInterval.
	throttled, remaining, retryAfterNs, admittedTAT := gcraCheck(0, 1000, -1, 1000, 1)
	assert.True(t, throttled, "negative capacity must fail closed")
	assert.Zero(t, remaining)
	assert.Zero(t, retryAfterNs)
	assert.EqualValues(t, 0, admittedTAT)

	throttled, remaining, retryAfterNs, admittedTAT = gcraCheck(0, 1000, 10, 1000, -1)
	assert.True(t, throttled, "negative cost must fail closed")
	assert.Zero(t, remaining)
	assert.Zero(t, retryAfterNs)
	assert.EqualValues(t, 0, admittedTAT)
}

func TestCeilDivNearMaxInt64DoesNotOverflow(t *testing.T) {
	// The naive (a + b - 1) / b formulation overflows here: a+b-1 exceeds
	// math.MaxInt64 and wraps negative. The fixed formulation must not.
	got := ceilDiv(math.MaxInt64-2, 1000)
	assert.EqualValues(t, 9223372036854776, got)
	assert.Positive(t, got, "must not wrap negative on overflow")
}

func TestGcraCheckRejectionDoesNotAdvanceTAT(t *testing.T) {
	// gcraCheck is pure and never mutates anything by itself; a rejected
	// request must report the same TAT the caller already had, so that a
	// caller who (incorrectly) persisted it on every call regardless of
	// throttled would still be safe.
	const capacity, emissionInterval = 1, 1000

	_, _, _, tat := gcraCheck(0, 0, capacity, emissionInterval, 1)
	throttled, _, _, tatAfterRejection := gcraCheck(tat, 0, capacity, emissionInterval, 1)
	require.True(t, throttled)
	assert.Equal(t, tat, tatAfterRejection, "TAT must be left unchanged on rejection")
}
