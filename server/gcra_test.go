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
	throttled, remaining, retryAfterNs, admittedTAT := gcraCheck(0, 1000, 10, 1, 1000, 1)
	require.False(t, throttled, "fresh key should not be throttled")
	assert.EqualValues(t, 10, remaining, "fresh key should report a full bucket")
	assert.Zero(t, retryAfterNs, "retryAfterNs should be 0 when not throttled")
	assert.EqualValues(t, 1000+1000, admittedTAT)
}

func TestGcraCheckBurstThenThrottle(t *testing.T) {
	// capacity=1, rate=1 unit/1000ns: a single unit of burst. The first
	// request at now=0 must be admitted; a second request at the same
	// instant must not be (the bucket has no room until the first unit
	// leaks out).
	const capacity, units, period = 1, 1, 1000

	throttled, _, _, tat := gcraCheck(0, 0, capacity, units, period, 1)
	require.False(t, throttled, "first request into an empty capacity-1 bucket should be admitted")

	throttled, remaining, retryAfterNs, _ := gcraCheck(tat, 0, capacity, units, period, 1)
	require.True(t, throttled, "second back-to-back request into a capacity-1 bucket should be throttled")
	assert.Zero(t, remaining, "bucket should be full")
	assert.EqualValues(t, period, retryAfterNs, "should be exactly one leak period away")
}

func TestGcraCheckRetryAfterAccountsForCost(t *testing.T) {
	// A bucket that's already full asked for 3 units at once (still within
	// capacity, so genuinely satisfiable once enough has drained) needs 3
	// leak periods of room, so retryAfterNs should scale with cost.
	const capacity, units, period = 5, 1, 1000

	_, _, _, tat := gcraCheck(0, 0, capacity, units, period, capacity) // fill the bucket completely
	throttled, _, retryAfterNs, _ := gcraCheck(tat, 0, capacity, units, period, 3)
	require.True(t, throttled)
	assert.EqualValues(t, 3*period, retryAfterNs)
}

func TestGcraCheckCostExceedingCapacityIsPermanentlyUnsatisfiable(t *testing.T) {
	// A single request whose cost exceeds the bucket's capacity can never
	// be admitted, no matter how long the caller waits: RetryAfter must
	// signal "don't bother retrying" (0), not a positive-looking value
	// that never actually resolves.
	const capacity, units, period = 1, 1, 1000

	throttled, _, retryAfterNs, tat := gcraCheck(0, 0, capacity, units, period, 5)
	require.True(t, throttled)
	assert.Zero(t, retryAfterNs, "an unsatisfiable request must not report a misleading RetryAfter")

	// Waiting an arbitrary amount of time and retrying changes nothing:
	// still throttled, still RetryAfter=0.
	throttled, _, retryAfterNs, _ = gcraCheck(tat, 1_000_000, capacity, units, period, 5)
	require.True(t, throttled)
	assert.Zero(t, retryAfterNs)
}

func TestGcraCheckRemainingReflectsCurrentLevelNotThisRequest(t *testing.T) {
	// remaining is the bucket's headroom before this request's own cost is
	// applied -- so a request that itself gets admitted still reports the
	// pre-request remaining, not post-consumption.
	const capacity, units, period = 5, 1, 1000

	throttled, remaining, _, _ := gcraCheck(0, 0, capacity, units, period, 3)
	require.False(t, throttled, "bucket was empty, cost 3 <= capacity 5")
	assert.EqualValues(t, capacity, remaining, "bucket was empty before this request")
}

func TestGcraCheckLeaksOverTime(t *testing.T) {
	// A bucket filled to capacity at t=0 should have exactly one unit of
	// room by the time one period has passed.
	const capacity, units, period = 3, 1, 1000

	tat := int64(0)
	for i := 0; i < int(capacity); i++ {
		var throttled bool
		throttled, _, _, tat = gcraCheck(tat, 0, capacity, units, period, 1)
		require.Falsef(t, throttled, "request %d should have been admitted while filling the bucket", i)
	}

	throttled, _, _, _ := gcraCheck(tat, 0, capacity, units, period, 1)
	require.True(t, throttled, "bucket should be exactly full immediately after filling it")

	throttled, remaining, _, _ := gcraCheck(tat, period, capacity, units, period, 1)
	require.False(t, throttled, "one period later, one unit should have leaked out")
	assert.EqualValues(t, 1, remaining, "only one unit has leaked")
}

func TestGcraCheckHighRateViaUnitsGreaterThanOne(t *testing.T) {
	// A duration-per-unit rate tops out at 1 unit/ns -- there's no such
	// thing as a sub-nanosecond time.Duration. Units lets the rate scale
	// past that: 4 units draining every 1ns is 4e9 units/sec, unreachable
	// with units=1 no matter how small period gets.
	const capacity, units, period = 4, 4, 1

	throttled, remaining, _, tat := gcraCheck(0, 0, capacity, units, period, 4)
	require.False(t, throttled, "a burst of 4 units should fit a capacity-4 bucket in one shot")
	assert.EqualValues(t, capacity, remaining, "bucket was empty before this request")

	throttled, remaining, retryAfterNs, _ := gcraCheck(tat, 0, capacity, units, period, 1)
	require.True(t, throttled, "the bucket has no room left immediately after filling it")
	assert.Zero(t, remaining)
	assert.EqualValues(t, 1, retryAfterNs, "should be exactly one leak period away")

	throttled, remaining, _, _ = gcraCheck(tat, 1, capacity, units, period, 1)
	require.False(t, throttled, "1ns later, all 4 units should have drained -- room for a full new burst")
	assert.EqualValues(t, capacity, remaining)
}

func TestGcraCheckZeroOrNegativePeriodFailsClosed(t *testing.T) {
	// A zero or negative leak rate is nonsensical; gcraCheck must refuse the
	// request rather than divide by zero.
	for _, period := range []time.Duration{0, -1, -1000} {
		throttled, remaining, retryAfterNs, admittedTAT := gcraCheck(0, 1000, 10, 1, period, 1)
		assert.Truef(t, throttled, "period=%d: expected fail-closed (throttled)", period)
		assert.Zerof(t, remaining, "period=%d", period)
		assert.Zerof(t, retryAfterNs, "period=%d", period)
		assert.Zerof(t, admittedTAT, "period=%d: tat should be left unchanged", period)
	}
}

func TestGcraCheckZeroOrNegativeUnitsFailsClosed(t *testing.T) {
	// units is the rate's denominator too -- callers must normalize a
	// caller-supplied zero/negative Units to 1 before calling in (see
	// deadhorse.EffectiveUnits); gcraCheck itself still refuses rather than
	// divide by zero if one slips through.
	for _, units := range []int64{0, -1, -1000} {
		throttled, remaining, retryAfterNs, admittedTAT := gcraCheck(0, 1000, 10, units, 1000, 1)
		assert.Truef(t, throttled, "units=%d: expected fail-closed (throttled)", units)
		assert.Zerof(t, remaining, "units=%d", units)
		assert.Zerof(t, retryAfterNs, "units=%d", units)
		assert.Zerof(t, admittedTAT, "units=%d: tat should be left unchanged", units)
	}
}

func TestGcraCheckOverflowingCostFailsClosedInsteadOfWrapping(t *testing.T) {
	// cost*period overflows int64 here (2^62 * 4 wraps to 0 mod 2^64) -- if
	// gcraCheck let that wrap silently through, the request would look like
	// it cost nothing and get admitted for free instead of being throttled.
	// Both capacity and cost arrive over the wire with no upper bound (see
	// textserver.parseEntry), so this is reachable from a crafted request,
	// not just a theoretical edge case.
	const capacity, units, period = 1, 1, 4
	const hugeCost = int64(1) << 62

	throttled, remaining, retryAfterNs, admittedTAT := gcraCheck(0, 1000, capacity, units, period, hugeCost)
	assert.True(t, throttled, "an overflowing cost must fail closed, not bypass throttling")
	assert.Zero(t, remaining)
	assert.Zero(t, retryAfterNs)
	assert.EqualValues(t, 0, admittedTAT, "TAT must be left unchanged, not silently advanced")
}

func TestGcraCheckOverflowingCapacityFailsClosed(t *testing.T) {
	// capacity*period overflows int64 here for the same reason as the cost
	// case above.
	const units, period = 1, 1 << 40
	const hugeCapacity = int64(1) << 40

	throttled, remaining, retryAfterNs, admittedTAT := gcraCheck(0, 1000, hugeCapacity, units, period, 1)
	assert.True(t, throttled, "an overflowing capacity*period must fail closed")
	assert.Zero(t, remaining)
	assert.Zero(t, retryAfterNs)
	assert.EqualValues(t, 0, admittedTAT)
}

func TestGcraCheckOverflowingCapacityTimesPeriodOverUnitsFailsClosed(t *testing.T) {
	// Same overflow hazard as the units=1 case above, but through the
	// units>1 (mulDivFloor) path: capacity*period alone already overflows
	// the 128-bit intermediate before units even gets to divide it back
	// down.
	const units = 2
	const period = 1 << 40
	const hugeCapacity = int64(1) << 40

	throttled, remaining, retryAfterNs, admittedTAT := gcraCheck(0, 1000, hugeCapacity, units, period, 1)
	assert.True(t, throttled, "an overflowing capacity*period/units must fail closed")
	assert.Zero(t, remaining)
	assert.Zero(t, retryAfterNs)
	assert.EqualValues(t, 0, admittedTAT)
}

func TestGcraCheckNegativeCapacityOrCostFailsClosed(t *testing.T) {
	// Neither should reach gcraCheck in practice (textserver.parseEntry and
	// EffectiveCost both reject/normalize negatives upstream), but gcraCheck
	// itself must still refuse rather than let a negative operand skew the
	// arithmetic, same as it does for a non-sane period.
	throttled, remaining, retryAfterNs, admittedTAT := gcraCheck(0, 1000, -1, 1, 1000, 1)
	assert.True(t, throttled, "negative capacity must fail closed")
	assert.Zero(t, remaining)
	assert.Zero(t, retryAfterNs)
	assert.EqualValues(t, 0, admittedTAT)

	throttled, remaining, retryAfterNs, admittedTAT = gcraCheck(0, 1000, 10, 1, 1000, -1)
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
	const capacity, units, period = 1, 1, 1000

	_, _, _, tat := gcraCheck(0, 0, capacity, units, period, 1)
	throttled, _, _, tatAfterRejection := gcraCheck(tat, 0, capacity, units, period, 1)
	require.True(t, throttled)
	assert.Equal(t, tat, tatAfterRejection, "TAT must be left unchanged on rejection")
}

func TestGcraCheckDebtOverflowFallbackStillThrottles(t *testing.T) {
	// When units > 1, computing debtUnits goes through mulDivCeil(level,
	// units, period) -- a different overflow hazard than the maxDebt/costNs
	// checks above, since it's level (derived from a stored tat, not a
	// caller-supplied Limit) that gets multiplied by units this time.
	// gcraCheck's own comment argues the "capacity+1" sentinel it falls
	// back to in that case is safe because the request is guaranteed to be
	// throttled by the maxDebt comparison regardless of the sentinel's
	// exact value -- verified here with concrete numbers rather than just
	// trusting the comment.
	const capacity, units, period = int64(1) << 62, 3, time.Duration(1)
	const level = int64(7_000_000_000_000_000_000) // chosen so level*units overflows the 128-bit intermediate
	const cost = 1

	maxDebt, ok := mulDivFloor(capacity, int64(period), units)
	require.True(t, ok, "maxDebt itself must not overflow -- that's what makes this scenario interesting")
	costNs, ok := mulDivCeil(cost, int64(period), units)
	require.True(t, ok, "costNs itself must not overflow either")
	_, debtOverflowed := mulDivCeil(level, units, int64(period))
	require.False(t, debtOverflowed, "level*units must actually overflow for this test to exercise the fallback branch")

	throttled, remaining, retryAfterNs, admittedTAT := gcraCheck(level, 0, capacity, units, period, cost)
	require.True(t, throttled, "level is already far beyond maxDebt; the overflow fallback must not accidentally admit")
	assert.Zero(t, remaining, "the overflow sentinel reports a fully depleted bucket")
	assert.EqualValues(t, level+costNs-maxDebt, retryAfterNs,
		"retryAfter should still reflect the real (non-overflowing) maxDebt comparison, not the sentinel")
	assert.EqualValues(t, level, admittedTAT, "rejection must not advance TAT")
}

func TestMulDivFloorAndCeil(t *testing.T) {
	got, ok := mulDivFloor(7, 3, 2)
	require.True(t, ok)
	assert.EqualValues(t, 10, got, "floor(7*3/2) = floor(10.5) = 10")

	got, ok = mulDivCeil(7, 3, 2)
	require.True(t, ok)
	assert.EqualValues(t, 11, got, "ceil(7*3/2) = ceil(10.5) = 11")

	got, ok = mulDivFloor(4, 3, 2)
	require.True(t, ok)
	assert.EqualValues(t, 6, got, "an exact division has nothing to round in either direction")

	got, ok = mulDivCeil(4, 3, 2)
	require.True(t, ok)
	assert.EqualValues(t, 6, got)

	_, ok = mulDivFloor(math.MaxInt64, math.MaxInt64, 1)
	assert.False(t, ok, "a product this large can't fit back into an int64 no matter the divisor")

	_, ok = mulDivFloor(-1, 3, 2)
	assert.False(t, ok, "negative operands are refused rather than misinterpreted as unsigned")

	_, ok = mulDivFloor(1, 3, 0)
	assert.False(t, ok, "a zero divisor is refused rather than dividing by zero")
}
