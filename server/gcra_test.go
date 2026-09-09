package server

import (
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
	// A bucket that's already full asked for 3 units at once needs 3
	// emission intervals of room, so retryAfterNs should scale with cost.
	const capacity, emissionInterval = 1, 1000

	_, _, _, tat := gcraCheck(0, 0, capacity, emissionInterval, 1) // fill the bucket
	throttled, _, retryAfterNs, _ := gcraCheck(tat, 0, capacity, emissionInterval, 3)
	require.True(t, throttled)
	assert.EqualValues(t, 3*emissionInterval, retryAfterNs)
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
