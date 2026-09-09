package server

import "time"

// gcraCheck evaluates one leaky-bucket admission check using GCRA (the Generic
// Cell Rate Algorithm): the leaky bucket "as a meter", reformulated so the
// only state that needs to persist between calls is a single timestamp
// instead of a decaying counter that has to be refilled on every read.
//
// tat and now are instants in nanoseconds since the same epoch (0 for tat
// means "never seen," which is deliberately treated the same as "now" -- a
// fresh key starts with a fully compliant history, i.e. an empty bucket).
// capacity is the bucket size in cost units, emissionInterval is how long it
// takes to leak one unit, and cost is how many units this request consumes.
//
// It returns whether the request is throttled, the bucket's current headroom
// in cost units (independent of whether this particular request fit), how
// long until this request would fit if it didn't, and the TAT the caller
// should persist if the request was admitted and is being applied for real
// (gcraCheck itself never mutates anything -- it's a pure function).
func gcraCheck(tat, now, capacity int64, emissionInterval time.Duration, cost int64) (throttled bool, remaining int64, retryAfter time.Duration, admittedTAT int64) {
	if emissionInterval <= 0 {
		// No sane leak rate: fail closed rather than divide by zero.
		return true, 0, 0, tat
	}
	ei := int64(emissionInterval)

	effectiveTAT := max(tat, now)

	level := effectiveTAT - now // ns of "debt" currently sitting in the bucket
	remainingUnits := max(capacity-ceilDiv(level, ei), 0)

	candidateTAT := effectiveTAT + cost*ei
	maxDebt := capacity * ei

	if candidateTAT-now > maxDebt {
		return true, remainingUnits, time.Duration(candidateTAT - now - maxDebt), tat
	}
	return false, remainingUnits, 0, candidateTAT
}

func ceilDiv(a, b int64) int64 {
	if a <= 0 {
		return 0
	}
	return (a + b - 1) / b
}
