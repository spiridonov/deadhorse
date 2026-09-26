package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCircuitBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	b := newCircuitBreaker(3, time.Hour)

	for i := 0; i < 2; i++ {
		require.True(t, b.allow(), "should still be closed before the threshold is reached")
		b.recordFailure()
	}
	require.True(t, b.allow(), "still closed: only 2 of 3 failures recorded")
	b.recordFailure()

	assert.False(t, b.allow(), "3 consecutive failures should have opened the breaker")
}

func TestCircuitBreakerSuccessResetsConsecutiveFailures(t *testing.T) {
	b := newCircuitBreaker(3, time.Hour)

	b.recordFailure()
	b.recordFailure()
	b.recordSuccess()
	b.recordFailure()
	b.recordFailure()

	// Without the reset, this would be the 4th failure in a row and the
	// breaker would already be open.
	assert.True(t, b.allow(), "a success in between should have reset the consecutive count")
}

func TestCircuitBreakerAllowsTrialAfterCooldown(t *testing.T) {
	b := newCircuitBreaker(1, 20*time.Millisecond)

	b.recordFailure()
	require.False(t, b.allow(), "should be open immediately after tripping")

	require.Eventually(t, b.allow, time.Second, time.Millisecond, "should allow a trial once the cooldown elapses")
}

func TestCircuitBreakerTrialSuccessCloses(t *testing.T) {
	b := newCircuitBreaker(2, 10*time.Millisecond)
	b.recordFailure()
	b.recordFailure()
	require.Eventually(t, b.allow, time.Second, time.Millisecond, "trial should become available")

	b.recordSuccess()

	require.True(t, b.allow(), "breaker should be closed after a successful trial")
	// One more failure alone must not reopen it -- proof recordSuccess
	// actually reset the consecutive-failure count, not just the open flag.
	b.recordFailure()
	assert.True(t, b.allow(), "a single failure right after a successful trial must not immediately reopen a threshold-2 breaker")
}

func TestCircuitBreakerTrialFailureReopensAndRestartsCooldown(t *testing.T) {
	b := newCircuitBreaker(1, 20*time.Millisecond)
	b.recordFailure()
	// Eventually's own polling of allow() is what claims the trial slot --
	// the poll that finally returns true *is* the trial being let through.
	require.Eventually(t, b.allow, time.Second, time.Millisecond, "trial should become available")
	b.recordFailure()

	assert.False(t, b.allow(), "failed trial should reopen the breaker immediately")
	require.Eventually(t, b.allow, time.Second, time.Millisecond, "a new trial should become available after the restarted cooldown")
}

func TestCircuitBreakerOnlyOneTrialAtATime(t *testing.T) {
	b := newCircuitBreaker(1, 10*time.Millisecond)
	b.recordFailure()
	require.Eventually(t, b.allow, time.Second, time.Millisecond, "trial should become available")

	// The trial slot was just consumed by the Eventually poll above (which
	// calls allow() repeatedly); any further concurrent call must still be
	// refused until that trial resolves.
	assert.False(t, b.allow(), "a second concurrent call must not also become a trial")
}

func TestCircuitBreakerDisabledWhenThresholdNonPositive(t *testing.T) {
	b := newCircuitBreaker(0, time.Hour)
	for i := 0; i < 100; i++ {
		b.recordFailure()
	}
	assert.True(t, b.allow(), "a non-positive failureThreshold must disable the breaker entirely")
}
