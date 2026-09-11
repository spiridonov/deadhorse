package server

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStoreAppliesDefaults(t *testing.T) {
	st := newStore(0, 0)
	defer st.close()

	assert.Len(t, st.stripes, defaultStripes, "numStripes<=0 should fall back to defaultStripes")
}

func TestStoreCloseIsIdempotent(t *testing.T) {
	st := newStore(0, time.Hour)
	assert.NotPanics(t, func() {
		st.close()
		st.close()
		st.close()
	}, "close must tolerate being called more than once, e.g. via a deferred Close alongside an explicit early one")
}

func TestStripeForIsDeterministic(t *testing.T) {
	st := newStore(16, time.Hour)
	defer st.close()

	for _, key := range []string{"a", "org:123:writes", ""} {
		first := st.stripeFor(key)
		for i := 0; i < 10; i++ {
			assert.Same(t, first, st.stripeFor(key), "stripeFor(%q) is not deterministic across calls", key)
		}
	}
}

func TestGetOrCreateReturnsSameBucketForSameKey(t *testing.T) {
	st := newStore(4, time.Hour)
	defer st.close()

	s := st.stripeFor("key")
	b1 := s.getOrCreate("key")
	b2 := s.getOrCreate("key")
	assert.Same(t, b1, b2, "getOrCreate returned different buckets for the same key")

	other := s.getOrCreate("other-key")
	assert.NotSame(t, b1, other, "getOrCreate returned the same bucket for two different keys")
}

func TestRotatePromotesFromColdBeforeDropping(t *testing.T) {
	s := newStripe()

	b := s.getOrCreate("key")
	b.tat = 42

	s.rotate() // key moves hot -> cold
	_, stillHot := s.hot["key"]
	assert.False(t, stillHot, "key should have moved out of hot after one rotation")
	_, inCold := s.cold["key"]
	assert.True(t, inCold, "key should be in cold after one rotation")

	// A touch while the key is only in cold must promote it back to hot,
	// preserving its state, not hand back a fresh bucket.
	got := s.getOrCreate("key")
	require.Same(t, b, got, "getOrCreate handed back a different bucket after cold promotion")
	assert.EqualValues(t, 42, got.tat, "promoted bucket lost its state")
	_, inHot := s.hot["key"]
	assert.True(t, inHot, "key should have been promoted into hot")
	_, stillInCold := s.cold["key"]
	assert.False(t, stillInCold, "key should have been removed from cold once promoted")

	// A second rotation with no intervening touch drops it for good.
	s.rotate()
	s.rotate()
	got = s.getOrCreate("key")
	assert.NotSame(t, b, got, "key should have been dropped after two untouched rotations")
	assert.Zero(t, got.tat, "a freshly dropped-and-recreated key should start at tat=0")
}

func TestStoreGCDropsIdleKeys(t *testing.T) {
	const gcInterval = 20 * time.Millisecond
	st := newStore(4, gcInterval)
	defer st.close()

	key := "idle-key"
	b := st.stripeFor(key).getOrCreate(key)
	b.mu.Lock()
	b.tat = 999999
	b.mu.Unlock()

	// Comfortably more than the 1-2 gc_interval window a key can survive
	// without being touched, to avoid racing the ticker.
	time.Sleep(8 * gcInterval)

	got := st.stripeFor(key).getOrCreate(key)
	require.NotSame(t, b, got, "key untouched for 8 GC intervals should have been dropped")
	assert.Zero(t, got.tat, "recreated key should start fresh")
}

func TestStoreGCSparesRepeatedlyTouchedKeys(t *testing.T) {
	const gcInterval = 20 * time.Millisecond
	st := newStore(4, gcInterval)
	defer st.close()

	key := "hot-key"
	b := st.stripeFor(key).getOrCreate(key)

	deadline := time.Now().Add(8 * gcInterval)
	for time.Now().Before(deadline) {
		require.Same(t, b, st.stripeFor(key).getOrCreate(key), "a key touched well within every GC interval must never be dropped")
		time.Sleep(gcInterval / 4)
	}
}

func TestStoreKeyCountEstimate(t *testing.T) {
	st := newStore(4, time.Hour)
	defer st.close()

	assert.Zero(t, st.keyCountEstimate(), "empty store should estimate 0 keys")

	for _, key := range []string{"a", "b", "c"} {
		st.stripeFor(key).getOrCreate(key)
	}
	assert.Equal(t, 3, st.keyCountEstimate())
}

func TestBucketStateConcurrentAccessIsSafe(t *testing.T) {
	st := newStore(4, time.Hour)
	defer st.close()

	const goroutines = 50
	const perGoroutine = 200
	key := "concurrent-key"

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				b := st.stripeFor(key).getOrCreate(key)
				b.mu.Lock()
				b.tat++
				b.mu.Unlock()
			}
		}()
	}
	wg.Wait()

	b := st.stripeFor(key).getOrCreate(key)
	b.mu.Lock()
	got := b.tat
	b.mu.Unlock()

	assert.EqualValues(t, goroutines*perGoroutine, got, "a lost update means the stripe/bucket locking is broken")
}
