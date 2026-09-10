package client

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/spiridonov/deadhorse"
	"github.com/spiridonov/deadhorse/deadhorsetest"
	"github.com/spiridonov/deadhorse/server"
)

// startServer spins up a real server.TextServer over the given Throttler on
// a freshly reserved port and returns its address, torn down at test
// cleanup. It reserves the port, closes the probe listener, and hands the
// bare address to ListenAndServe (which does its own net.Listen) rather
// than reaching into TextServer's internals -- there's an unavoidable,
// vanishingly small window where something else could grab the port in
// between, the standard tradeoff for testing a function that binds its own
// listener from a bare address string.
func startServer(t *testing.T, throttler deadhorse.Throttler) string {
	t.Helper()
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := probe.Addr().String()
	require.NoError(t, probe.Close())

	srv := server.NewTextServer(throttler, 0)
	go srv.ListenAndServe(addr)
	t.Cleanup(func() { srv.Close() })

	require.Eventually(t, func() bool {
		conn, dialErr := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if dialErr != nil {
			return false
		}
		conn.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond, "server should be listening promptly")

	return addr
}

func TestShardedClientSingleEntryRoundTrip(t *testing.T) {
	th := server.NewInMemoryThrottler(0, time.Hour)
	defer th.Close()
	addr := startServer(t, th)

	c := NewShardedClient([]string{addr})
	defer c.Close()

	entry := deadhorse.RequestEntry{Key: "user:1", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: time.Hour}}

	r1, err := c.Throttle(context.Background(), []deadhorse.RequestEntry{entry})
	require.NoError(t, err)
	require.False(t, r1[0].Throttled, "first request into an empty capacity-1 bucket should be admitted")
	assert.NoError(t, r1[0].Err)

	r2, err := c.Throttle(context.Background(), []deadhorse.RequestEntry{entry})
	require.NoError(t, err)
	assert.True(t, r2[0].Throttled, "second request should be throttled")
}

func TestShardedClientBatchAcrossKeys(t *testing.T) {
	th := server.NewInMemoryThrottler(0, time.Hour)
	defer th.Close()
	addr := startServer(t, th)

	c := NewShardedClient([]string{addr})
	defer c.Close()

	entries := []deadhorse.RequestEntry{
		{Key: "tenant-a", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: time.Hour}},
		{Key: "tenant-b", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: time.Hour}},
	}
	results, err := c.Throttle(context.Background(), entries)
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.False(t, results[0].Throttled)
	assert.False(t, results[1].Throttled)
}

func TestShardedClientPeekDoesNotConsume(t *testing.T) {
	th := server.NewInMemoryThrottler(0, time.Hour)
	defer th.Close()
	addr := startServer(t, th)

	c := NewShardedClient([]string{addr})
	defer c.Close()

	peek := deadhorse.RequestEntry{Key: "peek-key", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: time.Hour}, Peek: true}
	for i := 0; i < 3; i++ {
		r, err := c.Throttle(context.Background(), []deadhorse.RequestEntry{peek})
		require.NoError(t, err)
		require.False(t, r[0].Throttled, "peek %d should never consume the bucket", i)
	}

	real := peek
	real.Peek = false
	r, err := c.Throttle(context.Background(), []deadhorse.RequestEntry{real})
	require.NoError(t, err)
	assert.False(t, r[0].Throttled, "the first real request after only peeks should still be admitted")
}

func TestShardedClientInvalidKeyDoesNotSinkRestOfBatch(t *testing.T) {
	th := server.NewInMemoryThrottler(0, time.Hour)
	defer th.Close()
	addr := startServer(t, th)

	// Even with fail-closed configured, an invalid key must not affect the
	// other, valid entry sharing the same shard.
	c := NewShardedClient([]string{addr}, WithFailClosed())
	defer c.Close()

	entries := []deadhorse.RequestEntry{
		{Key: "bad key with spaces", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: time.Hour}},
		{Key: "good-key", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: time.Hour}},
	}
	results, err := c.Throttle(context.Background(), entries)
	require.Error(t, err, "the call-level error should surface the validation problem")
	require.Len(t, results, 2)

	assert.True(t, results[0].Throttled, "an invalid key is always reported as throttled")
	assert.ErrorIs(t, results[0].Err, ErrInvalidKey)

	assert.False(t, results[1].Throttled, "the valid entry in the same batch must still reach the server and succeed")
	assert.NoError(t, results[1].Err)
}

func TestShardedClientFailOpenOnUnreachableShard(t *testing.T) {
	// Nothing is listening on this address.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())

	c := NewShardedClient([]string{addr}, WithTimeout(50*time.Millisecond))
	defer c.Close()

	results, err := c.Throttle(context.Background(), []deadhorse.RequestEntry{
		{Key: "k", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: time.Hour}},
	})
	require.Error(t, err)
	require.Len(t, results, 1)
	assert.False(t, results[0].Throttled, "fail-open (the default) should report not-throttled on a network failure")
	assert.Error(t, results[0].Err)
}

func TestShardedClientFailClosedOnUnreachableShard(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())

	c := NewShardedClient([]string{addr}, WithTimeout(50*time.Millisecond), WithFailClosed())
	defer c.Close()

	results, err := c.Throttle(context.Background(), []deadhorse.RequestEntry{
		{Key: "k", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: time.Hour}},
	})
	require.Error(t, err)
	require.Len(t, results, 1)
	assert.True(t, results[0].Throttled, "fail-closed should report throttled on a network failure")
}

func TestShardedClientServerRejectionSurfacesErrEntryRejected(t *testing.T) {
	th := server.NewInMemoryThrottler(0, time.Hour)
	defer th.Close()
	addr := startServer(t, th)

	c := NewShardedClient([]string{addr})
	defer c.Close()

	// A negative capacity passes the client's own key validation (only the
	// key itself is checked locally) but fails the server's parseEntry,
	// which responds with the bare ERR token for this entry -- decodeResult
	// should turn that into Err wrapping ErrEntryRejected. This is a
	// well-formed, successfully decoded response (the server had an answer:
	// "no"), not a call-level failure, so the aggregate error stays nil --
	// only ResponseEntry.Err carries the detail.
	r, err := c.Throttle(context.Background(), []deadhorse.RequestEntry{
		{Key: "bad-limit", Limit: deadhorse.Limit{Capacity: -1, EmissionInterval: time.Hour}},
	})
	require.NoError(t, err)
	require.Len(t, r, 1)
	assert.True(t, r[0].Throttled, "a server-rejected entry is reported as throttled")
	assert.ErrorIs(t, r[0].Err, ErrEntryRejected)
}

func TestShardedClientContextCancellation(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	t.Cleanup(func() { lis.Close() })

	// Accept the connection but never respond, so the client blocks until
	// something -- the deadline or ctx -- unblocks it.
	go func() {
		conn, err := lis.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		<-t.Context().Done()
	}()

	c := NewShardedClient([]string{addr}, WithTimeout(10*time.Second))
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = c.Throttle(ctx, []deadhorse.RequestEntry{
		{Key: "k", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: time.Hour}},
	})
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Less(t, elapsed, 5*time.Second, "the context's short deadline should cut the call off well before the 10s client timeout")
}

// TestShardedClientConcurrentCallsAreCorrectlyMatched exercises shardConn's
// pipelining directly: many calls in flight at once on the same connection,
// each answered by the reader loop matching responses back to calls by
// plain FIFO order (see shardConn's doc comment). Each key gets a distinct
// capacity so its expected remaining count on the second hit is unique --
// if the reader loop ever matched a response to the wrong call, at least
// one key would come back with someone else's (differently-valued)
// remaining count instead of its own, deterministically catching the
// mismatch rather than relying on timing.
func TestShardedClientConcurrentCallsAreCorrectlyMatched(t *testing.T) {
	th := server.NewInMemoryThrottler(0, time.Hour)
	defer th.Close()
	addr := startServer(t, th)

	// A generous timeout: this test's point is to fire n calls at once onto
	// one connection, and 50-way contention on shardConn's submit mutex
	// (especially under -race) can legitimately take longer than the
	// client's normal small default timeout without anything being wrong.
	c := NewShardedClient([]string{addr}, WithTimeout(2*time.Second))
	defer c.Close()

	const n = 50
	entryFor := func(i int) deadhorse.RequestEntry {
		return deadhorse.RequestEntry{
			Key:   fmt.Sprintf("k-%d", i),
			Limit: deadhorse.Limit{Capacity: int64(100 + i), EmissionInterval: time.Hour},
		}
	}

	// Warm up: one hit per key, sequentially, so key i's bucket has exactly
	// one unit consumed before the concurrent round.
	for i := 0; i < n; i++ {
		_, err := c.Throttle(context.Background(), []deadhorse.RequestEntry{entryFor(i)})
		require.NoError(t, err)
	}

	// All n second hits fire at once, pipelined onto the same connection.
	// Remaining reports headroom *before* this hit is applied, so after one
	// prior hit each key's second hit should report capacity-1.
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := c.Throttle(context.Background(), []deadhorse.RequestEntry{entryFor(i)})
			assert.NoError(t, err)
			require.Len(t, r, 1)
			assert.Equalf(t, int64(100+i-1), r[0].Remaining, "key k-%d got a response meant for a different call", i)
		}(i)
	}
	wg.Wait()
}

func TestShardedClientNoOpThrottlerViaTextServer(t *testing.T) {
	addr := startServer(t, &deadhorsetest.NoOpThrottler{})

	c := NewShardedClient([]string{addr})
	defer c.Close()

	r, err := c.Throttle(context.Background(), []deadhorse.RequestEntry{
		{Key: "k", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: time.Hour}},
	})
	require.NoError(t, err)
	assert.False(t, r[0].Throttled)
}
