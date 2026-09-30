package client

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/spiridonov/deadhorse"
	"github.com/spiridonov/deadhorse/server"
)

func TestClientSingleEntryRoundTrip(t *testing.T) {
	th := server.NewInMemoryThrottler(0, time.Hour)
	defer th.Close()
	addr := startServer(t, th)

	c := NewClient(addr)
	defer c.Close()

	entry := deadhorse.RequestEntry{Key: "user:1", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}}

	r1, err := c.Throttle(context.Background(), []deadhorse.RequestEntry{entry})
	require.NoError(t, err)
	require.False(t, r1[0].Throttled, "first request into an empty capacity-1 bucket should be admitted")

	r2, err := c.Throttle(context.Background(), []deadhorse.RequestEntry{entry})
	require.NoError(t, err)
	assert.True(t, r2[0].Throttled, "second request should be throttled")
}

func TestClientBatchIsOneAllOrNoneLine(t *testing.T) {
	// Same guarantee as ShardedClient (see
	// TestShardedClientShardKeyGroupsEntriesOntoOneLine): with a single
	// server there's no shardKey to think about, so every entry passed to
	// one Throttle call is already grouped onto the one line/transaction.
	th := server.NewInMemoryThrottler(0, time.Hour)
	defer th.Close()
	addr := startServer(t, th)

	c := NewClient(addr)
	defer c.Close()

	org := deadhorse.RequestEntry{Key: "org:acme:writes", Limit: deadhorse.Limit{Capacity: 100, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}}
	user := deadhorse.RequestEntry{Key: "user:42:writes", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}}

	r, err := c.Throttle(context.Background(), []deadhorse.RequestEntry{user})
	require.NoError(t, err)
	require.False(t, r[0].Throttled, "warm-up call should exhaust user's one unit of capacity")

	results, err := c.Throttle(context.Background(), []deadhorse.RequestEntry{org, user})
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.True(t, results[0].Throttled, "org must be denied too: it shares a call/line with the failing user check")
	assert.True(t, results[1].Throttled, "user's own bucket is exhausted")
}

func TestClientPassesThroughOptions(t *testing.T) {
	// Nothing is listening on this address.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())

	c := NewClient(addr, WithTimeout(50*time.Millisecond), WithFailClosed())
	defer c.Close()

	results, err := c.Throttle(context.Background(), []deadhorse.RequestEntry{
		{Key: "k", Limit: deadhorse.Limit{Capacity: 1, Rate: deadhorse.Rate{Units: 1, Period: time.Hour}}},
	})
	require.Error(t, err)
	require.Len(t, results, 1)
	assert.True(t, results[0].Throttled, "WithFailClosed should carry through NewClient just like NewShardedClient")
}
