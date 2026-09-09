package deadhorsetest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/spiridonov/deadhorse"
)

func TestNoOpThrottlerNeverThrottles(t *testing.T) {
	th := &NoOpThrottler{}
	entries := []deadhorse.RequestEntry{
		{Key: "a", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: 1}},
		{Key: "b", Limit: deadhorse.Limit{Capacity: 0, EmissionInterval: 0}},
	}
	got, err := th.Throttle(context.Background(), entries)
	require.NoError(t, err)
	require.Len(t, got, len(entries))
	for i, r := range got {
		assert.Equal(t, entries[i].Key, r.Key)
		assert.False(t, r.Throttled)
		assert.NoError(t, r.Err)
	}
}

func TestAlwaysTrueThrottlerAlwaysThrottles(t *testing.T) {
	th := &AlwaysTrueThrottler{}
	entries := []deadhorse.RequestEntry{
		{Key: "a", Limit: deadhorse.Limit{Capacity: 1000, EmissionInterval: 1}},
	}
	got, err := th.Throttle(context.Background(), entries)
	require.NoError(t, err)
	assert.True(t, got[0].Throttled)
}
