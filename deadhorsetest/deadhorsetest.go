// Package deadhorsetest provides Throttler test doubles, in the spirit of
// the standard library's httptest/iotest: they're for tests that need a
// deadhorse.Throttler but don't care about its real behavior, not part of
// the production API surface.
package deadhorsetest

import (
	"context"

	"github.com/spiridonov/deadhorse"
)

// NoOpThrottler never throttles anything.
type NoOpThrottler struct{}

var _ deadhorse.Throttler = &NoOpThrottler{}

func (t *NoOpThrottler) Throttle(ctx context.Context, entries []deadhorse.RequestEntry) ([]deadhorse.ResponseEntry, error) {
	result := make([]deadhorse.ResponseEntry, len(entries))
	for i, e := range entries {
		result[i].Key = e.Key
	}
	return result, nil
}

// AlwaysTrueThrottler always reports every entry as throttled.
type AlwaysTrueThrottler struct{}

var _ deadhorse.Throttler = &AlwaysTrueThrottler{}

func (t *AlwaysTrueThrottler) Throttle(ctx context.Context, entries []deadhorse.RequestEntry) ([]deadhorse.ResponseEntry, error) {
	result := make([]deadhorse.ResponseEntry, len(entries))
	for i, e := range entries {
		result[i].Key = e.Key
		result[i].Throttled = true
	}
	return result, nil
}
