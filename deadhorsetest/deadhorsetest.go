// Package deadhorsetest provides server.Throttler test doubles, in the
// spirit of the standard library's httptest/iotest: they're for tests that
// need a Throttler to plug into a TextServer but don't care about its real
// behavior, not part of the production API surface.
//
// This package deliberately does not import server (which would create an
// import cycle: server's own tests import this package) -- so conformance
// to server.Throttler is structural only, not asserted with a `var _`
// here. server's own tests are what actually exercise these against a real
// TextServer.
package deadhorsetest

import (
	"context"

	"github.com/spiridonov/deadhorse"
)

// NoOpThrottler never throttles anything.
type NoOpThrottler struct{}

func (t *NoOpThrottler) Throttle(ctx context.Context, entries []deadhorse.RequestEntry) ([]deadhorse.ResponseEntry, error) {
	result := make([]deadhorse.ResponseEntry, len(entries))
	for i, e := range entries {
		result[i].Key = e.Key
	}
	return result, nil
}

// AlwaysTrueThrottler always reports every entry as throttled.
type AlwaysTrueThrottler struct{}

func (t *AlwaysTrueThrottler) Throttle(ctx context.Context, entries []deadhorse.RequestEntry) ([]deadhorse.ResponseEntry, error) {
	result := make([]deadhorse.ResponseEntry, len(entries))
	for i, e := range entries {
		result[i].Key = e.Key
		result[i].Throttled = true
	}
	return result, nil
}
