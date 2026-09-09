// Package client is a DHP/1 client for DeadHorse.
//
// A ShardedClient holds a static list of shard addresses and, for every key,
// picks exactly one shard by hashing the key -- no discovery, no
// rebalancing, no coordination with the servers at all. It implements
// deadhorse.Throttler, so it's a drop-in Throttler wherever one is expected.
package client

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/spiridonov/deadhorse"
)

const defaultTimeout = 10 * time.Millisecond

// ShardedClient routes each key to shard hash(key) % len(addrs), a static
// list configured at construction time. This is deliberately plain modulo
// hashing rather than consistent hashing: remapping a key when the shard
// count changes just resets that key's bucket on its new shard (one extra
// burst of unthrottled traffic, once), which is cheap enough here that the
// simpler scheme is the right default. Switch to a consistent-hashing
// variant instead if the shard count changes often enough that even brief
// under-enforcement during a resize is a problem.
type ShardedClient struct {
	shards   []*shardConn
	timeout  time.Duration
	failOpen bool
}

var _ deadhorse.Throttler = &ShardedClient{}

type Option func(*ShardedClient)

// WithTimeout overrides the per-call network timeout (default 10ms -- a
// starting point for a same-rack/same-DC deployment; tune from observed
// p99.9 in practice). A context deadline passed to Throttle, if earlier,
// still takes precedence.
func WithTimeout(d time.Duration) Option {
	return func(c *ShardedClient) { c.timeout = d }
}

// WithFailClosed makes a shard that's unreachable or times out count as
// throttled instead of the default fail-open (not throttled): a rate limiter
// should not be the reason the service it protects goes down. Reach for
// fail-closed only for limits that are also a security/quota control, not
// just protective. This never applies to entries rejected by local
// validation (e.g. a malformed key) -- those are always reported as
// throttled, since a caller bug is not something fail-open is meant to
// paper over.
func WithFailClosed() Option {
	return func(c *ShardedClient) { c.failOpen = false }
}

// NewShardedClient builds a client over a static list of shard addresses
// (host:port). Connections are opened lazily, on first use per shard.
func NewShardedClient(addrs []string, opts ...Option) *ShardedClient {
	c := &ShardedClient{
		shards:   make([]*shardConn, len(addrs)),
		timeout:  defaultTimeout,
		failOpen: true,
	}
	for i, addr := range addrs {
		c.shards[i] = &shardConn{addr: addr}
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Throttle groups entries by shard, checks each shard's group concurrently
// (one THROTTLE batch per shard), and reassembles the results in the
// caller's original order. The returned slice is always fully populated,
// even when the returned error is non-nil; the error is a join of every
// distinct problem encountered, for callers who just want a cheap "did
// anything go wrong" check without walking the results themselves.
func (c *ShardedClient) Throttle(ctx context.Context, entries []deadhorse.RequestEntry) ([]deadhorse.ResponseEntry, error) {
	byShard := make(map[int][]int, len(c.shards))
	for i, e := range entries {
		shard := c.shardFor(e.Key)
		byShard[shard] = append(byShard[shard], i)
	}

	results := make([]deadhorse.ResponseEntry, len(entries))
	errsCh := make(chan error, len(byShard))
	var wg sync.WaitGroup
	for shard, idxs := range byShard {
		wg.Add(1)
		go func(shard int, idxs []int) {
			defer wg.Done()

			subEntries := make([]deadhorse.RequestEntry, len(idxs))
			for j, idx := range idxs {
				subEntries[j] = entries[idx]
			}

			subResults, err := c.shards[shard].throttle(ctx, subEntries, c.timeout, c.failOpen)
			for j, idx := range idxs {
				results[idx] = subResults[j]
			}
			if err != nil {
				errsCh <- err
			}
		}(shard, idxs)
	}
	wg.Wait()
	close(errsCh)

	var errs []error
	for err := range errsCh {
		errs = append(errs, err)
	}
	return results, errors.Join(errs...)
}

func (c *ShardedClient) shardFor(key string) int {
	return int(fnv1a(key) % uint64(len(c.shards)))
}

// Close closes every shard's connection.
func (c *ShardedClient) Close() {
	for _, s := range c.shards {
		s.close()
	}
}

func fnv1a(s string) uint64 {
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211

	h := uint64(offset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}
