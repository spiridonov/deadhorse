package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spiridonov/deadhorse"
)

// ErrInvalidKey is wrapped into a RequestEntry's ResponseEntry.Err when its
// key can't be sent over DHP/1 at all (empty, or containing whitespace/'|').
// It's a caller bug, not an operational failure, so it's always reported as
// throttled regardless of the client's fail-open/closed setting.
var ErrInvalidKey = errors.New("deadhorse: invalid key")

// ErrEntryRejected is wrapped into a ResponseEntry.Err when the server sent
// back the bare ERR token for that entry -- it rejected the entry outright
// (a bad field on the wire), so there's no computed decision to trust.
var ErrEntryRejected = errors.New("deadhorse: entry rejected by server")

// shardConn is one persistent connection to one shard, reconnected lazily on
// the next call after any error. A single mutex serializes request/response
// pairs on it -- correct and simple; a client that needs more parallelism
// against one shard should hold several ShardedClients or shardConns, not
// something this type needs to grow pipelining to provide.
type shardConn struct {
	addr string

	mu     sync.Mutex
	conn   net.Conn
	reader *bufio.Reader
}

// throttle validates keys locally first, so one malformed key doesn't spoil
// the rest of the batch, sends the valid entries over the wire, and always
// returns a fully populated, same-length result slice. The returned error,
// if any, is a join of every distinct problem (validation and/or network)
// encountered -- for the network case specifically, failOpen decides what
// Throttled becomes for the entries that never got a real answer.
func (c *shardConn) throttle(ctx context.Context, entries []deadhorse.RequestEntry, timeout time.Duration, failOpen bool) ([]deadhorse.ResponseEntry, error) {
	results := make([]deadhorse.ResponseEntry, len(entries))
	validEntries := make([]deadhorse.RequestEntry, 0, len(entries))
	validIdx := make([]int, 0, len(entries))
	var errs []error

	for i, e := range entries {
		if err := validateKey(e.Key); err != nil {
			results[i] = deadhorse.ResponseEntry{Key: e.Key, Throttled: true, Err: err}
			errs = append(errs, err)
			continue
		}
		validEntries = append(validEntries, e)
		validIdx = append(validIdx, i)
	}
	if len(validEntries) == 0 {
		return results, errors.Join(errs...)
	}

	if err := c.exchange(ctx, validEntries, timeout, results, validIdx); err != nil {
		for _, idx := range validIdx {
			results[idx] = deadhorse.ResponseEntry{Key: entries[idx].Key, Throttled: !failOpen, Err: err}
		}
		errs = append(errs, err)
	}
	return results, errors.Join(errs...)
}

// exchange does the actual wire round trip for a batch of already-validated
// entries and fills results at the positions named by validIdx. It returns
// only a network/protocol-level error -- never a per-entry one, since a
// per-entry ERR is not an exchange failure (see decodeResult).
func (c *shardConn) exchange(ctx context.Context, validEntries []deadhorse.RequestEntry, timeout time.Duration, results []deadhorse.ResponseEntry, validIdx []int) error {
	req := encodeThrottle(validEntries)

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.ensureConnLocked(); err != nil {
		return err
	}

	// Captured once, locally: closeLocked (below, or from a later call once
	// we unlock) can reassign or nil out the c.conn *field* at any time, and
	// the watcher below runs unsynchronized with that. Operating on this
	// local copy of the net.Conn value instead avoids a data race on the
	// field.
	conn := c.conn

	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	conn.SetDeadline(deadline)

	// Honor ctx cancellation even before the deadline above by forcing an
	// immediate deadline if ctx is done first, which unblocks the blocking
	// Write/Read below. context.AfterFunc (rather than a hand-rolled
	// watcher goroutine selecting on ctx.Done() vs. a stop channel) matters
	// here beyond style: a caller that cancels ctx right after Throttle
	// returns -- the ordinary `ctx, cancel := context.WithTimeout(...);
	// defer cancel()` pattern -- makes ctx.Done() and "this call already
	// finished" become ready at nearly the same instant. A hand-rolled
	// select can resolve in favor of ctx.Done() even then, calling
	// SetDeadline on a connection that's already been handed back to the
	// pool and picked up by a *different*, unrelated call -- aborting it
	// with a spurious timeout. AfterFunc's stop() is specifically
	// synchronized against a concurrent firing to close exactly this race.
	stopWatch := context.AfterFunc(ctx, func() { conn.SetDeadline(time.Now()) })
	defer stopWatch()

	if _, err := conn.Write([]byte(req)); err != nil {
		c.closeLocked()
		return firstNonNil(ctx.Err(), err)
	}

	line, err := c.reader.ReadString('\n')
	if err != nil {
		c.closeLocked()
		return firstNonNil(ctx.Err(), err)
	}

	validResults, err := decodeResult(line, validEntries)
	if err != nil {
		c.closeLocked()
		return err
	}
	for j, idx := range validIdx {
		results[idx] = validResults[j]
	}
	return nil
}

func firstNonNil(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *shardConn) ensureConnLocked() error {
	if c.conn != nil {
		return nil
	}
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return err
	}
	c.conn = conn
	c.reader = bufio.NewReader(conn)
	return nil
}

func (c *shardConn) closeLocked() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.reader = nil
	}
}

func (c *shardConn) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeLocked()
}

func validateKey(key string) error {
	if key == "" || strings.ContainsAny(key, " \t\r\n|") {
		return fmt.Errorf("deadhorse: key %q is empty or contains a space/'|', which DHP/1 forbids: %w", key, ErrInvalidKey)
	}
	return nil
}

// encodeThrottle assumes every entry has already passed validateKey.
func encodeThrottle(entries []deadhorse.RequestEntry) string {
	parts := make([]string, len(entries))
	for i, e := range entries {
		mode := "R"
		if e.Peek {
			mode = "P"
		}
		parts[i] = fmt.Sprintf("%s|%d|%d|%d|%s", e.Key, e.Limit.Capacity, int64(e.Limit.EmissionInterval), deadhorse.EffectiveCost(e.Cost), mode)
	}
	if len(parts) == 0 {
		return "THROTTLE\n"
	}
	return "THROTTLE " + strings.Join(parts, " ") + "\n"
}

func decodeResult(line string, entries []deadhorse.RequestEntry) ([]deadhorse.ResponseEntry, error) {
	line = strings.TrimRight(line, "\r\n")
	tokens := strings.Fields(line)
	if len(tokens) == 0 || tokens[0] != "RESULT" {
		return nil, fmt.Errorf("deadhorse: unexpected response %q", line)
	}
	resultTokens := tokens[1:]
	if len(resultTokens) != len(entries) {
		return nil, fmt.Errorf("deadhorse: response entry count mismatch: got %d, want %d in %q", len(resultTokens), len(entries), line)
	}

	results := make([]deadhorse.ResponseEntry, len(entries))
	for i, tok := range resultTokens {
		if tok == "ERR" {
			results[i] = deadhorse.ResponseEntry{Key: entries[i].Key, Throttled: true, Err: ErrEntryRejected}
			continue
		}

		fields := strings.Split(tok, "|")
		if len(fields) != 4 {
			return nil, fmt.Errorf("deadhorse: malformed result %q", tok)
		}
		remaining, err1 := strconv.ParseInt(fields[2], 10, 64)
		retryAfter, err2 := strconv.ParseInt(fields[3], 10, 64)
		if err1 != nil || err2 != nil || remaining < 0 {
			return nil, fmt.Errorf("deadhorse: malformed result %q", tok)
		}
		results[i] = deadhorse.ResponseEntry{
			Key:        fields[0],
			Throttled:  fields[1] == "1",
			Remaining:  remaining,
			RetryAfter: time.Duration(retryAfter),
		}
	}
	return results, nil
}
