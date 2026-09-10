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

// maxInFlight bounds how many THROTTLE calls a single shardConn will pipeline
// onto its connection at once. A call beyond this simply waits its turn to
// submit -- backpressure against a shard that's accepting writes faster than
// it's answering them, rather than an unbounded, memory-growing queue.
const maxInFlight = 256

// idleReadTimeout bounds how long shardConn's reader will wait for a response
// with nothing at all coming back, so a shard that accepted a connection but
// then went silent forever eventually gets noticed and reconnected instead of
// wedging the pipeline for good. It's deliberately generous and unrelated to
// any per-call timeout: normal gaps between bursts of traffic are expected
// and shouldn't cost a reconnect.
const idleReadTimeout = 30 * time.Second

// shardConn is one persistent, pipelined connection to one shard, reconnected
// lazily on the next call after any error. Multiple Throttle calls may be in
// flight on it concurrently: each submits its request and waits on its own
// channel rather than holding the connection for the whole round trip, so one
// slow call never blocks another's request from going out. This relies on
// the server answering one connection's requests strictly in the order they
// arrived (see TextServer.handleConn), which is what lets responses be
// matched back to calls by plain FIFO order instead of a request ID.
type shardConn struct {
	addr string

	// mu guards conn and pending, and serializes submit's write+enqueue pair
	// (see submit) so that wire order and queue order never diverge. It is
	// deliberately not held across any blocking network read -- only reads
	// and writes local to establishing/tearing down a connection.
	mu      sync.Mutex
	conn    net.Conn
	pending chan *pendingCall
}

// pendingCall is one THROTTLE request waiting for its response. resultCh is
// buffered so the reader loop's delivery never blocks on a caller that has
// already given up (its ctx expired) and stopped listening.
type pendingCall struct {
	entries  []deadhorse.RequestEntry
	resultCh chan pendingResult
}

type pendingResult struct {
	results []deadhorse.ResponseEntry
	err     error
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

// exchange submits a batch of already-validated entries and waits for its
// own response, without ever holding the connection for the duration of the
// wait -- see submit and shardConn's pipelining doc comment. It returns only
// a network/protocol-level error -- never a per-entry one, since a per-entry
// ERR is not an exchange failure (see decodeResult).
func (c *shardConn) exchange(ctx context.Context, validEntries []deadhorse.RequestEntry, timeout time.Duration, results []deadhorse.ResponseEntry, validIdx []int) error {
	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	waitCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	call := &pendingCall{entries: validEntries, resultCh: make(chan pendingResult, 1)}
	req := encodeThrottle(validEntries)

	if err := c.submit(waitCtx, call, req); err != nil {
		return err
	}

	select {
	case res := <-call.resultCh:
		if res.err != nil {
			return res.err
		}
		for j, idx := range validIdx {
			results[idx] = res.results[j]
		}
		return nil
	case <-waitCtx.Done():
		// call's response, if the shard eventually sends one, is still read
		// by the connection's reader loop and simply dropped into resultCh's
		// one-slot buffer unread -- the connection itself is left alone for
		// every other call still pipelined on it.
		return waitCtx.Err()
	}
}

// submit enqueues call and writes req to the wire as a single unit under mu,
// so the order calls are queued in always matches the order their requests
// hit the wire -- required for the reader loop's FIFO response matching to
// stay correct. ctx bounds how long submit will wait for room in a full
// pending queue; it does not bound the write itself, which gets its own
// deadline from ctx below.
func (c *shardConn) submit(ctx context.Context, call *pendingCall, req string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureConnLocked(); err != nil {
		return err
	}

	select {
	case c.pending <- call:
	case <-ctx.Done():
		return ctx.Err()
	}

	if d, ok := ctx.Deadline(); ok {
		c.conn.SetWriteDeadline(d)
	}
	if _, err := c.conn.Write([]byte(req)); err != nil {
		c.failLocked(err)
		return err
	}
	return nil
}

// ensureConnLocked dials a fresh connection and starts its reader loop if
// none is currently established. Caller must hold mu.
func (c *shardConn) ensureConnLocked() error {
	if c.conn != nil {
		return nil
	}
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return err
	}
	pending := make(chan *pendingCall, maxInFlight)
	c.conn = conn
	c.pending = pending
	go c.readLoop(conn, pending)
	return nil
}

// readLoop owns conn's read side for its entire lifetime: it reads one
// response line at a time and delivers each to the oldest still-pending
// call, relying on the server never answering a connection's requests out of
// order. Any read or decode error desyncs that ordering for good, so it
// takes the whole connection down with it via abort rather than trying to
// recover.
func (c *shardConn) readLoop(conn net.Conn, pending chan *pendingCall) {
	r := bufio.NewReader(conn)
	for {
		conn.SetReadDeadline(time.Now().Add(idleReadTimeout))
		line, err := r.ReadString('\n')
		if err != nil {
			c.abort(conn, pending, err)
			return
		}

		var call *pendingCall
		select {
		case call = <-pending:
		default:
			c.abort(conn, pending, fmt.Errorf("deadhorse: response with nothing pending: %q", strings.TrimRight(line, "\r\n")))
			return
		}

		results, err := decodeResult(line, call.entries)
		call.resultCh <- pendingResult{results: results, err: err}
		if err != nil {
			c.abort(conn, pending, err)
			return
		}
	}
}

// abort tears down conn and fails every call still waiting in pending with
// err. It's called from the reader loop itself (outside mu, since it must
// never block a concurrent submit on a network read), and only clears
// shardConn's own conn/pending fields if they still refer to this exact
// generation -- a concurrent submit may already have reconnected.
func (c *shardConn) abort(conn net.Conn, pending chan *pendingCall, err error) {
	c.mu.Lock()
	if c.conn == conn {
		c.conn = nil
		c.pending = nil
	}
	c.mu.Unlock()

	conn.Close()
	drainPending(pending, err)
}

// failLocked is abort's counterpart for a write failure inside submit, where
// mu is already held. It's safe to close and drain right here, without
// releasing mu first: nothing else can observe or touch this generation's
// conn/pending until submit returns.
func (c *shardConn) failLocked(err error) {
	conn := c.conn
	pending := c.pending
	c.conn = nil
	c.pending = nil
	if conn != nil {
		conn.Close()
	}
	drainPending(pending, err)
}

func drainPending(pending chan *pendingCall, err error) {
	for {
		select {
		case call := <-pending:
			call.resultCh <- pendingResult{err: err}
		default:
			return
		}
	}
}

func (c *shardConn) close() {
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.pending = nil
	c.mu.Unlock()

	if conn != nil {
		conn.Close()
	}
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
