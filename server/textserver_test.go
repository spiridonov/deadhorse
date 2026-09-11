package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/spiridonov/deadhorse"
	"github.com/spiridonov/deadhorse/deadhorsetest"
)

// -----------------------------------------------------------------------
// Unit-level tests: exercise the parsing/formatting/dispatch helpers
// directly, with no network involved.
// -----------------------------------------------------------------------

func TestParseEntry(t *testing.T) {
	cases := []struct {
		name string
		tok  string
		want deadhorse.RequestEntry
		ok   bool
	}{
		{
			name: "valid real mode",
			tok:  "key|10|1000|1|R",
			want: deadhorse.RequestEntry{Key: "key", Limit: deadhorse.Limit{Capacity: 10, EmissionInterval: 1000}, Cost: 1, Peek: false},
			ok:   true,
		},
		{
			name: "valid peek mode",
			tok:  "key|10|1000|1|P",
			want: deadhorse.RequestEntry{Key: "key", Limit: deadhorse.Limit{Capacity: 10, EmissionInterval: 1000}, Cost: 1, Peek: true},
			ok:   true,
		},
		{name: "too few fields", tok: "key|10|1000", ok: false},
		{name: "too many fields", tok: "key|10|1000|1|R|extra", ok: false},
		{name: "empty key", tok: "|10|1000|1|R", ok: false},
		{name: "non-numeric capacity", tok: "key|x|1000|1|R", ok: false},
		{name: "negative capacity", tok: "key|-1|1000|1|R", ok: false},
		{name: "non-numeric emission interval", tok: "key|10|x|1|R", ok: false},
		{name: "negative cost", tok: "key|10|1000|-1|R", ok: false},
		{name: "invalid mode", tok: "key|10|1000|1|X", ok: false},
		{name: "lowercase mode is invalid", tok: "key|10|1000|1|r", ok: false},
		{name: "key containing whitespace never reaches here as one token", tok: "", ok: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := parseEntry(c.tok)
			require.Equal(t, c.ok, ok)
			if c.ok {
				assert.Equal(t, c.want, got)
			}
		})
	}
}

func TestFormatResult(t *testing.T) {
	assert.Equal(t, "key|0|5|0", formatResult(deadhorse.ResponseEntry{Key: "key", Throttled: false, Remaining: 5, RetryAfter: 0}))
	assert.Equal(t, "key|1|0|1000", formatResult(deadhorse.ResponseEntry{Key: "key", Throttled: true, Remaining: 0, RetryAfter: 1000}))
	assert.Equal(t, "ERR", formatResult(deadhorse.ResponseEntry{Key: "key", Throttled: true, Err: errors.New("boom")}),
		"an errored entry collapses to the bare ERR token regardless of what Throttled says")
}

func TestReadLineSimple(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("PING\nPONG\r\n"))

	line, err := readLine(r, defaultMaxLineSize)
	require.NoError(t, err)
	assert.Equal(t, "PING", line)

	line, err = readLine(r, defaultMaxLineSize)
	require.NoError(t, err)
	assert.Equal(t, "PONG", line, "a trailing \\r before \\n must be trimmed")
}

func TestReadLineTooLong(t *testing.T) {
	r := bufio.NewReader(strings.NewReader(strings.Repeat("x", 100) + "\n"))
	_, err := readLine(r, 10)
	assert.ErrorIs(t, err, errLineTooLong)
}

func TestReadLineSpansMultipleInternalBufferFills(t *testing.T) {
	// A tiny bufio.Reader buffer forces ReadSlice to return bufio.ErrBufferFull
	// partway through the line; readLine must keep accumulating fragments
	// rather than stopping early.
	long := strings.Repeat("a", 500)
	r := bufio.NewReaderSize(strings.NewReader(long+"\n"), 16)

	line, err := readLine(r, 10_000)
	require.NoError(t, err)
	assert.Equal(t, long, line)
}

func TestReadLineNoTrailingNewlineReturnsError(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("no newline here"))
	_, err := readLine(r, defaultMaxLineSize)
	assert.Error(t, err)
	assert.NotErrorIs(t, err, errLineTooLong)
}

func TestDispatchPing(t *testing.T) {
	srv := NewTextServer(&deadhorsetest.NoOpThrottler{}, 0)
	resp, closeConn := srv.dispatch("PING")
	assert.Equal(t, "PONG", resp)
	assert.False(t, closeConn)
}

func TestDispatchQuit(t *testing.T) {
	srv := NewTextServer(&deadhorsetest.NoOpThrottler{}, 0)
	resp, closeConn := srv.dispatch("QUIT")
	assert.Empty(t, resp, "QUIT must not produce a response line")
	assert.True(t, closeConn)
}

func TestDispatchUnknownCommand(t *testing.T) {
	srv := NewTextServer(&deadhorsetest.NoOpThrottler{}, 0)
	resp, closeConn := srv.dispatch("FROBNICATE")
	assert.Equal(t, "ERROR unknown command", resp)
	assert.False(t, closeConn)
}

func TestDispatchHello(t *testing.T) {
	srv := NewTextServer(&deadhorsetest.NoOpThrottler{}, 0)

	resp, _ := srv.dispatch("HELLO 1")
	assert.Equal(t, "OK 1", resp)

	resp, _ = srv.dispatch("HELLO 2")
	assert.Equal(t, "ERROR unsupported version", resp)

	resp, _ = srv.dispatch("HELLO")
	assert.Equal(t, "ERROR missing version", resp)
}

func TestDispatchStats(t *testing.T) {
	// NoOpThrottler doesn't implement the optional keyCountEstimate hook, so
	// STATS should report 0 keys without panicking.
	srv := NewTextServer(&deadhorsetest.NoOpThrottler{}, 0)
	resp, _ := srv.dispatch("STATS")
	fields := strings.Fields(resp)
	require.Len(t, fields, 3)
	assert.Equal(t, "STATS", fields[0])
	assert.Equal(t, "0", fields[2], "key count should be 0 for a throttler without keyCountEstimate")

	// InMemoryThrottler does implement it, and STATS should reflect real usage.
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()
	th.Throttle(context.Background(), []deadhorse.RequestEntry{{Key: "a", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: 1}}})
	th.Throttle(context.Background(), []deadhorse.RequestEntry{{Key: "b", Limit: deadhorse.Limit{Capacity: 1, EmissionInterval: 1}}})

	srv = NewTextServer(th, 0)
	resp, _ = srv.dispatch("STATS")
	fields = strings.Fields(resp)
	require.Len(t, fields, 3)
	assert.Equal(t, "2", fields[2])
}

func TestHandleThrottleZeroEntries(t *testing.T) {
	srv := NewTextServer(&deadhorsetest.NoOpThrottler{}, 0)
	assert.Equal(t, "RESULT", srv.handleThrottle(""))
}

func TestHandleThrottleAllEntriesMalformed(t *testing.T) {
	srv := NewTextServer(&deadhorsetest.NoOpThrottler{}, 0)
	assert.Equal(t, "RESULT ERR", srv.handleThrottle("not-a-valid-entry"))
}

func TestHandleThrottleMixedValidAndMalformedPreservesOrder(t *testing.T) {
	srv := NewTextServer(&deadhorsetest.NoOpThrottler{}, 0)
	got := srv.handleThrottle("badtoken key|1|1000|1|R")
	assert.Equal(t, "RESULT ERR key|0|0|0", got)
}

// erroringThrottler always fails the whole call, as a buggy or overloaded
// custom Throttler might.
type erroringThrottler struct{}

func (erroringThrottler) Throttle(context.Context, []deadhorse.RequestEntry) ([]deadhorse.ResponseEntry, error) {
	return nil, errors.New("boom")
}

func TestHandleThrottleThrottlerErrorDoesNotSinkWholeBatch(t *testing.T) {
	srv := NewTextServer(erroringThrottler{}, 0)
	got := srv.handleThrottle("key-a|1|1000|1|R key-b|1|1000|1|R")
	assert.Equal(t, "RESULT ERR ERR", got, "a throttler-level error must report ERR per affected entry, not abort the whole line")

	// The connection-level behavior matters too: an aborted line used to
	// return "ERROR ...", which handleConn would send verbatim but keep the
	// connection open for -- but a caller reading responses positionally
	// could still be desynced by a line that isn't RESULT-shaped. Confirm
	// the line is always RESULT-shaped when there's at least one entry.
	assert.True(t, strings.HasPrefix(got, "RESULT "))
}

// shortResponseThrottler returns fewer responses than it was asked to
// evaluate, without an error -- a buggy custom Throttler violating its own
// documented contract (see the Throttler interface in types.go).
type shortResponseThrottler struct{}

func (shortResponseThrottler) Throttle(_ context.Context, entries []deadhorse.RequestEntry) ([]deadhorse.ResponseEntry, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	return []deadhorse.ResponseEntry{{Key: entries[0].Key}}, nil
}

func TestHandleThrottleMismatchedResponseLengthDoesNotSinkWholeBatch(t *testing.T) {
	srv := NewTextServer(shortResponseThrottler{}, 0)
	got := srv.handleThrottle("key-a|1|1000|1|R key-b|1|1000|1|R")
	assert.Equal(t, "RESULT ERR ERR", got, "a Throttler returning the wrong-length slice must be treated as a failure, not panic or leave entries blank")
}

// -----------------------------------------------------------------------
// Integration-level tests: a real TextServer over a real TCP connection.
// -----------------------------------------------------------------------

// startTestServer binds a listener, wires it into a TextServer exactly as
// ListenAndServe would, and returns the address to connect to. Doing the
// bind here (rather than letting ListenAndServe pick a port and racing to
// discover it) keeps the test deterministic; ListenAndServe/Close get their
// own dedicated test below.
func startTestServer(t *testing.T, throttler deadhorse.Throttler) string {
	t.Helper()
	srv := NewTextServer(throttler, 0)
	return startTextServerListener(t, srv)
}

func startTextServerListener(t *testing.T, srv *TextServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv.listener = lis

	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			go srv.handleConn(conn)
		}
	}()
	t.Cleanup(func() { lis.Close() })
	return lis.Addr().String()
}

type testClient struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func dialTestServer(t *testing.T, addr string) *testClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	t.Cleanup(func() { conn.Close() })
	return &testClient{t: t, conn: conn, r: bufio.NewReader(conn)}
}

func (c *testClient) send(line string) {
	c.t.Helper()
	_, err := c.conn.Write([]byte(line + "\n"))
	require.NoError(c.t, err)
}

func (c *testClient) recv() string {
	c.t.Helper()
	line, err := c.r.ReadString('\n')
	require.NoError(c.t, err)
	return strings.TrimRight(line, "\r\n")
}

func (c *testClient) sendRecv(line string) string {
	c.t.Helper()
	c.send(line)
	return c.recv()
}

func TestTextServerListenAndServeAndClose(t *testing.T) {
	// Reserve a free port, then hand the bare address to ListenAndServe --
	// which does its own net.Listen -- rather than reading srv's unexported
	// listener field from this goroutine while ListenAndServe's goroutine
	// writes it, which would be a genuine data race. There's an
	// unavoidable, vanishingly small window where something else could grab
	// the port in between; that's the standard tradeoff for testing a
	// function that binds its own listener from a bare address string.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := probe.Addr().String()
	require.NoError(t, probe.Close())

	srv := NewTextServer(&deadhorsetest.NoOpThrottler{}, 0)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe(addr) }()

	// Detect that the bind succeeded by actually connecting -- a real dial
	// only succeeds once the listener is up, so this is race-free (unlike
	// polling an internal field written by the other goroutine).
	var conn net.Conn
	require.Eventually(t, func() bool {
		c, dialErr := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if dialErr != nil {
			return false
		}
		conn = c
		return true
	}, 2*time.Second, 10*time.Millisecond, "ListenAndServe should bind and accept promptly")
	t.Cleanup(func() { conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))

	client := &testClient{t: t, conn: conn, r: bufio.NewReader(conn)}
	assert.Equal(t, "PONG", client.sendRecv("PING"))

	require.NoError(t, srv.Close())

	select {
	case err := <-errCh:
		assert.Error(t, err, "ListenAndServe should return once its listener is closed")
	case <-time.After(2 * time.Second):
		t.Fatal("ListenAndServe did not return after Close")
	}
}

func TestTextServerCloseWithoutListenIsNoop(t *testing.T) {
	srv := NewTextServer(&deadhorsetest.NoOpThrottler{}, 0)
	assert.NoError(t, srv.Close())
}

func TestTextServerCloseBeforeListenAndServePreventsAcceptLoop(t *testing.T) {
	// Regression test for the Close()/ListenAndServe() ordering race: if
	// Close() runs (even just barely) before ListenAndServe finishes
	// net.Listen and installs its listener, ListenAndServe must not go on
	// to bind and accept forever with nothing left able to stop it.
	srv := NewTextServer(&deadhorsetest.NoOpThrottler{}, 0)
	require.NoError(t, srv.Close())

	err := srv.ListenAndServe("127.0.0.1:0")
	assert.ErrorIs(t, err, net.ErrClosed, "ListenAndServe must refuse to serve once Close has already been requested")
}

func TestTextServerPingPong(t *testing.T) {
	addr := startTestServer(t, &deadhorsetest.NoOpThrottler{})
	client := dialTestServer(t, addr)
	assert.Equal(t, "PONG", client.sendRecv("PING"))
	// The connection stays open across multiple commands.
	assert.Equal(t, "PONG", client.sendRecv("PING"))
}

func TestTextServerHelloOverTheWire(t *testing.T) {
	addr := startTestServer(t, &deadhorsetest.NoOpThrottler{})
	client := dialTestServer(t, addr)
	assert.Equal(t, "OK 1", client.sendRecv("HELLO 1"))
}

func TestTextServerUnknownCommandKeepsConnectionOpen(t *testing.T) {
	addr := startTestServer(t, &deadhorsetest.NoOpThrottler{})
	client := dialTestServer(t, addr)
	assert.Equal(t, "ERROR unknown command", client.sendRecv("BOGUS"))
	assert.Equal(t, "PONG", client.sendRecv("PING"), "connection should still be usable after an ERROR")
}

func TestTextServerQuitClosesConnection(t *testing.T) {
	addr := startTestServer(t, &deadhorsetest.NoOpThrottler{})
	client := dialTestServer(t, addr)

	client.send("QUIT")
	_, err := client.r.ReadString('\n')
	assert.Error(t, err, "the server must close the connection without sending a response to QUIT")
}

func TestTextServerThrottleSingleKey(t *testing.T) {
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()
	addr := startTestServer(t, th)
	client := dialTestServer(t, addr)

	got := client.sendRecv("THROTTLE org:acme:writes|1|3600000000000|1|R")
	assert.Equal(t, "RESULT org:acme:writes|0|1|0", got, "first request into an empty capacity-1 bucket")

	got = client.sendRecv("THROTTLE org:acme:writes|1|3600000000000|1|R")
	require.Contains(t, got, "org:acme:writes|1|0|", "second request should be throttled")
}

func TestTextServerThrottleBatchMultipleKeys(t *testing.T) {
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()
	addr := startTestServer(t, th)
	client := dialTestServer(t, addr)

	got := client.sendRecv("THROTTLE tenant-a|1|1000|1|R tenant-b|1|1000|1|R")
	assert.Equal(t, "RESULT tenant-a|0|1|0 tenant-b|0|1|0", got)
}

func TestTextServerThrottleMalformedEntryMidBatch(t *testing.T) {
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()
	addr := startTestServer(t, th)
	client := dialTestServer(t, addr)

	got := client.sendRecv("THROTTLE not-a-valid-entry good-key|1|1000|1|R")
	assert.Equal(t, "RESULT ERR good-key|0|1|0", got, "a malformed entry must not sink the rest of the batch")

	// The connection must still be usable afterwards.
	assert.Equal(t, "PONG", client.sendRecv("PING"))
}

func TestTextServerThrottleEmptyBatch(t *testing.T) {
	addr := startTestServer(t, &deadhorsetest.NoOpThrottler{})
	client := dialTestServer(t, addr)

	assert.Equal(t, "RESULT", client.sendRecv("THROTTLE"))
	assert.Equal(t, "PONG", client.sendRecv("PING"))
}

func TestTextServerPipelining(t *testing.T) {
	addr := startTestServer(t, &deadhorsetest.NoOpThrottler{})
	client := dialTestServer(t, addr)

	// Write three requests before reading any response, then read all three
	// back in order -- this is the whole point of the protocol being
	// pipeline-friendly.
	client.send("PING")
	client.send("PING")
	client.send("PING")
	for i := 0; i < 3; i++ {
		assert.Equal(t, "PONG", client.recv())
	}
}

// readDeadlineRecorder wraps a net.Conn and captures every deadline passed
// to SetReadDeadline, so a test can observe handleConn's idle-timeout
// behavior without actually waiting out a real 30-second timeout.
type readDeadlineRecorder struct {
	net.Conn
	deadlines chan time.Time
}

func (r *readDeadlineRecorder) SetReadDeadline(t time.Time) error {
	select {
	case r.deadlines <- t:
	default:
	}
	return r.Conn.SetReadDeadline(t)
}

func TestHandleConnSetsIdleReadDeadline(t *testing.T) {
	// Before the fix, a connection that never sent anything (or never sent
	// a trailing '\n') was held open by handleConn forever -- no deadline
	// was ever set on it at all. This asserts the mechanism directly
	// (SetReadDeadline is called, with a generous-but-bounded deadline)
	// rather than waiting out a real 30s timeout.
	serverSide, clientSide := net.Pipe()
	defer clientSide.Close()

	rec := &readDeadlineRecorder{Conn: serverSide, deadlines: make(chan time.Time, 1)}
	srv := NewTextServer(&deadhorsetest.NoOpThrottler{}, 0)
	go srv.handleConn(rec)

	select {
	case deadline := <-rec.deadlines:
		remaining := time.Until(deadline)
		assert.Greater(t, remaining, 20*time.Second, "an idle connection should get a generous deadline, not an unbounded read")
		assert.LessOrEqual(t, remaining, idleReadTimeout, "deadline should not exceed idleReadTimeout")
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn never set a read deadline")
	}
}

func TestTextServerLineTooLongClosesConnection(t *testing.T) {
	srv := NewTextServer(&deadhorsetest.NoOpThrottler{}, 16)
	addr := startTextServerListener(t, srv)
	client := dialTestServer(t, addr)

	client.send(strings.Repeat("x", 1000))
	assert.Equal(t, "ERROR line too long", client.recv())

	_, err := client.r.ReadString('\n')
	assert.Error(t, err, "the server should close the connection after a too-long line")
}

func TestTextServerBlankLinesAreIgnored(t *testing.T) {
	addr := startTestServer(t, &deadhorsetest.NoOpThrottler{})
	client := dialTestServer(t, addr)

	client.send("")
	client.send("PING")
	assert.Equal(t, "PONG", client.recv(), "a blank line should be silently skipped, not answered or fatal")
}

func TestTextServerConcurrentConnections(t *testing.T) {
	// Dials and asserts happen on background goroutines here, so this test
	// deliberately avoids dialTestServer/require (both of which call
	// t.FailNow-family methods that must only run on the test's own
	// goroutine) and reports failures back over a channel instead.
	th := NewInMemoryThrottler(0, time.Hour)
	defer th.Close()
	addr := startTestServer(t, th)

	const clients = 20
	errs := make(chan error, clients)
	for i := 0; i < clients; i++ {
		go func() {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			r := bufio.NewReader(conn)

			for j := 0; j < 10; j++ {
				if _, err := conn.Write([]byte("PING\n")); err != nil {
					errs <- err
					return
				}
				line, err := r.ReadString('\n')
				if err != nil {
					errs <- err
					return
				}
				if got := strings.TrimRight(line, "\r\n"); got != "PONG" {
					errs <- fmt.Errorf("got %q, want PONG", got)
					return
				}
			}
			errs <- nil
		}()
	}
	for i := 0; i < clients; i++ {
		select {
		case err := <-errs:
			assert.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for concurrent clients")
		}
	}
}
