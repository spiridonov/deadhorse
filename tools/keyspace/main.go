// Command keyspace drives DeadHorse with a large number of distinct keys
// rather than a large number of requests. Where tools/loadtest samples a
// (possibly huge) keyspace at a target request rate to simulate realistic
// traffic, keyspace does the opposite: it iterates once over every key
// index in [0, N), sending exactly one request per key per pass, then keeps
// repeating that same pass for -duration. The point is to actually create N
// distinct keys on the server as fast as possible and then keep touching
// them, so you can watch whether the server's key count stays bounded
// instead of growing forever -- see -stats-interval, which polls each
// shard's STATS command directly and prints its live key count. Run for at
// least a couple of the server's -gc-interval periods to actually see that
// plateau.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/spiridonov/deadhorse"
	"github.com/spiridonov/deadhorse/client"
)

var (
	addrsFlag        = flag.String("addrs", "localhost:9000", "Comma-separated list of DeadHorse shard addresses")
	numKeys          = flag.Int("keys", 100_000, "Number of distinct keys to generate, indexed 0..keys-1")
	keyPrefix        = flag.String("key-prefix", "keyspace", "Prefix for generated key names (keys are <prefix>-<n>)")
	duration         = flag.Duration("duration", 2*time.Minute, "How long to keep looping over the keyspace (set well above the server's -gc-interval to see the key count plateau)")
	concurrency      = flag.Int("concurrency", 50, "Number of concurrent client connections, each looping over its own slice of the keyspace")
	requestTimeout   = flag.Duration("request-timeout", 2*time.Second, "Per-request timeout")
	failClosed       = flag.Bool("fail-closed", false, "Treat an unreachable shard as throttled instead of the client's default fail-open")
	capacityFlag     = flag.Int64("capacity", 1000, "Limit.Capacity sent with every request")
	emissionInterval = flag.Duration("emission-interval", 1*time.Millisecond, "Limit.Rate.Period sent with every request (Units is always 1)")
	costFlag         = flag.Int64("cost", 1, "Cost sent with every request")
	peekFlag         = flag.Bool("peek", false, "Send requests in peek mode (never consumes a bucket)")
	reportInterval   = flag.Duration("report-interval", 1*time.Second, "How often to print progress")
	statsInterval    = flag.Duration("stats-interval", 5*time.Second, "How often to poll each shard's STATS command for its live key count (0 disables)")
)

func main() {
	log.SetFlags(0)
	flag.Parse()

	addrs := splitAddrs(*addrsFlag)
	if len(addrs) == 0 {
		log.Fatal("-addrs must name at least one shard")
	}
	if *numKeys <= 0 {
		log.Fatal("-keys must be at least 1")
	}
	if *concurrency <= 0 {
		log.Fatal("-concurrency must be at least 1")
	}
	if *concurrency > *numKeys {
		log.Printf("note: -concurrency=%d exceeds -keys=%d, capping to %d", *concurrency, *numKeys, *numKeys)
		*concurrency = *numKeys
	}

	limit := deadhorse.Limit{Capacity: *capacityFlag, Rate: deadhorse.Rate{Units: 1, Period: *emissionInterval}}

	log.Printf("deadhorse keyspace: %d keys, %d connections, running for %s -> %s",
		*numKeys, *concurrency, *duration, strings.Join(addrs, ","))

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	runCtx, cancel := context.WithTimeout(runCtx, *duration)
	defer cancel()

	st := &stats{}
	done := make(chan struct{})
	for i := 0; i < *concurrency; i++ {
		opts := []client.Option{client.WithTimeout(*requestTimeout)}
		if *failClosed {
			opts = append(opts, client.WithFailClosed())
		}
		c := client.NewShardedClient(addrs, opts...)

		go func(workerIdx int) {
			defer func() {
				c.Close()
				done <- struct{}{}
			}()
			runWorker(runCtx, workerIdx, *concurrency, *numKeys, *keyPrefix, c, limit, *costFlag, *peekFlag, *requestTimeout, st)
		}(i)
	}

	if *statsInterval > 0 {
		go pollStats(runCtx, addrs, *statsInterval)
	}

	start := time.Now()
	reportTicker := time.NewTicker(*reportInterval)
	defer reportTicker.Stop()

	var lastRequests int64
	var lastElapsed time.Duration
	finished := 0
	for finished < *concurrency {
		select {
		case <-reportTicker.C:
			elapsed := time.Since(start)
			requests := st.requests.Load()
			printProgress(elapsed, elapsed-lastElapsed, requests, requests-lastRequests, *numKeys, st)
			lastElapsed = elapsed
			lastRequests = requests
		case <-done:
			finished++
		}
	}

	fmt.Println("---")
	printProgress(time.Since(start), 0, st.requests.Load(), 0, *numKeys, st)
}

// stats is a set of plain request-outcome counters, updated by every
// worker concurrently. There's deliberately no per-key or per-latency
// bookkeeping here -- the point of this tool is key count, not timing.
type stats struct {
	requests  atomic.Int64
	throttled atomic.Int64
	errors    atomic.Int64
}

func (s *stats) record(throttled, errored bool) {
	s.requests.Add(1)
	if throttled {
		s.throttled.Add(1)
	}
	if errored {
		s.errors.Add(1)
	}
}

func printProgress(elapsed, window time.Duration, requests, windowRequests int64, numKeys int, st *stats) {
	rate := 0.0
	if window > 0 {
		rate = float64(windowRequests) / window.Seconds()
	}
	passes := float64(requests) / float64(numKeys)
	fmt.Printf("[%6s] requests=%-9d (%8.1f/s)  passes=%-6.2f  throttled=%s  errors=%s\n",
		elapsed.Round(time.Second), requests, rate, passes,
		pctString(st.throttled.Load(), requests),
		pctString(st.errors.Load(), requests),
	)
}

func pctString(n, total int64) string {
	if total == 0 {
		return "0.0%"
	}
	return fmt.Sprintf("%.1f%%", 100*float64(n)/float64(total))
}

func splitAddrs(s string) []string {
	var addrs []string
	for _, a := range strings.Split(s, ",") {
		a = strings.TrimSpace(a)
		if a != "" {
			addrs = append(addrs, a)
		}
	}
	return addrs
}

// runWorker repeatedly sweeps this worker's slice of [0, numKeys) --
// indices workerIdx, workerIdx+concurrency, workerIdx+2*concurrency, ... --
// sending exactly one request per key per sweep, until ctx is done. Unlike
// loadtest, there's no target rate here: a worker sends as fast as it can,
// since the goal is to populate (and keep touching) as many distinct keys
// as possible, not to model realistic traffic.
func runWorker(ctx context.Context, workerIdx, concurrency, numKeys int, prefix string, c *client.ShardedClient, limit deadhorse.Limit, cost int64, peek bool, timeout time.Duration, st *stats) {
	for {
		for i := workerIdx; i < numKeys; i += concurrency {
			select {
			case <-ctx.Done():
				return
			default:
			}

			key := fmt.Sprintf("%s-%d", prefix, i)
			entry := deadhorse.RequestEntry{Key: key, Limit: limit, Cost: cost, Peek: peek}

			reqCtx, cancel := context.WithTimeout(ctx, timeout)
			resp, err := c.Throttle(reqCtx, key, []deadhorse.RequestEntry{entry})
			cancel()

			throttled := len(resp) > 0 && resp[0].Throttled
			st.record(throttled, err != nil)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// pollStats periodically queries every shard's STATS command directly over
// a raw DHP/1 connection (bypassing client.ShardedClient, which has no
// STATS support) and prints each shard's live key count -- the actual
// number this tool exists to watch level off once GC starts recycling idle
// keys.
func pollStats(ctx context.Context, addrs []string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			printShardStats(addrs)
		}
	}
}

func printShardStats(addrs []string) {
	parts := make([]string, len(addrs))
	for i, addr := range addrs {
		keys, err := fetchKeyCount(addr)
		if err != nil {
			parts[i] = fmt.Sprintf("%s=err(%v)", addr, err)
			continue
		}
		parts[i] = fmt.Sprintf("%s=%d", addr, keys)
	}
	log.Printf("server keys: %s", strings.Join(parts, "  "))
}

func fetchKeyCount(addr string) (int, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write([]byte("STATS\n")); err != nil {
		return 0, err
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(line)
	if len(fields) != 3 || fields[0] != "STATS" {
		return 0, fmt.Errorf("unexpected response %q", strings.TrimSpace(line))
	}
	return strconv.Atoi(fields[2])
}
