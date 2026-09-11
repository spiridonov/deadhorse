// Command loadtest drives sustained, concurrent load against one or more
// DeadHorse shards over the real DHP/1 wire protocol (not the in-process
// Throttler), so it exercises the same server/network path a real client
// would. It's designed to scale to billions of keys, up to ~1000 concurrent
// connections, and request rates up to the low tens of thousands per
// second -- see generateWork and pickKey for how that's kept O(1) in memory
// regardless of key count.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/spiridonov/deadhorse"
	"github.com/spiridonov/deadhorse/client"
)

var (
	addrsFlag        = flag.String("addrs", "localhost:9000", "Comma-separated list of DeadHorse shard addresses")
	numKeys          = flag.Int("keys", 1_000_000, "Total number of distinct keys to generate load across (supports billions)")
	minKeyWeight     = flag.Int("min-key-weight", 1, "Minimum relative selection weight for a key")
	maxKeyWeight     = flag.Int("max-key-weight", 10, "Maximum relative selection weight for a key (a key with weight W is picked, on average, W times as often as a weight-1 key)")
	rateFlag         = flag.Float64("rate", 1000, "Target aggregate requests/sec across all keys and connections")
	duration         = flag.Duration("duration", 30*time.Second, "How long to run the load test")
	concurrency      = flag.Int("concurrency", 200, "Number of concurrent client connections (each opens its own connection per shard; supports up to ~1000)")
	reportInterval   = flag.Duration("report-interval", 1*time.Second, "How often to print interval stats")
	requestTimeout   = flag.Duration("request-timeout", 2*time.Second, "Per-request timeout")
	failClosed       = flag.Bool("fail-closed", false, "Treat an unreachable shard as throttled instead of the client's default fail-open")
	keyPrefix        = flag.String("key-prefix", "loadtest", "Prefix for generated key names (keys are <prefix>-<n>)")
	capacityFlag     = flag.Int64("capacity", 1000, "Limit.Capacity sent with every request")
	emissionInterval = flag.Duration("emission-interval", 1*time.Millisecond, "Limit.EmissionInterval sent with every request")
	costFlag         = flag.Int64("cost", 1, "Cost sent with every request")
	peekFlag         = flag.Bool("peek", false, "Send requests in peek mode (never consumes a bucket)")
	seedFlag         = flag.Uint64("seed", 0, "Seed for per-key weight assignment (0 = pick a random seed each run and log it)")
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
	if *minKeyWeight < 0 || *maxKeyWeight <= 0 || *maxKeyWeight < *minKeyWeight {
		log.Fatalf("invalid weight range: -min-key-weight=%d -max-key-weight=%d", *minKeyWeight, *maxKeyWeight)
	}
	if *concurrency <= 0 {
		log.Fatal("-concurrency must be at least 1")
	}
	if *rateFlag <= 0 {
		log.Fatal("-rate must be positive")
	}

	limit := deadhorse.Limit{Capacity: *capacityFlag, EmissionInterval: *emissionInterval}

	seed := *seedFlag
	if seed == 0 {
		seed = rand.Uint64()
	}

	log.Printf("deadhorse loadtest: %d keys (weight %d-%d), target %.0f req/s, %d connections, seed=%d, running for %s -> %s",
		*numKeys, *minKeyWeight, *maxKeyWeight, *rateFlag, *concurrency, seed, *duration, strings.Join(addrs, ","))
	if *concurrency > 200 {
		log.Printf("note: %d connections opened per shard address; raise the process's open-file limit (ulimit -n) if connections start failing", *concurrency)
	}

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	runCtx, cancel := context.WithTimeout(runCtx, *duration)
	defer cancel()

	work := make(chan string, *concurrency*4)
	go generateWork(runCtx, *numKeys, *minKeyWeight, *maxKeyWeight, seed, *keyPrefix, *rateFlag, work)

	st := newStats()
	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		opts := []client.Option{client.WithTimeout(*requestTimeout)}
		if *failClosed {
			opts = append(opts, client.WithFailClosed())
		}
		c := client.NewShardedClient(addrs, opts...)

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer c.Close()
			runWorker(runCtx, c, limit, *costFlag, *peekFlag, *requestTimeout, work, st)
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	start := time.Now()
	ticker := time.NewTicker(*reportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			st.snapshot(*reportInterval).print(os.Stdout, fmt.Sprintf("[%6s]", time.Since(start).Round(time.Second)))
		case <-done:
			fmt.Println("---")
			st.final(time.Since(start)).print(os.Stdout, "[total]")
			return
		}
	}
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

// generateWork paces itself to rate requests/sec (see rateTicker) and, for
// each tick, picks one key (see pickKey) and sends its name into out. It
// keeps going until ctx is done, at which point it closes out so workers
// drain and exit.
//
// There is deliberately no per-key state anywhere -- not a count, not a
// remaining-requests counter, nothing indexed by key. A key's selection
// weight is derived fresh, on demand, from a hash of its index (see
// keyWeight), so memory use here is O(1) regardless of whether -keys is a
// thousand or a billion.
func generateWork(ctx context.Context, numKeys, minWeight, maxWeight int, seed uint64, prefix string, rate float64, out chan<- string) {
	defer close(out)

	ticks := rateTicker(ctx, rate)
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-ticks:
			if !ok {
				return
			}
			key := fmt.Sprintf("%s-%d", prefix, pickKey(numKeys, minWeight, maxWeight, seed))
			select {
			case <-ctx.Done():
				return
			case out <- key:
			}
		}
	}
}

// rateTicker sends on the returned channel at a steady rate requests/sec
// until ctx is done, at which point it closes the channel.
func rateTicker(ctx context.Context, rate float64) <-chan struct{} {
	out := make(chan struct{})
	go func() {
		defer close(out)
		interval := time.Duration(float64(time.Second) / rate)
		if interval <= 0 {
			interval = 1 // effectively unthrottled; a Ticker panics on <=0
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				select {
				case out <- struct{}{}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// pickKey chooses a key index in [0, numKeys) with probability proportional
// to that key's weight in [minWeight, maxWeight], via rejection sampling:
// try a uniformly random index, accept it with probability weight/maxWeight,
// otherwise retry. Expected tries per success is a small constant (roughly
// 2*maxWeight/(minWeight+maxWeight)), independent of numKeys -- this is what
// lets weighting work without ever storing a per-key value.
func pickKey(numKeys, minWeight, maxWeight int, seed uint64) int {
	for {
		i := rand.IntN(numKeys)
		if rand.IntN(maxWeight) < keyWeight(i, minWeight, maxWeight, seed) {
			return i
		}
	}
}

// keyWeight deterministically derives key i's weight in [minWeight,
// maxWeight] from a hash of i and seed -- the same index always yields the
// same weight, without it ever being stored anywhere.
func keyWeight(i, minWeight, maxWeight int, seed uint64) int {
	if maxWeight <= minWeight {
		return minWeight
	}
	h := mix64(uint64(i) ^ seed)
	return minWeight + int(h%uint64(maxWeight-minWeight+1))
}

// mix64 is the 64-bit finalizer from MurmurHash3: a fast, well-distributed,
// non-cryptographic integer hash.
func mix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

// runCtx is the run's overall deadline/cancellation (duration elapsed, or
// Ctrl+C): it's the parent of every per-request context here so that an
// in-flight request aborts immediately on either, rather than a worker
// blocking for up to the full per-request timeout after the run should
// have already stopped.
func runWorker(runCtx context.Context, c *client.ShardedClient, limit deadhorse.Limit, cost int64, peek bool, timeout time.Duration, work <-chan string, st *stats) {
	for key := range work {
		entry := deadhorse.RequestEntry{Key: key, Limit: limit, Cost: cost, Peek: peek}

		ctx, cancel := context.WithTimeout(runCtx, timeout)
		start := time.Now()
		resp, err := c.Throttle(ctx, []deadhorse.RequestEntry{entry})
		elapsed := time.Since(start)
		cancel()

		throttled := len(resp) > 0 && resp[0].Throttled
		st.record(elapsed, throttled, err != nil)
	}
}
