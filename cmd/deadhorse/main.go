package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spiridonov/deadhorse/server"
)

var (
	host           = flag.String("host", "", "The DeadHorse and Prometheus listen host (empty = all interfaces)")
	port           = flag.Int("port", 9000, "The DeadHorse server port")
	prometheusPort = flag.Int("prometheus-port", 9090, "The Prometheus metrics port")
	stripes        = flag.Int("stripes", server.DefaultStripes, "Number of concurrency stripes in the key/bucket store")
	gcInterval     = flag.Duration("gc-interval", server.DefaultGCInterval, "How often idle keys are garbage-collected")
	maxLineSize    = flag.Int("max-line-size", server.DefaultMaxLineSize, "Maximum DHP/1 protocol line size in bytes")
)

func main() {
	flag.Parse()

	log.Println("Initializing...")

	serveMetrics(*host, *prometheusPort)

	throttler := server.NewInMemoryThrottler(*stripes, *gcInterval)
	defer throttler.Close()

	textServer := server.NewTextServer(throttler, *maxLineSize)
	defer textServer.Close()

	addr := fmt.Sprintf("%s:%d", *host, *port)
	log.Printf("Starting DeadHorse server on %s (metrics on port %d)...", addr, *prometheusPort)
	if err := textServer.ListenAndServe(addr); err != nil {
		log.Fatalf("text protocol server stopped: %v", err)
	}
}

// serveMetrics exposes the process's default Prometheus registry -- Go
// runtime/process stats plus the server package's own metrics (key counts,
// request rate/latency, connections, line/batch sizes -- see
// server/metrics.go), since server registers into this same default
// registry -- on /metrics, in the background.
func serveMetrics(host string, port int) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{EnableOpenMetrics: true}))

	go func() {
		if err := http.ListenAndServe(fmt.Sprintf("%s:%d", host, port), mux); err != nil {
			log.Printf("metrics server stopped: %v", err)
		}
	}()
}
