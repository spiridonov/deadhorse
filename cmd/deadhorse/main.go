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
	port           = flag.Int("port", 9000, "The DHP/1 text protocol server port")
	prometheusPort = flag.Int("prometheus-port", 9090, "The Prometheus metrics port")
	stripes        = flag.Int("stripes", 0, "Number of concurrency stripes in the key/bucket store (0 = default)")
	gcInterval     = flag.Duration("gc-interval", 0, "How often idle keys are garbage-collected (0 = default)")
	maxLineSize    = flag.Int("max-line-size", 0, "Maximum DHP/1 protocol line size in bytes (0 = default)")
)

func main() {
	log.Println("Initializing...")

	flag.Parse()

	serveMetrics(*prometheusPort)

	throttler := server.NewInMemoryThrottler(*stripes, *gcInterval)
	defer throttler.Close()

	textServer := server.NewTextServer(throttler, *maxLineSize)
	defer textServer.Close()

	log.Printf("Starting DeadHorse text protocol server on port %d (metrics on port %d)...", *port, *prometheusPort)
	if err := textServer.ListenAndServe(fmt.Sprintf("localhost:%d", *port)); err != nil {
		log.Fatalf("text protocol server stopped: %v", err)
	}
}

// serveMetrics exposes the process's default Prometheus registry (which,
// even with no application-specific metrics registered, already includes Go
// runtime and process stats -- goroutines, GC pauses, memory, open file
// descriptors) on /metrics, in the background.
func serveMetrics(port int) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{EnableOpenMetrics: true}))

	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", port), mux); err != nil {
			log.Printf("metrics server stopped: %v", err)
		}
	}()
}
