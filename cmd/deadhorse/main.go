package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

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

	// Without this, the only way ListenAndServe below ever returns is a
	// genuine accept error, which goes straight to log.Fatalf -> os.Exit --
	// skipping every deferred call above, including throttler.Close() (and
	// therefore its GC goroutine's shutdown). Wiring SIGINT/SIGTERM into an
	// explicit textServer.Close() gives ListenAndServe a clean, expected way
	// to return so those defers actually run.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Println("Shutting down...")
		textServer.Close()
	}()

	addr := fmt.Sprintf("%s:%d", *host, *port)
	log.Printf("Starting DeadHorse server on %s (metrics on port %d)...", addr, *prometheusPort)
	if err := textServer.ListenAndServe(addr); err != nil {
		log.Fatalf("text protocol server stopped: %v", err)
	}
}

func serveMetrics(host string, port int) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{EnableOpenMetrics: true}))

	go func() {
		if err := http.ListenAndServe(fmt.Sprintf("%s:%d", host, port), mux); err != nil {
			log.Printf("metrics server stopped: %v", err)
		}
	}()
}
