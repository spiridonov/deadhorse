package server

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics are package-level (rather than per-TextServer/per-store) so that a
// process running exactly one of each -- the normal case, see cmd/deadhorse
// -- gets one set of series on the default registry without any wiring.
// Tests that spin up many short-lived TextServers/stores in one binary just
// share these same series; nothing here is asserted on in tests, so that's
// harmless.
var (
	// keysGauge reports the store's key count by generation ("hot"/"cold"),
	// refreshed periodically by (*store).updateKeyMetrics -- see
	// metricsUpdateInterval. It's a plain Gauge rather than a GaugeFunc
	// because a GaugeFunc's callback is fixed at registration time, and
	// there's no single store to close over at package init.
	keysGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "deadhorse",
		Name:      "keys",
		Help:      "Number of keys currently held by the store, by generation.",
	}, []string{"generation"})

	connectionsGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "deadhorse",
		Name:      "connections",
		Help:      "Number of currently open DHP/1 client connections.",
	})

	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "deadhorse",
		Name:      "requests_total",
		Help:      "Number of DHP/1 command lines handled, by command.",
	}, []string{"command"})

	requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "deadhorse",
		Name:      "request_duration_seconds",
		Help:      "Time to handle one DHP/1 command line, by command.",
		// Native-histogram-only: Buckets is deliberately left unset (see
		// throttleBatchSize below for why that leaves no classic buckets).
		NativeHistogramBucketFactor:    nativeHistogramBucketFactor,
		NativeHistogramMaxBucketNumber: nativeHistogramMaxBucketNumber,
	}, []string{"command"})

	throttleEntriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "deadhorse",
		Name:      "throttle_entries_total",
		Help:      "Number of THROTTLE entries evaluated, by mode (real/peek) and result (admitted/throttled/err).",
	}, []string{"mode", "result"})

	throttleBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "deadhorse",
		Name:      "throttle_batch_size",
		Help:      "Number of entries carried by one THROTTLE line.",
		// Leaving Buckets nil/empty while NativeHistogramBucketFactor is set
		// means no classic buckets are created at all -- see HistogramOpts.Buckets.
		NativeHistogramBucketFactor:    nativeHistogramBucketFactor,
		NativeHistogramMaxBucketNumber: nativeHistogramMaxBucketNumber,
	})

	lineLength = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace:                      "deadhorse",
		Name:                           "line_length_bytes",
		Help:                           "Length in bytes of each protocol line read, excluding the terminator.",
		NativeHistogramBucketFactor:    nativeHistogramBucketFactor,
		NativeHistogramMaxBucketNumber: nativeHistogramMaxBucketNumber,
	})

	lineTooLongTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "deadhorse",
		Name:      "line_too_long_total",
		Help:      "Number of connections dropped for exceeding the configured maximum line size.",
	})
)

const (
	// nativeHistogramBucketFactor of 1.1 is the trade-off the client_golang
	// docs recommend: each bucket at most 10% wider than the last (8 buckets
	// per power of two).
	nativeHistogramBucketFactor = 1.1

	// nativeHistogramMaxBucketNumber caps how many sparse buckets a single
	// histogram may populate. line_length_bytes and throttle_batch_size are
	// driven by values a DHP/1 client controls directly, so leaving this
	// unbounded (the default) would make an unlimited-bucket native
	// histogram a memory-DoS vector -- see HistogramOpts.NativeHistogramMaxBucketNumber.
	nativeHistogramMaxBucketNumber = 160
)
