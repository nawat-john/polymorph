// Package metrics holds the Prometheus metric definitions used by the
// services (design-plan.md section 10).
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Ingestor metrics, per the design-plan.md section 10 table.
var (
	IngestorWSConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ingestor_ws_connections",
		Help: "Current number of open Polymarket CLOB WebSocket shard connections.",
	})
	IngestorEventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ingestor_events_total",
		Help: "Total normalized events received from Polymarket, labeled by kind.",
	}, []string{"kind"})
	IngestorReconnectsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingestor_reconnects_total",
		Help: "Total number of WebSocket shard reconnect attempts.",
	})
	IngestorProduceErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingestor_produce_errors_total",
		Help: "Total number of Kafka produce errors.",
	})
	IngestorDroppedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingestor_dropped_total",
		Help: "Total number of events dropped because the internal buffer was full.",
	})
)

// Processor metrics, per the design-plan.md section 10 table.
var (
	ProcessorConsumeLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "processor_consume_lag",
		Help: "Approximate consumer lag (records) on pm.raw for the processor consumer group.",
	})
	ProcessorEventsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "processor_events_total",
		Help: "Total number of pm.raw events processed.",
	})
	ProcessorAlertsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "processor_alerts_total",
		Help: "Total number of surge alerts produced to pm.alerts.",
	})
	ProcessorHandleSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "processor_handle_seconds",
		Help:    "Time to handle one pm.raw event: state update plus derived produces.",
		Buckets: prometheus.DefBuckets,
	})
)

// Handler serves the default Prometheus registry: the metrics above, plus
// the Go runtime/process collectors client_golang registers there by
// default (design-plan.md section 10: "system" row).
func Handler() http.Handler { return promhttp.Handler() }

// IngestorAdapter adapts the package-level ingestor metrics above to the
// small Metrics interface internal/polymarket/clobws depends on, so that
// package stays decoupled from prometheus/client_golang.
type IngestorAdapter struct{}

func (IngestorAdapter) SetWSConnections(n int) { IngestorWSConnections.Set(float64(n)) }

func (IngestorAdapter) IncEventsTotal(kind string, n int) {
	IngestorEventsTotal.WithLabelValues(kind).Add(float64(n))
}

func (IngestorAdapter) IncReconnects() { IngestorReconnectsTotal.Inc() }
