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

// Gateway metrics, per the design-plan.md section 10 table.
var (
	GatewayClients = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_clients",
		Help: "Current number of connected WebSocket clients.",
	})
	GatewaySubscriptions = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_subscriptions",
		Help: "Current total number of channel subscriptions across all clients.",
	})
	GatewayMessagesOutTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_messages_out_total",
		Help: "Total number of WebSocket frames written to clients.",
	})
	GatewayBytesOutTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_bytes_out_total",
		Help: "Total number of bytes written to clients.",
	})
	GatewayE2ELatencySeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gateway_e2e_latency_seconds",
		Help:    "End-to-end latency from ingestor receive (Tick.RecvTS) to gateway consuming the tick off pm.ticks.",
		Buckets: prometheus.DefBuckets,
	})
	GatewaySlowClientEvictionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_slow_client_evictions_total",
		Help: "Total number of clients evicted for a persistently full outbound queue.",
	})
	GatewayQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_queue_depth",
		Help: "Sum of all clients' outbound queue lengths, sampled each flush cycle.",
	})
)

// Handler serves the default Prometheus registry: the metrics above, plus
// the Go runtime/process collectors client_golang registers there by
// default (design-plan.md section 10: "system" row).
func Handler() http.Handler { return promhttp.Handler() }

// GatewayAdapter adapts the package-level gateway metrics above to the small
// hub.Metrics interface internal/hub depends on, so that package stays
// decoupled from prometheus/client_golang (same pattern as IngestorAdapter).
type GatewayAdapter struct{}

func (GatewayAdapter) SetClients(n int)         { GatewayClients.Set(float64(n)) }
func (GatewayAdapter) SetSubscriptions(n int64) { GatewaySubscriptions.Set(float64(n)) }
func (GatewayAdapter) AddMessagesOut(n int)     { GatewayMessagesOutTotal.Add(float64(n)) }
func (GatewayAdapter) AddBytesOut(n int)        { GatewayBytesOutTotal.Add(float64(n)) }
func (GatewayAdapter) IncSlowClientEvictions()  { GatewaySlowClientEvictionsTotal.Inc() }
func (GatewayAdapter) SetQueueDepth(n int)      { GatewayQueueDepth.Set(float64(n)) }

// IngestorAdapter adapts the package-level ingestor metrics above to the
// small Metrics interface internal/polymarket/clobws depends on, so that
// package stays decoupled from prometheus/client_golang.
type IngestorAdapter struct{}

func (IngestorAdapter) SetWSConnections(n int) { IngestorWSConnections.Set(float64(n)) }

func (IngestorAdapter) IncEventsTotal(kind string, n int) {
	IngestorEventsTotal.WithLabelValues(kind).Add(float64(n))
}

func (IngestorAdapter) IncReconnects() { IngestorReconnectsTotal.Inc() }
