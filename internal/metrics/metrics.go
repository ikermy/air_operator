package metrics

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	registerOnce sync.Once

	MessageEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "air",
			Subsystem: "oper",
			Name:      "message_events_total",
			Help:      "Total number of message events by source, result and type.",
		},
		[]string{"source", "result", "message_type"},
	)
	TelegramSendTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "air",
			Subsystem: "oper",
			Name:      "telegram_send_total",
			Help:      "Total number of Telegram send attempts by operation and result.",
		},
		[]string{"operation", "result"},
	)
	TelegramSendDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "air",
			Subsystem: "oper",
			Name:      "telegram_send_duration_seconds",
			Help:      "Duration of Telegram send operations in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"operation"},
	)
	HistoryRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "air",
			Subsystem: "oper",
			Name:      "history_requests_total",
			Help:      "Total number of dialog history fetches by status.",
		},
		[]string{"status"},
	)
	HistoryRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "air",
			Subsystem: "oper",
			Name:      "history_request_duration_seconds",
			Help:      "Duration of dialog history fetches in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"status"},
	)
	OperatorSyncTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "air",
			Subsystem: "oper",
			Name:      "operator_sync_total",
			Help:      "Total number of operator sync attempts by status.",
		},
		[]string{"status"},
	)
	SSESessionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "air",
			Subsystem: "oper",
			Name:      "sse_sessions_total",
			Help:      "Total number of SSE session lifecycle events.",
		},
		[]string{"event"},
	)
	HTTPRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "air",
			Subsystem: "oper",
			Name:      "http_requests_total",
			Help:      "Total number of HTTP requests handled by the service.",
		},
		[]string{"method", "route", "status"},
	)
	HTTPRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "air",
			Subsystem: "oper",
			Name:      "http_request_duration_seconds",
			Help:      "Duration of HTTP requests handled by the service.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"method", "route"},
	)
	ActiveDialogs = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "air",
			Subsystem: "oper",
			Name:      "active_dialogs",
			Help:      "Current number of active dialogs tracked in memory.",
		},
	)
	ActiveOperators = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "air",
			Subsystem: "oper",
			Name:      "active_operators",
			Help:      "Current number of configured Telegram operator bindings tracked in memory.",
		},
	)
)

func Register() {
	registerOnce.Do(func() {
		registerCollector(collectors.NewGoCollector())
		registerCollector(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
		registerCollector(MessageEvents)
		registerCollector(TelegramSendTotal)
		registerCollector(TelegramSendDuration)
		registerCollector(HistoryRequestsTotal)
		registerCollector(HistoryRequestDuration)
		registerCollector(OperatorSyncTotal)
		registerCollector(SSESessionsTotal)
		registerCollector(HTTPRequests)
		registerCollector(HTTPRequestDuration)
		registerCollector(ActiveDialogs)
		registerCollector(ActiveOperators)
	})
}

func registerCollector(collector prometheus.Collector) {
	if err := prometheus.Register(collector); err != nil {
		var alreadyRegisteredError prometheus.AlreadyRegisteredError
		if errors.As(err, &alreadyRegisteredError) {
			return
		}
		panic(err)
	}
}

func Handler() http.Handler {
	Register()
	return promhttp.Handler()
}

func ObserveDuration(observer prometheus.Observer, startedAt time.Time) {
	observer.Observe(time.Since(startedAt).Seconds())
}

func NormalizeRoute(path string) string {
	if path == "" {
		return "unknown"
	}
	if path == "/metrics" {
		return path
	}
	trimmed := strings.TrimSuffix(path, "/")
	if trimmed == "" {
		return "/"
	}
	return trimmed
}

func ObserveMessageEvent(source, result, messageType string) {
	MessageEvents.WithLabelValues(
		normalizeLabel(source, "unknown"),
		normalizeLabel(result, "unknown"),
		normalizeLabel(messageType, "unknown"),
	).Inc()
}

func ObserveTelegramSend(operation, result string, startedAt time.Time) {
	operation = normalizeLabel(operation, "unknown")
	TelegramSendTotal.WithLabelValues(operation, normalizeLabel(result, "unknown")).Inc()
	ObserveDuration(TelegramSendDuration.WithLabelValues(operation), startedAt)
}

func ObserveHistoryRequest(status string, startedAt time.Time) {
	status = normalizeLabel(status, "unknown")
	HistoryRequestsTotal.WithLabelValues(status).Inc()
	ObserveDuration(HistoryRequestDuration.WithLabelValues(status), startedAt)
}

func ObserveOperatorSync(status string) {
	OperatorSyncTotal.WithLabelValues(normalizeLabel(status, "unknown")).Inc()
}

func ObserveSSESession(event string) {
	SSESessionsTotal.WithLabelValues(normalizeLabel(event, "unknown")).Inc()
}

func SetActiveDialogs(count int) {
	ActiveDialogs.Set(float64(count))
}

func SetActiveOperators(count int) {
	ActiveOperators.Set(float64(count))
}

func normalizeLabel(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}
