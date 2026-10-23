package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "overcast_api_http_requests_total",
		Help: "Total number of HTTP requests handled by the Query API",
	}, []string{"endpoint", "status"})

	CacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "overcast_api_cache_hits_total",
		Help: "Total number of score queries served from Redis cache",
	})

	CacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "overcast_api_cache_misses_total",
		Help: "Total number of score queries falling back to PostgreSQL",
	})

	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "overcast_api_http_request_duration_seconds",
		Help:    "Duration of HTTP requests in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"endpoint", "status"})
)
