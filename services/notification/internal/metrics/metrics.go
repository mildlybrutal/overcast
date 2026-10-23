package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Track active Match Actors running in memory
	ActiveActors = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "overcast_active_match_actors",
		Help: "Current number of active Match Actors running in memory",
	})

	// Track total WebSocket clients connected
	ActiveClients = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "overcast_active_websocket_clients",
		Help: "Current number of active WebSocket connections across all match actors",
	})

	// Track total messages processed by Actors
	MessagesDelivered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "overcast_actor_messages_delivered_total",
		Help: "Total number of score update messages delivered to clients",
	}, []string{"match_id"})
)
