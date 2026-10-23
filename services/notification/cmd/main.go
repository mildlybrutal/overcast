package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mildlybrutal/overcast/services/notification/internal/actor"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

const (
	writeWait      = 10 * time.Second  // max time for one write to the peer
	pongWait       = 60 * time.Second  // peer must answer a ping within this
	pingPeriod     = pongWait * 9 / 10 // send pings a bit more often than pongWait
	maxMessageSize = 512               // clients only send control frames
	shutdownWait   = 10 * time.Second
)

// newUpgrader allows only the comma-separated origins in ALLOWED_ORIGINS.
// When unset (local development) every origin is accepted, with a warning.
func newUpgrader() websocket.Upgrader {
	raw := os.Getenv("ALLOWED_ORIGINS")
	if raw == "" {
		log.Println("WARNING: ALLOWED_ORIGINS not set; accepting WebSocket connections from any origin")
		return websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	}

	allowed := map[string]bool{}
	for _, o := range strings.Split(raw, ",") {
		allowed[strings.TrimSpace(o)] = true
	}
	return websocket.Upgrader{CheckOrigin: func(r *http.Request) bool {
		return allowed[r.Header.Get("Origin")]
	}}
}

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	// Cancelled on SIGINT/SIGTERM: stops every actor and their Redis subscriptions.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Initialize Supervisor
	sup := actor.NewSupervisor(ctx, rdb)
	upgrader := newUpgrader()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		matchID := r.URL.Query().Get("match_id")
		if matchID == "" {
			http.Error(w, "Missing match_id", http.StatusBadRequest)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("Upgrade error: %v", err)
			return
		}

		client := &actor.Client{
			ID:   uuid.New().String(),
			Conn: conn,
			Send: make(chan []byte, 256),
		}

		// Route client to Match Actor
		sup.RouteClientJoin(matchID, client)

		go writePump(client)
		go readPump(sup, matchID, client)
	})

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		log.Println("Shutting down notification service...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownWait)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("Notification Service (Actor Model) running on port %s...", port)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Server error: %v", err)
	}
}

// writePump is the only goroutine that writes to the connection. It ends when
// the actor closes client.Send (client left, was too slow, or the actor
// stopped) or a write fails, and closes the socket so readPump ends too.
func writePump(client *actor.Client) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		client.Conn.Close()
	}()

	for {
		select {
		case msg, ok := <-client.Send:
			client.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = client.Conn.WriteMessage(websocket.CloseMessage, nil)
				return
			}
			if err := client.Conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			client.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := client.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// readPump detects disconnects: a closed socket, or a peer that stopped
// answering pings (half-open TCP), makes ReadMessage fail.
func readPump(sup *actor.Supervisor, matchID string, client *actor.Client) {
	defer func() {
		sup.RouteClientLeave(matchID, client)
		client.Conn.Close()
	}()

	client.Conn.SetReadLimit(maxMessageSize)
	client.Conn.SetReadDeadline(time.Now().Add(pongWait))
	client.Conn.SetPongHandler(func(string) error {
		return client.Conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		if _, _, err := client.Conn.ReadMessage(); err != nil {
			return
		}
	}
}
