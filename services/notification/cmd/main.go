package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/mildlybrutal/overcast/services/notification/internal/actor"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize Supervisor
	sup := actor.NewSupervisor(ctx, rdb)

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
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

		// Socket Writer Goroutine
		go func() {
			defer func() {
				sup.RouteClientLeave(matchID, client)
				client.Conn.Close()
			}()

			for msg := range client.Send {
				if err := client.Conn.WriteMessage(websocket.TextMessage, msg); err != nil {
					return
				}
			}
		}()
	})

	log.Printf("Notification Service (Actor Model) running on port %s...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
