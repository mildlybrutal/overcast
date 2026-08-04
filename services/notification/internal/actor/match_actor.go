package actor

import (
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

type MatchActor struct {
	matchID   string
	mailbox   chan ActorMessage
	clients   map[*Client]bool
	rdb       *redis.Client
	onStopped func(matchID string)
}

func NewMatchActor(matchID string, rdb *redis.Client, onStopped func(matchID string)) *MatchActor {
	return &MatchActor{
		matchID:   matchID,
		mailbox:   make(chan ActorMessage),
		clients:   make(map[*Client]bool),
		rdb:       rdb,
		onStopped: onStopped, // Callback to notify Supervisor when destroyed
	}
}

func (a *MatchActor) Send(msg ActorMessage) {
	a.mailbox <- msg
}

func (a *MatchActor) Start(ctx context.Context) {
	log.Printf("[Actor: %s] Spawned and running", a.matchID)

	// Spawn a background goroutine to stream from Redis directly into this actor's mailbox
	go a.listenRedis(ctx)

	// Inactivity timer: auto-kill actor if 0 clients for 60 seconds
	idleTimer := time.NewTimer(60 * time.Second)
	idleTimer.Stop()

	defer func() {
		log.Printf("[Actor: %s] Self-destructing and cleaning up resources", a.matchID)
		for client := range a.clients {
			close(client.Send)
		}
		a.onStopped(a.matchID)
	}()

	// --- Actor Event Loop ---
	for {
		select {
		case <-ctx.Done():
			return

		case <-idleTimer.C:
			if len(a.clients) == 0 {
				log.Printf("[Actor: %s] Idle timeout reached (0 clients). Stopping.", a.matchID)
				return
			}

		case msg := <-a.mailbox:
			switch msg.Type {

			case ClientJoin:
				a.clients[msg.Client] = true
				idleTimer.Stop() // Cancel idle shutdown if someone joined
				log.Printf("[Actor: %s] Client joined. Total active clients: %d", a.matchID, len(a.clients))

			case ClientLeave:
				if _, exists := a.clients[msg.Client]; exists {
					delete(a.clients, msg.Client)
					close(msg.Client.Send)
					log.Printf("🎭 [Actor: %s] Client left. Remaining clients: %d", a.matchID, len(a.clients))
				}
				if len(a.clients) == 0 {
					idleTimer.Reset(30 * time.Second) // Start shutdown countdown
				}

			case BroadcastUpdate:
				for client := range a.clients {
					select {
					case client.Send <- msg.Payload:
					default:
						// Client buffer full, drop and remove client safely
						close(client.Send)
						delete(a.clients, client)
					}
				}

			case StopActor:
				return
			}
		}
	}
}

func (a *MatchActor) listenRedis(ctx context.Context) {
	channel := "match:updates:" + a.matchID
	pubsub := a.rdb.Subscribe(ctx, channel)
	defer pubsub.Close()

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			// Push Redis payload into actor mailbox
			a.Send(ActorMessage{
				Type:    BroadcastUpdate,
				Payload: []byte(msg.Payload),
			})
		}
	}
}
