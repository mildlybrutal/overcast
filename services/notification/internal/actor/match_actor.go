package actor

import (
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mildlybrutal/overcast/services/notification/internal/metrics"
)

// IdleTimeout is how long an actor with zero clients stays alive before it
// stops itself. A variable so tests can shorten it.
var IdleTimeout = 3 * time.Second

type MatchActor struct {
	matchID   string
	mailbox   chan ActorMessage
	done      chan struct{} // closed once the event loop has exited
	clients   map[*Client]bool
	rdb       *redis.Client
	onStopped func(a *MatchActor)
}

func NewMatchActor(matchID string, rdb *redis.Client, onStopped func(a *MatchActor)) *MatchActor {
	return &MatchActor{
		matchID:   matchID,
		mailbox:   make(chan ActorMessage),
		done:      make(chan struct{}),
		clients:   make(map[*Client]bool),
		rdb:       rdb,
		onStopped: onStopped, // Callback to notify Supervisor when destroyed
	}
}

// Send delivers msg to the actor. It returns false if the actor has already
// stopped, instead of blocking forever on a mailbox nobody reads.
func (a *MatchActor) Send(msg ActorMessage) bool {
	select {
	case a.mailbox <- msg:
		return true
	case <-a.done:
		return false
	}
}

// Done is closed when the actor has stopped.
func (a *MatchActor) Done() <-chan struct{} { return a.done }

func (a *MatchActor) Start(parent context.Context) {
	log.Printf("[Actor: %s] Spawned and running", a.matchID)
	metrics.ActiveActors.Inc()

	// The actor's own context: cancelling it on exit also stops listenRedis
	// and closes its Redis subscription.
	ctx, cancel := context.WithCancel(parent)

	// Spawn a background goroutine to stream from Redis directly into this actor's mailbox
	go a.listenRedis(ctx)

	// Inactivity timer: armed whenever the actor has 0 clients
	idleTimer := time.NewTimer(IdleTimeout)
	idleTimer.Stop()

	defer func() {
		log.Printf("[Actor: %s] Self-destructing and cleaning up resources", a.matchID)
		cancel()
		close(a.done) // unblock every pending and future Send
		metrics.ActiveActors.Dec()
		for client := range a.clients {
			metrics.ActiveClients.Dec()
			close(client.Send)
		}
		a.onStopped(a)
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
				metrics.ActiveClients.Inc()
				idleTimer.Stop() // Cancel idle shutdown if someone joined
				log.Printf("[Actor: %s] Client joined. Total active clients: %d", a.matchID, len(a.clients))

			case ClientLeave:
				if _, exists := a.clients[msg.Client]; exists {
					delete(a.clients, msg.Client)
					metrics.ActiveClients.Dec()
					close(msg.Client.Send)
					log.Printf("[Actor: %s] Client left. Remaining clients: %d", a.matchID, len(a.clients))
				}
				if len(a.clients) == 0 {
					idleTimer.Reset(IdleTimeout) // Start shutdown countdown
				}

			case BroadcastUpdate:
				for client := range a.clients {
					select {
					case client.Send <- msg.Payload:
						metrics.MessagesDelivered.WithLabelValues(a.matchID).Inc()
					default:
						// Client buffer full, drop and remove client safely
						close(client.Send)
						delete(a.clients, client)
						metrics.ActiveClients.Dec()
					}
				}
				if len(a.clients) == 0 {
					idleTimer.Reset(IdleTimeout)
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
			// Push Redis payload into actor mailbox; stop if the actor is gone.
			if !a.Send(ActorMessage{
				Type:    BroadcastUpdate,
				Payload: []byte(msg.Payload),
			}) {
				return
			}
		}
	}
}
