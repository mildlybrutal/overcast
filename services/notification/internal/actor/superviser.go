package actor

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"
)

type Supervisor struct {
	actors map[string]*MatchActor
	rdb    *redis.Client
	mu     sync.RWMutex
	ctx    context.Context
}

func NewSupervisor(ctx context.Context, rdb *redis.Client) *Supervisor {
	return &Supervisor{
		actors: make(map[string]*MatchActor),
		rdb:    rdb,
		ctx:    ctx,
	}
}

// RouteClientJoin hands client to the match's actor, spawning one if needed.
// If the actor it finds is shutting down (idle timeout raced with this join),
// the dead actor is dropped and a fresh one is spawned.
func (s *Supervisor) RouteClientJoin(matchID string, client *Client) {
	for {
		if s.ctx.Err() != nil {
			// Shutting down: no actor will ever own this client.
			close(client.Send)
			return
		}

		act := s.getOrSpawn(matchID)
		if act.Send(ActorMessage{Type: ClientJoin, Client: client}) {
			return
		}
		s.removeActor(act)
	}
}

func (s *Supervisor) getOrSpawn(matchID string) *MatchActor {
	s.mu.Lock()
	defer s.mu.Unlock()

	act, exists := s.actors[matchID]
	if !exists {
		// Spawn Match Actor dynamically on-demand
		act = NewMatchActor(matchID, s.rdb, s.removeActor)
		s.actors[matchID] = act
		go act.Start(s.ctx)
	}
	return act
}

func (s *Supervisor) RouteClientLeave(matchID string, client *Client) {
	s.mu.RLock()
	act, exists := s.actors[matchID]
	s.mu.RUnlock()

	if exists {
		// If the actor already stopped it has closed client.Send itself.
		act.Send(ActorMessage{
			Type:   ClientLeave,
			Client: client,
		})
	}
}

// removeActor forgets act, but only if it is still the registered actor for
// its match; a replacement spawned in the meantime must not be removed.
func (s *Supervisor) removeActor(act *MatchActor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.actors[act.matchID] == act {
		delete(s.actors, act.matchID)
	}
}

func (s *Supervisor) actorCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.actors)
}
