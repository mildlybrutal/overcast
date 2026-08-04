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

func (s *Supervisor) RouteClientJoin(matchID string, client *Client) {
	s.mu.Lock()
	act, exists := s.actors[matchID]
	if !exists {
		// Spawn Match Actor dynamically on-demand
		act = NewMatchActor(matchID, s.rdb, s.removeActor)
		s.actors[matchID] = act
		go act.Start(s.ctx)
	}
	s.mu.Unlock()

	act.Send(ActorMessage{
		Type:   ClientJoin,
		Client: client,
	})
}

func (s *Supervisor) RouteClientLeave(matchID string, client *Client) {
	s.mu.RLock()
	act, exists := s.actors[matchID]
	s.mu.RUnlock()

	if exists {
		act.Send(ActorMessage{
			Type:   ClientLeave,
			Client: client,
		})
	}
}

func (s *Supervisor) removeActor(matchID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.actors, matchID)
}
