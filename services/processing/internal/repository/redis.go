package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mildlybrutal/overcast/internal/models"
	"github.com/redis/go-redis/v9"
)

type RedisRepo struct {
	client *redis.Client
}

func NewRedisRepo(addr string) (*RedisRepo, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	return &RedisRepo{client: rdb}, nil
}

func (r *RedisRepo) CacheLiveMatch(ctx context.Context, match models.MatchData) error {
	payload, err := json.Marshal(match)
	if err != nil {
		return fmt.Errorf("cache live match failed: %w", err)
	}

	key := fmt.Sprintf("match:live:%s", match.ID)

	return r.client.Set(ctx, key, payload, 10*time.Minute).Err()
}

func (r *RedisRepo) Close() error {
	return r.client.Close()
}
