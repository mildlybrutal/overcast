package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type APIRepository struct {
	db  *sql.DB
	rdb *redis.Client
}

type MatchRecord struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Status    string          `json:"status"`
	MatchType string          `json:"match_type"`
	Venue     string          `json:"venue"`
	Score     json.RawMessage `json:"score"`
	UpdatedAt string          `json:"updated_at"`
}

func NewAPIRepository(postgresConn string, redisAddr string) (*APIRepository, error) {
	db, err := sql.Open("postgres", postgresConn)
	if err != nil {
		return nil, fmt.Errorf("postgres open error : %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("postgres ping error: %w", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		db.Close()
		rdb.Close()
		return nil, fmt.Errorf("redis ping error: %w", err)
	}
	return &APIRepository{db: db, rdb: rdb}, nil
}

// Read the active score from redis
func (r *APIRepository) GetLiveMatchFromCache(ctx context.Context, matchID string) (string, error) {
	key := fmt.Sprintf("match:live:%s", matchID)
	val, err := r.rdb.Get(ctx, key).Result()

	if errors.Is(err, redis.Nil) {
		return "", nil // cache miss
	} else if err != nil {
		return "", err
	}

	return val, nil
}

// cacheTTL matches the processing service's TTL, so a match that stops
// updating (finished, or never live) eventually drops out of the cache.
const cacheTTL = 10 * time.Minute

// Write fetched match record to redis cache
func (r *APIRepository) SetLiveMatchInCache(ctx context.Context, match *MatchRecord) error {
	payload, err := json.Marshal(match)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("match:live:%s", match.ID)
	return r.rdb.Set(ctx, key, payload, cacheTTL).Err()
}

// fetch single match record from postgres
func (r *APIRepository) GetMatchFromDB(ctx context.Context, matchID string) (*MatchRecord, error) {
	query := `SELECT id, name, status, match_type, venue, score_json, updated_at FROM matches WHERE id = $1`
	row := r.db.QueryRowContext(ctx, query, matchID)

	var match MatchRecord
	err := row.Scan(&match.ID, &match.Name, &match.Status, &match.MatchType, &match.Venue, &match.Score, &match.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	return &match, nil
}

//retrieve all historic and ongoing matches

func (r *APIRepository) ListMatchesFromDB(ctx context.Context) ([]MatchRecord, error) {
	query := `SELECT id, name, status, match_type, venue, score_json, updated_at FROM matches ORDER BY updated_at DESC LIMIT 50`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var matches []MatchRecord

	for rows.Next() {
		var match MatchRecord
		if err := rows.Scan(&match.ID, &match.Name, &match.Status, &match.MatchType, &match.Venue, &match.Score, &match.UpdatedAt); err != nil {
			return nil, err
		}
		matches = append(matches, match)
	}

	return matches, rows.Err()
}

func (r *APIRepository) Close() {
	r.db.Close()
	r.rdb.Close()
}
