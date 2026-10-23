package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type ScoreItem struct {
	Inning  string  `json:"inning"`
	Runs    int     `json:"r"`
	Overs   float64 `json:"o"`
	Wickets int     `json:"w"`
}

type SeedMatch struct {
	ID        string      `json:"id"`
	Name      string      `json:"name"`
	Status    string      `json:"status"`
	MatchType string      `json:"match_type"`
	Venue     string      `json:"venue"`
	Score     []ScoreItem `json:"score"`
}

func main() {
	pgConn := "postgres://cricket_admin:cricket_password@localhost:5432/cricket_db?sslmode=disable"
	redisAddr := "localhost:6379"

	db, err := sql.Open("postgres", pgConn)
	if err != nil {
		log.Fatalf("Postgres conn error: %v", err)
	}
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	ctx := context.Background()

	log.Println("🌱 Seeding 50 test match records into Postgres and Redis...")

	for i := 1; i <= 50; i++ {
		matchID := fmt.Sprintf("test-seed-match-%03d", i)
		match := SeedMatch{
			ID:        matchID,
			Name:      fmt.Sprintf("Team Alpha vs Team Beta, Match %d", i),
			Status:    "In Progress",
			MatchType: "t20",
			Venue:     "Overcast International Stadium",
			Score: []ScoreItem{
				{Inning: "Team Alpha Inning 1", Runs: 150 + i, Overs: 19.4, Wickets: 5},
			},
		}

		scoreJSON, err := json.Marshal(match.Score)
		if err != nil {
			log.Fatalf("Marshal score error: %v", err)
		}

		// Insert into Postgres
		_, err = db.ExecContext(ctx, `
			INSERT INTO matches (id, name, status, match_type, venue, score_json, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, NOW())
			ON CONFLICT (id) DO UPDATE SET
				status = EXCLUDED.status,
				score_json = EXCLUDED.score_json,
				updated_at = NOW();
		`, match.ID, match.Name, match.Status, match.MatchType, match.Venue, scoreJSON)
		if err != nil {
			log.Fatalf("Postgres insert error for %s: %v", matchID, err)
		}

		// Insert into Redis
		payload, err := json.Marshal(match)
		if err != nil {
			log.Fatalf("Marshal payload error: %v", err)
		}

		redisKey := fmt.Sprintf("match:live:%s", matchID)
		if err := rdb.Set(ctx, redisKey, payload, 0).Err(); err != nil {
			log.Fatalf("Redis set error for %s: %v", matchID, err)
		}
	}

	log.Println("✅ Successfully seeded 50 match records into Postgres and Redis!")
}
