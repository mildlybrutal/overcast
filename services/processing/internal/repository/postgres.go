package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/mildlybrutal/overcast/internal/models"
)

type PostgresRepo struct {
	db *sql.DB
}

func NewPostgresRepo(connStr string) (*PostgresRepo, error) {
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return nil, fmt.Errorf("postgres connection error: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("postgres ping error: %w", err)
	}

	repo := &PostgresRepo{db: db}
	if err := repo.initSchema(); err != nil {
		return nil, fmt.Errorf("failed creating schema: %w", err)
	}

	return repo, nil
}

func (p *PostgresRepo) initSchema() error {
	query := `
		CREATE TABLE IF NOT EXISTS matches (
			id VARCHAR(100) PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			status VARCHAR(255),
			match_type VARCHAR(50),
			venue VARCHAR(255),
			score_json JSONB,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);`

	_, err := p.db.Exec(query)
	return err
}

// UpsertMatch is idempotent, so a redelivered Kafka message is harmless.
func (p *PostgresRepo) UpsertMatch(ctx context.Context, match models.MatchData) error {
	scoreJSON, err := json.Marshal(match.Score)
	if err != nil {
		return fmt.Errorf("failed marshaling score: %w", err)
	}

	query := `
		INSERT INTO matches (id, name, status, match_type, venue, score_json, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			score_json = EXCLUDED.score_json,
			updated_at = CURRENT_TIMESTAMP;
		`

	_, err = p.db.ExecContext(ctx, query, match.ID, match.Name, match.Status, match.MatchType, match.Venue, scoreJSON)
	return err
}

func (p *PostgresRepo) Close() error {
	return p.db.Close()
}
