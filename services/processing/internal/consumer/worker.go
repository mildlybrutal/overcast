package consumer

import (
	"context"
	"log"
	"time"

	cricketv1 "github.com/mildlybrutal/overcast/gen/cricket/v1"
	"github.com/mildlybrutal/overcast/internal/models"
	"github.com/mildlybrutal/overcast/services/processing/internal/repository"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// Retry backoff for Postgres writes. Variables so tests can shorten them.
var (
	initialBackoff = 200 * time.Millisecond
	maxBackoff     = 10 * time.Second
)

type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// matchStore is the source of truth. Writes must be idempotent because a
// message can be processed more than once (at-least-once delivery).
type matchStore interface {
	UpsertMatch(ctx context.Context, match models.MatchData) error
}

// matchCache is derived state: failures are logged, never block the stream.
type matchCache interface {
	CacheLiveMatch(ctx context.Context, match models.MatchData) error
	PublishMatchUpdate(ctx context.Context, match models.MatchData) error
}

type ProcessingWorker struct {
	reader messageReader
	pg     matchStore
	redis  matchCache
}

func NewProcessingWorker(broker string, topic string, groupID string, pg *repository.PostgresRepo, rdb *repository.RedisRepo) *ProcessingWorker {
	return newWorker(kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10,
		MaxBytes: 10e6,
	}), pg, rdb)
}

func newWorker(reader messageReader, pg matchStore, cache matchCache) *ProcessingWorker {
	return &ProcessingWorker{reader: reader, pg: pg, redis: cache}
}

// Start consumes with at-least-once semantics: a message's offset is committed
// only after it has been written to Postgres. If the process dies before the
// commit, the message is redelivered and the idempotent upsert absorbs it.
func (pw *ProcessingWorker) Start(ctx context.Context) {
	defer pw.reader.Close()
	log.Printf("Processing Worker listening for incoming match events on Kafka...")

	for {
		// FetchMessage, unlike ReadMessage, does not auto-commit the offset.
		msg, err := pw.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("Safely winding down worker")
				return
			}
			log.Printf("Fetch error: %v", err)
			continue
		}

		if !pw.process(ctx, msg) {
			return // shutting down mid-retry: leave the offset uncommitted
		}

		if err := pw.reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return
			}
			// Not fatal: the message will be redelivered and re-applied.
			log.Printf("Commit error at offset %d: %v", msg.Offset, err)
		}
	}
}

// process handles one message and reports whether its offset may be
// committed. It returns false only when ctx is cancelled before the
// Postgres write succeeded.
func (pw *ProcessingWorker) process(ctx context.Context, msg kafka.Message) bool {
	var matchEvent cricketv1.MatchData
	if err := proto.Unmarshal(msg.Value, &matchEvent); err != nil {
		// A malformed payload will never succeed; retrying would block the
		// partition forever. Skip it (a dead-letter topic would keep it).
		log.Printf("Skipping undecodable message at offset %d: %v", msg.Offset, err)
		return true
	}

	match := models.MatchDataFromProto(&matchEvent)

	// Postgres first: the cache and live push must never show data the source
	// of truth rejected. Retry until it succeeds; skipping would lose the event.
	backoff := initialBackoff
	for {
		err := pw.pg.UpsertMatch(ctx, match)
		if err == nil {
			break
		}
		log.Printf("Postgres write error for match %s (retrying in %v): %v", match.ID, backoff, err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}

	if err := pw.redis.CacheLiveMatch(ctx, match); err != nil {
		log.Printf("Redis cache error for match %s: %v", match.ID, err)
	}
	if err := pw.redis.PublishMatchUpdate(ctx, match); err != nil {
		log.Printf("Redis publish error for match %s: %v", match.ID, err)
	}

	log.Printf("Processed match event: %s (%s)", match.Name, match.ID)
	return true
}
