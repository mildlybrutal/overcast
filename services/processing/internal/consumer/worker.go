package consumer

import (
	"context"
	"log"

	cricketv1 "github.com/mildlybrutal/overcast/gen/cricket/v1"
	"github.com/mildlybrutal/overcast/internal/models"
	"github.com/mildlybrutal/overcast/services/processing/internal/repository"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type ProcessingWorker struct {
	reader *kafka.Reader
	pg     *repository.PostgresRepo
	redis  *repository.RedisRepo
}

func NewProcessingWorker(broker string, topic string, groupID string, pg *repository.PostgresRepo, rdb *repository.RedisRepo) *ProcessingWorker {
	return &ProcessingWorker{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{broker},
			Topic:    topic,
			GroupID:  groupID,
			MinBytes: 10,
			MaxBytes: 10e6,
		}),
		pg:    pg,
		redis: rdb,
	}
}

func (pw *ProcessingWorker) Start(ctx context.Context) {
	defer pw.reader.Close()
	log.Printf("Processing Worker listening for incoming match events on Kafka...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Safely winding down worker")
			return
		default:
			msg, err := pw.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Read error: %v", err)
				continue
			}

			var matchEvent cricketv1.MatchData
			if err := proto.Unmarshal(msg.Value, &matchEvent); err != nil {
				log.Printf("Failed deserializing payload: %v", err)
				continue
			}

			match := models.MatchDataFromProto(&matchEvent)

			if err := pw.redis.CacheLiveMatch(ctx, match); err != nil {
				log.Printf("Redis Write Error for match %s: %v", match.ID, err)
			}

			if err := pw.pg.UpsertMatch(match); err != nil {
				log.Printf("Postgres Write Error for match %s: %v", match.ID, err)
			}

			log.Printf("Processed match event: %s (%s)", match.Name, match.ID)
		}
	}
}
