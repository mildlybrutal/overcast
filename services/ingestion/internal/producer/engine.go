package producer

import (
	"context"
	"log"
	"time"

	cricketv1 "github.com/mildlybrutal/overcast/gen/cricket/v1"
	"github.com/mildlybrutal/overcast/services/ingestion/internal/api"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type IngestionEngine struct {
	client *api.CricAPIClient
	writer *kafka.Writer
}

func NewIngestionEngine(apiKey string, kafkaBroker string, topic string) *IngestionEngine {
	return &IngestionEngine{
		client: api.NewCricAPIClient(apiKey),
		writer: &kafka.Writer{
			Addr:     kafka.TCP(kafkaBroker),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (ie *IngestionEngine) Start(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)

	defer ticker.Stop()

	defer ie.writer.Close()

	for {
		select {
		case <-ctx.Done():
			log.Println("Gracefully terminating ingestion client operations...")
			return
		case <-ticker.C:
			matches, err := ie.client.FetchLiveMatches()
			if err != nil {
				log.Printf("Error fetching matches: %v", err)
				continue
			}

			for _, match := range matches {
				scoresProto := make([]*cricketv1.MatchScore, len(match.Score))
				for i, s := range match.Score {
					scoresProto[i] = &cricketv1.MatchScore{
						R:      int32(s.R),
						W:      int32(s.W),
						O:      s.O,
						Inning: s.Inning,
					}
				}

				matchProto := &cricketv1.MatchData{
					Id:             match.ID,
					Name:           match.Name,
					Status:         match.Status,
					MatchType:      match.MatchType,
					Venue:          match.Venue,
					Date:           match.Date,
					DateTimeGmt:    match.DateTimeGMT,
					Teams:          match.Teams,
					Score:          scoresProto,
					SeriesId:       match.SeriesID,
					FantasyEnabled: match.FantasyEnabled,
				}
				payload, err := proto.Marshal(matchProto)
				if err != nil {
					log.Printf("Failed encoding match %s: %v", match.ID, err)
					continue
				}
				err = ie.writer.WriteMessages(ctx, kafka.Message{
					Key:   []byte(match.ID),
					Value: payload,
				})

				if err != nil {
					log.Printf("Failed pushing match %s to kafka: %v", match.ID, err)
				}
			}
		}
	}
}
