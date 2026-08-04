package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mildlybrutal/overcast/services/ingestion/internal/producer"
)

func main() {
	apiKey := os.Getenv("CRICKET_API_KEY")

	if apiKey == "" {
		log.Fatal("Cricket API is not available")
	}

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	topic := os.Getenv("KAFKA_TOPIC")

	if topic == "" {
		topic = "raw-match-events"
	}

	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	engine := producer.NewIngestionEngine(apiKey, kafkaBroker, topic)

	go func() {
		<-sigChan
		log.Println("Termination stopped")

		cancel()
	}()

	engine.Start(ctx)
}
