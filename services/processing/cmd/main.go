package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mildlybrutal/overcast/services/processing/internal/consumer"
	"github.com/mildlybrutal/overcast/services/processing/internal/repository"
)

func main() {
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	topic := os.Getenv("KAFKA_TOPIC")
	groupID := os.Getenv("KAFKA_GROUP_ID")
	pgConn := os.Getenv("POSTGRES_CONN")
	redisAddr := os.Getenv("REDIS_ADDR")

	pgRepo, err := repository.NewPostgresRepo(pgConn)

	if err != nil {
		log.Fatalf("DB connection initialization failed: %v", err)
	}

	defer pgRepo.Close()

	redisRepo, err := repository.NewRedisRepo(redisAddr)

	if err != nil {
		log.Fatalf("Redis Connection initialization failed: %v", err)
	}

	defer redisRepo.Close()

	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	worker := consumer.NewProcessingWorker(kafkaBroker, topic, groupID, pgRepo, redisRepo)

	go func() {
		<-sigChan
		log.Println("Termination signal recieved. Exiting...")
		cancel()
	}()

	worker.Start(ctx)
}
