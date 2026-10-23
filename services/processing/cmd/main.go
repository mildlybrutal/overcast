package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/mildlybrutal/overcast/services/processing/internal/consumer"
	"github.com/mildlybrutal/overcast/services/processing/internal/repository"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	topic := os.Getenv("KAFKA_TOPIC")
	groupID := os.Getenv("KAFKA_GROUP_ID")
	pgConn := os.Getenv("POSTGRES_CONN")
	redisAddr := os.Getenv("REDIS_ADDR")

	// kafka-go panics on an empty topic; fail with a clear message instead.
	for name, val := range map[string]string{
		"KAFKA_BROKER": kafkaBroker, "KAFKA_TOPIC": topic, "KAFKA_GROUP_ID": groupID,
		"POSTGRES_CONN": pgConn, "REDIS_ADDR": redisAddr,
	} {
		if val == "" {
			log.Fatalf("missing required environment variable %s", name)
		}
	}

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

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Processing service metrics server listening on :8082...")
		if err := http.ListenAndServe(":8082", nil); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()

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
