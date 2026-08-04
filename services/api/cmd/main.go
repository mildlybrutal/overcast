package main

import (
	"log"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/mildlybrutal/overcast/services/api/internal/handler"
	"github.com/mildlybrutal/overcast/services/api/internal/repository"
)

func main() {
	postgresConn := os.Getenv("POSTGRES_CONN")
	if postgresConn == "" {
		postgresConn = "postgres://cricket_admin:cricket_password@localhost:5432/cricket_db?sslmode=disable"
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	repo, err := repository.NewAPIRepository(postgresConn, redisAddr)
	if err != nil {
		log.Fatalf("Failed to connect to database/cache: %v", err)
	}
	defer repo.Close()

	router := gin.Default()
	apiHandler := handler.NewAPIHandler(repo)
	apiHandler.RegisterRoutes(router)

	log.Printf("Query API Service starting on port %s...", port)
	if err := router.Run(":" + port); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
