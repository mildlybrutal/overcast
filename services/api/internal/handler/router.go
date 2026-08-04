package handler

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/mildlybrutal/overcast/services/api/internal/repository"
)

type APIHandler struct {
	repo *repository.APIRepository
}

func NewAPIHandler(repo *repository.APIRepository) *APIHandler {
	return &APIHandler{
		repo: repo,
	}
}

func (h *APIHandler) RegisterRoutes(r *gin.Engine) {
	r.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	v1 := r.Group("/api/v1")
	{
		v1.GET("/matches", h.ListMatches)
		v1.GET("/matches/:id", h.GetMatchDetails)
	}

}

func (h *APIHandler) ListMatches(c *gin.Context) {
	matches, err := h.repo.ListMatchesFromDB(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch matches"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"count":  len(matches),
		"data":   matches,
	})
}

func (h *APIHandler) GetMatchDetails(c *gin.Context) {
	matchID := c.Param("id")

	cachedData, err := h.repo.GetLiveMatchFromCache(c.Request.Context(), matchID)

	if err == nil && cachedData != "" {
		var cachedJSON map[string]interface{}
		if err := json.Unmarshal([]byte(cachedData), &cachedJSON); err == nil {
			c.JSON(http.StatusOK, gin.H{
				"status": "success",
				"source": "cache",
				"data":   cachedJSON,
			})
			return
		}
	}

	match, err := h.repo.GetMatchFromDB(c.Request.Context(), matchID)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch match from db"})
		return
	}

	if match == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Match not found"})

		return
	}
	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"source": "database",
		"data":   match,
	})

}
