package handler

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/mildlybrutal/overcast/services/api/internal/metrics"
	"github.com/mildlybrutal/overcast/services/api/internal/repository"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.Use(func(c *gin.Context) {
		start := time.Now()
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
		if c.FullPath() != "" {
			duration := time.Since(start).Seconds()
			metrics.HTTPRequestsTotal.WithLabelValues(c.FullPath(), strconv.Itoa(c.Writer.Status())).Inc()
			metrics.HTTPRequestDuration.WithLabelValues(c.FullPath(), strconv.Itoa(c.Writer.Status())).Observe(duration)
		}
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
			metrics.CacheHits.Inc()
			c.JSON(http.StatusOK, gin.H{
				"status": "success",
				"source": "cache",
				"data":   cachedJSON,
			})
			return
		}
	}

	metrics.CacheMisses.Inc()
	match, err := h.repo.GetMatchFromDB(c.Request.Context(), matchID)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch match from db"})
		return
	}

	if match == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Match not found"})

		return
	}

	_ = h.repo.SetLiveMatchInCache(c.Request.Context(), match)

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"source": "database",
		"data":   match,
	})

}
