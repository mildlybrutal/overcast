package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/mildlybrutal/overcast/internal/models"
)

type CricAPIClient struct {
	apiKey     string
	httpClient *http.Client
	endPoint   string
}

func NewCricAPIClient(apiKey string) *CricAPIClient {
	return &CricAPIClient{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		endPoint: "https://api.cricapi.com/v1/currentMatches",
	}
}

func (c *CricAPIClient) FetchLiveMatches() ([]models.MatchData, error) {
	url := fmt.Sprintf("%s?apikey=%s&offset=0", c.endPoint, c.apiKey)

	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("error fetching live matches: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received non-200 status code: %d", resp.StatusCode)
	}

	var apiResponse models.CricketAPIResponse

	if err := json.NewDecoder(resp.Body).Decode(&apiResponse); err != nil {
		return nil, fmt.Errorf("failed decoding response payload: %w", err)
	}

	if apiResponse.Status != "success" {
		return nil, fmt.Errorf("API response status reported failure")
	}

	return apiResponse.Data, nil
}
