package models

import cricketv1 "github.com/mildlybrutal/overcast/gen/cricket/v1"

type MatchScore struct {
	R      int     `json:"r"`
	W      int     `json:"w"`
	O      float32 `json:"o"`
	Inning string  `json:"inning"`
}

type MatchData struct {
	ID             string       `json:"id"`
	Name           string       `json:"name"`
	Status         string       `json:"status"`
	MatchType      string       `json:"matchType"`
	Venue          string       `json:"venue"`
	Date           string       `json:"date"`
	DateTimeGMT    string       `json:"dateTimeGMT"`
	Teams          []string     `json:"teams"`
	Score          []MatchScore `json:"score"`
	SeriesID       string       `json:"series_id"`
	FantasyEnabled bool         `json:"fantasyEnabled"`
}

type CricketAPIResponse struct {
	Data   []MatchData `json:"data"`
	Status string      `json:"status"`
}

func (m MatchData) ToProto() *cricketv1.MatchData {
	scores := make([]*cricketv1.MatchScore, 0, len(m.Score))
	for _, score := range m.Score {
		scores = append(scores, &cricketv1.MatchScore{
			R:      int32(score.R),
			W:      int32(score.W),
			O:      score.O,
			Inning: score.Inning,
		})
	}

	return &cricketv1.MatchData{
		Id:             m.ID,
		Name:           m.Name,
		Status:         m.Status,
		MatchType:      m.MatchType,
		Venue:          m.Venue,
		Date:           m.Date,
		DateTimeGmt:    m.DateTimeGMT,
		Teams:          append([]string(nil), m.Teams...),
		Score:          scores,
		SeriesId:       m.SeriesID,
		FantasyEnabled: m.FantasyEnabled,
	}
}

func MatchDataFromProto(match *cricketv1.MatchData) MatchData {
	if match == nil {
		return MatchData{}
	}

	scores := make([]MatchScore, 0, len(match.GetScore()))
	for _, score := range match.GetScore() {
		scores = append(scores, MatchScore{
			R:      int(score.GetR()),
			W:      int(score.GetW()),
			O:      score.GetO(),
			Inning: score.GetInning(),
		})
	}

	return MatchData{
		ID:             match.GetId(),
		Name:           match.GetName(),
		Status:         match.GetStatus(),
		MatchType:      match.GetMatchType(),
		Venue:          match.GetVenue(),
		Date:           match.GetDate(),
		DateTimeGMT:    match.GetDateTimeGmt(),
		Teams:          append([]string(nil), match.GetTeams()...),
		Score:          scores,
		SeriesID:       match.GetSeriesId(),
		FantasyEnabled: match.GetFantasyEnabled(),
	}
}
