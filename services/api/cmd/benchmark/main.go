package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

const (
	pgConn     = "postgres://cricket_admin:cricket_password@localhost:5432/cricket_db?sslmode=disable"
	redisAddr  = "localhost:6379"
	apiBase    = "http://localhost:8080"
	promBase   = "http://localhost:9090"
	numMatches = 50
)

type ScoreItem struct {
	Inning  string  `json:"inning"`
	Runs    int     `json:"r"`
	Overs   float64 `json:"o"`
	Wickets int     `json:"w"`
}

type SeedMatch struct {
	ID        string      `json:"id"`
	Name      string      `json:"name"`
	Status    string      `json:"status"`
	MatchType string      `json:"match_type"`
	Venue     string      `json:"venue"`
	Score     []ScoreItem `json:"score"`
}

type LatencyResult struct {
	Durations []time.Duration
	P50       time.Duration
	P95       time.Duration
	P99       time.Duration
	Mean      time.Duration
	TotalReqs int
	Duration  time.Duration
}

func seedMatches(db *sql.DB, rdb *redis.Client, seedRedis bool) []string {
	ctx := context.Background()
	matchIDs := make([]string, numMatches)

	for i := 1; i <= numMatches; i++ {
		mID := fmt.Sprintf("test-seed-match-%03d", i)
		matchIDs[i-1] = mID

		match := SeedMatch{
			ID:        mID,
			Name:      fmt.Sprintf("Team Alpha vs Team Beta, Match %d", i),
			Status:    "In Progress",
			MatchType: "t20",
			Venue:     "Overcast International Stadium",
			Score: []ScoreItem{
				{Inning: "Team Alpha Inning 1", Runs: 140 + i, Overs: 19.2, Wickets: 4},
			},
		}

		scoreJSON, _ := json.Marshal(match.Score)
		_, err := db.ExecContext(ctx, `
			INSERT INTO matches (id, name, status, match_type, venue, score_json, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, NOW())
			ON CONFLICT (id) DO UPDATE SET
				status = EXCLUDED.status,
				score_json = EXCLUDED.score_json,
				updated_at = NOW();
		`, match.ID, match.Name, match.Status, match.MatchType, match.Venue, scoreJSON)
		if err != nil {
			log.Fatalf("DB seed error: %v", err)
		}

		if seedRedis {
			payload, _ := json.Marshal(match)
			rdb.Set(ctx, "match:live:"+mID, payload, 0)
		}
	}
	return matchIDs
}

func flushRedisSeededKeys(rdb *redis.Client, matchIDs []string) {
	ctx := context.Background()
	for _, id := range matchIDs {
		rdb.Del(ctx, "match:live:"+id)
	}
}

func runLoadTest(matchIDs []string, totalReqs int, concurrency int, invalidateCache bool, rdb *redis.Client) LatencyResult {
	httpClient := &http.Client{Timeout: 5 * time.Second}
	durations := make([]time.Duration, totalReqs)

	reqChan := make(chan int, totalReqs)
	for i := 0; i < totalReqs; i++ {
		reqChan <- i
	}
	close(reqChan)

	var wg sync.WaitGroup
	startTime := time.Now()

	// Invalidator goroutine if cold test requiring continuous DB fallback
	ctx, cancel := context.WithCancel(context.Background())
	if invalidateCache {
		go func() {
			ticker := time.NewTicker(2 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					flushRedisSeededKeys(rdb, matchIDs)
				}
			}
		}()
	}

	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range reqChan {
				mID := matchIDs[rand.Intn(len(matchIDs))]
				url := fmt.Sprintf("%s/api/v1/matches/%s", apiBase, mID)

				t0 := time.Now()
				resp, err := httpClient.Get(url)
				dur := time.Since(t0)

				if err == nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
				durations[idx] = dur
			}
		}()
	}

	wg.Wait()
	cancel()
	totalDuration := time.Since(startTime)

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	var sum time.Duration
	for _, d := range durations {
		sum += d
	}

	p50 := durations[int(float64(totalReqs)*0.50)]
	p95 := durations[int(float64(totalReqs)*0.95)]
	p99 := durations[int(float64(totalReqs)*0.99)]
	mean := sum / time.Duration(totalReqs)

	return LatencyResult{
		Durations: durations,
		P50:       p50,
		P95:       p95,
		P99:       p99,
		Mean:      mean,
		TotalReqs: totalReqs,
		Duration:  totalDuration,
	}
}

func runMixedLoadTest(matchIDs []string, totalReqs int, concurrency int) LatencyResult {
	httpClient := &http.Client{Timeout: 5 * time.Second}
	durations := make([]time.Duration, totalReqs)

	reqChan := make(chan int, totalReqs)
	for i := 0; i < totalReqs; i++ {
		reqChan <- i
	}
	close(reqChan)

	var wg sync.WaitGroup
	startTime := time.Now()

	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range reqChan {
				var mID string
				// 70% cached valid match ID, 30% non-existent ID
				if rand.Float32() < 0.70 {
					mID = matchIDs[rand.Intn(len(matchIDs))]
				} else {
					mID = fmt.Sprintf("non-existent-match-%d", rand.Intn(1000))
				}
				url := fmt.Sprintf("%s/api/v1/matches/%s", apiBase, mID)

				t0 := time.Now()
				resp, err := httpClient.Get(url)
				dur := time.Since(t0)

				if err == nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
				durations[idx] = dur
			}
		}()
	}

	wg.Wait()
	totalDuration := time.Since(startTime)

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	var sum time.Duration
	for _, d := range durations {
		sum += d
	}

	p50 := durations[int(float64(totalReqs)*0.50)]
	p95 := durations[int(float64(totalReqs)*0.95)]
	p99 := durations[int(float64(totalReqs)*0.99)]
	mean := sum / time.Duration(totalReqs)

	return LatencyResult{
		Durations: durations,
		P50:       p50,
		P95:       p95,
		P99:       p99,
		Mean:      mean,
		TotalReqs: totalReqs,
		Duration:  totalDuration,
	}
}

type PromQueryResult struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Value []interface{} `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

func queryPrometheusMetric(query string) string {
	url := fmt.Sprintf("%s/api/v1/query?query=%s", promBase, query)
	resp, err := http.Get(url)
	if err != nil {
		return "N/A"
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var res PromQueryResult
	if err := json.Unmarshal(body, &res); err != nil || len(res.Data.Result) == 0 {
		return "0"
	}
	if len(res.Data.Result[0].Value) > 1 {
		return fmt.Sprintf("%v", res.Data.Result[0].Value[1])
	}
	return "0"
}

func main() {
	log.Println("==========================================================================")
	log.Println("🚀 OVERCAST PLATFORM: EMPIRICAL COLD vs WARM LOAD TEST BENCHMARK RUNNER")
	log.Println("==========================================================================")

	db, err := sql.Open("postgres", pgConn)
	if err != nil {
		log.Fatalf("Postgres connection error: %v", err)
	}
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	// Step 1: Seed 50 Match Records
	log.Println("\n[1/5] Seeding 50 match records into PostgreSQL and Redis...")
	matchIDs := seedMatches(db, rdb, false)
	log.Printf("✔ Seeded %d matches into database successfully.", len(matchIDs))

	// Step 2: COLD PATH TEST (DB Fallback)
	log.Println("\n[2/5] Running COLD PATH TEST (Guaranteed DB Fallback Read)...")
	log.Println("  Profile: 20 Concurrent Workers | 500 Total Requests | 0% Cache Hits")
	flushRedisSeededKeys(rdb, matchIDs)
	coldResult := runLoadTest(matchIDs, 500, 20, true, rdb)
	log.Printf("✔ Cold Path Test Finished in %v", coldResult.Duration)
	log.Printf("  p50: %.2f ms | p95: %.2f ms | p99: %.2f ms | Mean: %.2f ms",
		float64(coldResult.P50.Microseconds())/1000.0,
		float64(coldResult.P95.Microseconds())/1000.0,
		float64(coldResult.P99.Microseconds())/1000.0,
		float64(coldResult.Mean.Microseconds())/1000.0)

	// Step 3: WARM PATH TEST (Redis Cache Hit)
	log.Println("\n[3/5] Running WARM PATH TEST (Guaranteed Redis In-Memory Read)...")
	log.Println("  Re-seeding Redis & performing warm-up pass...")
	seedMatches(db, rdb, true)

	// Warm-up pass
	for _, id := range matchIDs {
		resp, err := http.Get(fmt.Sprintf("%s/api/v1/matches/%s", apiBase, id))
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}
	log.Println("  Profile: 20 Concurrent Workers | 500 Total Requests | 100% Cache Hits")
	warmResult := runLoadTest(matchIDs, 500, 20, false, rdb)
	log.Printf("✔ Warm Path Test Finished in %v", warmResult.Duration)
	log.Printf("  p50: %.2f ms | p95: %.2f ms | p99: %.2f ms | Mean: %.2f ms",
		float64(warmResult.P50.Microseconds())/1000.0,
		float64(warmResult.P95.Microseconds())/1000.0,
		float64(warmResult.P99.Microseconds())/1000.0,
		float64(warmResult.Mean.Microseconds())/1000.0)

	// Step 4: MIXED CACHE HIT RATIO TEST (Statistically Significant Sample)
	log.Println("\n[4/5] Running MIXED CACHE HIT RATIO TEST (Statistical Accumulation)...")
	log.Println("  Profile: 20 Concurrent Workers | 2000 Total Requests (70% Cached / 30% Uncached)")
	mixedResult := runMixedLoadTest(matchIDs, 2000, 20)
	log.Printf("✔ Mixed Load Test Finished in %v", mixedResult.Duration)

	// Sleep 5 seconds for Prometheus scrape
	log.Println("\n[5/5] Waiting 15s for Prometheus scrape cycle to finalize TSDB metrics...")
	time.Sleep(15 * time.Second)

	hitsStr := queryPrometheusMetric("overcast_api_cache_hits_total")
	missesStr := queryPrometheusMetric("overcast_api_cache_misses_total")

	var hits, misses float64
	fmt.Sscanf(hitsStr, "%f", &hits)
	fmt.Sscanf(missesStr, "%f", &misses)

	hitRatio := 0.0
	if (hits + misses) > 0 {
		hitRatio = (hits / (hits + misses)) * 100.0
	}

	log.Println("\n==========================================================================")
	log.Println("📊 EMPIRICAL BENCHMARK SUMMARY & PROMETHEUS TELEMETRY REPORT")
	log.Println("==========================================================================")
	fmt.Printf("Concurrency Level:            20 Workers\n")
	fmt.Printf("Cold Test Requests:           500 Requests\n")
	fmt.Printf("Warm Test Requests:           500 Requests\n")
	fmt.Printf("Mixed Test Requests:          2000 Requests\n")
	fmt.Printf("Total Test Duration:          %.2f seconds\n\n", (coldResult.Duration + warmResult.Duration + mixedResult.Duration).Seconds())

	fmt.Printf("--- LATENCY COMPARISON (COLD DB vs WARM REDIS) ---\n")
	fmt.Printf("Metric     | Cold Path (DB Fallback) | Warm Path (Redis Cache) | Improvement\n")
	fmt.Printf("-----------------------------------------------------------------------------\n")
	fmt.Printf("p50        | %8.2f ms             | %8.2f ms             | %5.1f%%\n",
		float64(coldResult.P50.Microseconds())/1000.0,
		float64(warmResult.P50.Microseconds())/1000.0,
		(1.0-float64(warmResult.P50)/float64(coldResult.P50))*100.0)

	fmt.Printf("p95        | %8.2f ms             | %8.2f ms             | %5.1f%%\n",
		float64(coldResult.P95.Microseconds())/1000.0,
		float64(warmResult.P95.Microseconds())/1000.0,
		(1.0-float64(warmResult.P95)/float64(coldResult.P95))*100.0)

	fmt.Printf("p99        | %8.2f ms             | %8.2f ms             | %5.1f%%\n",
		float64(coldResult.P99.Microseconds())/1000.0,
		float64(warmResult.P99.Microseconds())/1000.0,
		(1.0-float64(warmResult.P99)/float64(coldResult.P99))*100.0)

	fmt.Printf("Mean       | %8.2f ms             | %8.2f ms             | %5.1f%%\n\n",
		float64(coldResult.Mean.Microseconds())/1000.0,
		float64(warmResult.Mean.Microseconds())/1000.0,
		(1.0-float64(warmResult.Mean)/float64(coldResult.Mean))*100.0)

	fmt.Printf("--- PROMETHEUS ACCUMULATED TELEMETRY ---\n")
	fmt.Printf("Prometheus Cache Hits:        %.0f\n", hits)
	fmt.Printf("Prometheus Cache Misses:      %.0f\n", misses)
	fmt.Printf("Accumulated Cache Hit Ratio:  %.2f%%\n", hitRatio)
	log.Println("==========================================================================")
}
