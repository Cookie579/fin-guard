package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

//go:embed scripts/acquire.lua
var acquireLua string

//go:embed scripts/report.lua
var reportLua string

type APIConfig struct {
	Name        string
	Host        string
	RatePerSec  float64
	Burst       int
	MaxInflight int
	Keys        []string
}

type Server struct {
	rdb      *redis.Client
	apis     map[string]*APIConfig
	acqSHA   string
	repSHA   string
	rrKeyFmt string // "rl:%s:rr"
	keyFmt   string // "rl:%s:%d"
}

type acquireReq struct {
	API    string `json:"api"`
	Weight int    `json:"weight"`
}
type acquireResp struct {
	LeaseID string `json:"lease_id"`
	Host    string `json:"host"`
	Key     string `json:"key"`
	WaitMs  int    `json:"wait_ms"`
}

type reportReq struct {
	LeaseID      string `json:"lease_id"`
	OK           bool   `json:"ok"`
	Status       int    `json:"status,omitempty"`
	RetryAfterMs int    `json:"retry_after_ms,omitempty"`
}

// --- retrying ping so the broker never crashes on startup if Redis isn't ready ---
func waitForRedis(ctx context.Context, rdb *redis.Client) {
	backoff := 1 * time.Second
	const maxBackoff = 15 * time.Second
	for {
		if err := rdb.Ping(ctx).Err(); err == nil {
			log.Println("connected to Redis")
			return
		}
		log.Printf("waiting for Redis... next try in %v", backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			log.Fatalf("exiting while waiting for Redis: %v", ctx.Err())
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func newServer() *Server {
	// Load file-backed config (no envs)
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("load config.json: %v", err)
	}

	// Redis client (addr should be "127.0.0.1:6379" for sidecar or a real endpoint)
	rdb := redis.NewClient(&redis.Options{
		Addr: GetRedisAddr(cfg),
		DB:   0,
	})
	// Wait (with backoff) until Redis responds
	waitForRedis(context.Background(), rdb)

	// Stocktwits rate/host/keys from config.json
	host := GetStocktwitsHost(cfg)                // default "stocktwits.p.rapidapi.com"
	rate, burst, maxInf := GetStocktwitsRate(cfg) // defaults 0.2777778, 10, 1
	keys, err := LoadRapidAPIKeys()               // required: config.json.stocktwits_keys
	if err != nil || len(keys) == 0 {
		log.Fatalf("rapidapi keys: %v", err)
	}

	apiCfg := &APIConfig{
		Name:        "stocktwits",
		Host:        host,
		RatePerSec:  rate,
		Burst:       burst,
		MaxInflight: maxInf,
		Keys:        keys,
	}

	// Load Lua scripts
	ctx := context.Background()
	acqSHA, err := rdb.ScriptLoad(ctx, acquireLua).Result()
	if err != nil {
		log.Fatalf("load acquire script: %v", err)
	}
	repSHA, err := rdb.ScriptLoad(ctx, reportLua).Result()
	if err != nil {
		log.Fatalf("load report script: %v", err)
	}

	return &Server{
		rdb:      rdb,
		apis:     map[string]*APIConfig{"stocktwits": apiCfg},
		acqSHA:   acqSHA,
		repSHA:   repSHA,
		rrKeyFmt: "rl:%s:rr",
		keyFmt:   "rl:%s:%d",
	}
}

func (s *Server) apiOrErr(name string) (*APIConfig, error) {
	cfg := s.apis[strings.ToLower(name)]
	if cfg == nil {
		return nil, fmt.Errorf("unknown api %q", name)
	}
	return cfg, nil
}

func (s *Server) acquire(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req acquireReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	cfg, err := s.apiOrErr(req.API)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	n := len(cfg.Keys)
	now := time.Now().Unix()
	start := 0
	// Round-robin starting point
	if n > 1 {
		rrKey := fmt.Sprintf(s.rrKeyFmt, cfg.Name)
		rr, _ := s.rdb.Incr(ctx, rrKey).Result()
		start = int(rr % int64(n))
	}

	var bestWait int64 = 1<<31 - 1
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		keyHash := fmt.Sprintf(s.keyFmt, cfg.Name, idx)
		res, err := s.rdb.EvalSha(ctx, s.acqSHA, []string{keyHash},
			now, cfg.RatePerSec, cfg.Burst, cfg.MaxInflight).Result()
		if err != nil {
			http.Error(w, "redis error: "+err.Error(), 500)
			return
		}
		arr, ok := res.([]interface{})
		if !ok || len(arr) != 2 {
			http.Error(w, "redis bad result", 500)
			return
		}
		okFlag := int(arr[0].(int64))
		waitSec := int64(arr[1].(int64))
		if okFlag == 1 {
			leaseID := uuid.NewString()
			leaseKey := "lease:" + leaseID
			pipe := s.rdb.TxPipeline()
			pipe.HSet(ctx, leaseKey, "api", cfg.Name, "idx", idx)
			pipe.Expire(ctx, leaseKey, 2*time.Minute)
			if _, err := pipe.Exec(ctx); err != nil {
				http.Error(w, "redis lease error: "+err.Error(), 500)
				return
			}
			writeJSON(w, acquireResp{
				LeaseID: leaseID,
				Host:    cfg.Host,
				Key:     cfg.Keys[idx],
				WaitMs:  0,
			})
			return
		}
		if waitSec < bestWait {
			bestWait = waitSec
		}
	}
	// no capacity now
	if bestWait < 0 {
		bestWait = 1
	}
	writeJSON(w, acquireResp{WaitMs: int(bestWait * 1000)})
}

func (s *Server) report(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req reportReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if req.LeaseID == "" {
		http.Error(w, "lease_id required", 400)
		return
	}
	leaseKey := "lease:" + req.LeaseID
	vals, err := s.rdb.HMGet(ctx, leaseKey, "api", "idx").Result()
	if err != nil {
		http.Error(w, "redis error: "+err.Error(), 500)
		return
	}
	if vals[0] == nil || vals[1] == nil {
		w.WriteHeader(204)
		return
	}
	api := vals[0].(string)
	idxStr := fmt.Sprintf("%v", vals[1])
	idx, convErr := strconv.Atoi(idxStr)
	if convErr != nil {
		w.WriteHeader(204)
		return
	}
	cfg, err := s.apiOrErr(api)
	if err != nil {
		w.WriteHeader(204)
		return
	}
	keyHash := fmt.Sprintf(s.keyFmt, cfg.Name, idx)
	now := time.Now().Unix()

	retrySec := 0
	if req.RetryAfterMs > 0 {
		retrySec = (req.RetryAfterMs + 999) / 1000
	}
	status := 0
	if !req.OK {
		status = req.Status
	}
	_, luaErr := s.rdb.EvalSha(ctx, s.repSHA, []string{keyHash}, now, status, retrySec).Result()
	if luaErr != nil && !errors.Is(luaErr, redis.Nil) {
		http.Error(w, "redis error: "+luaErr.Error(), 500)
		return
	}
	_ = s.rdb.Del(ctx, leaseKey).Err()
	w.WriteHeader(204)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func main() {
	s := newServer()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/acquire", s.acquire)
	mux.HandleFunc("/v1/report", s.report)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })

	const listenAddr = ":8080" // fixed listen address
	log.Printf("rate-broker listening on %s", listenAddr)
	if err := http.ListenAndServe(listenAddr, mux); err != nil {
		log.Fatal(err)
	}
}
