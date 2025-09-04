package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
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

func mustEnv(name string) string {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		log.Fatalf("missing required env %s", name)
	}
	return v
}
func getEnv(name, def string) string {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	return v
}
func parseKeysJSON(env string) []string {
	raw := strings.TrimSpace(os.Getenv(env))
	if raw == "" {
		return nil
	}
	var keys []string
	if raw[0] == '[' {
		if err := json.Unmarshal([]byte(raw), &keys); err != nil {
			log.Fatalf("invalid %s JSON: %v", env, err)
		}
	} else {
		for _, p := range strings.Split(raw, ",") {
			if s := strings.TrimSpace(p); s != "" {
				keys = append(keys, s)
			}
		}
	}
	return keys
}

func newServer() *Server {
	// Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     mustEnv("REDIS_ADDR"), // "host:6379"
		Password: getEnv("REDIS_PASSWORD", ""),
		DB:       0,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}

	// STOCKTWITS config via env
	rate := 0.2777778
	if v := getEnv("STOCKTWITS_RATE_PER_SEC", ""); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			rate = f
		}
	}
	burst := 10
	if v := getEnv("STOCKTWITS_BURST", ""); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			burst = n
		}
	}
	maxInf := 1
	if v := getEnv("STOCKTWITS_MAX_INFLIGHT", ""); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxInf = n
		}
	}
	host := getEnv("STOCKTWITS_HOST", "stocktwits.p.rapidapi.com")
	keys := parseKeysJSON("rapidapi_keys")
	if len(keys) == 0 {
		log.Fatalf("no keys provided in rapidapi_keys")
	}

	cfg := &APIConfig{
		Name:        "stocktwits",
		Host:        host,
		RatePerSec:  rate,
		Burst:       burst,
		MaxInflight: maxInf,
		Keys:        keys,
	}

	// Load scripts
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
		apis:     map[string]*APIConfig{"stocktwits": cfg},
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

	addr := getEnv("LISTEN_ADDR", ":8080")
	log.Printf("rate-broker listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
