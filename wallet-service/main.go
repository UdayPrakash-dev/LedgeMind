// Wallet service entry point: connects to Postgres and serves the wallet API.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"ledgemind/wallet-service/api"
	"ledgemind/wallet-service/audit"
	"ledgemind/wallet-service/eventstore"
	"ledgemind/wallet-service/ledger"
	"ledgemind/wallet-service/migrations"
	"ledgemind/wallet-service/recoverylog"
	"ledgemind/wallet-service/saga"
	"ledgemind/wallet-service/snapshot"
	"ledgemind/wallet-service/stuckqueue"
)

func main() {
	dbURL := getenv("DATABASE_URL", "postgres://ledgemind:ledgemind@localhost:5432/ledgemind?sslmode=disable")
	port := getenv("PORT", "8080")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := connectWithRetry(ctx, dbURL)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer pool.Close()

	if err := migrations.Apply(ctx, pool); err != nil {
		log.Fatalf("migrations: %v", err)
	}

	// Redis snapshots are optional: without REDIS_ADDR every load is a full replay.
	var snaps ledger.SnapshotStore
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		rs := snapshot.NewRedisStore(addr)
		defer rs.Close()
		if err := rs.Ping(ctx); err != nil {
			log.Printf("redis %s not reachable yet (%v); continuing, loads fall back to replay", addr, err)
		}
		snaps = rs
	}
	every, _ := strconv.ParseInt(getenv("SNAPSHOT_EVERY", "50"), 10, 64)
	events := eventstore.New(pool)
	svc := ledger.NewService(events, snaps, every)

	// Payment rail: the fake-rail service from Day 4, or "always succeeds" if unset.
	var rail saga.PaymentRail = saga.AlwaysOKRail{}
	if url := os.Getenv("RAIL_URL"); url != "" {
		rail = saga.NewHTTPRail(url, 2*time.Second)
		log.Printf("payment rail: %s", url)
	} else {
		log.Printf("payment rail: none configured, every payment succeeds")
	}
	cfg := saga.DefaultConfig()
	cfg.Policy = getenv("RECOVERY_POLICY", saga.PolicyRefund) // refund | retry | escalate
	if ms, err := strconv.Atoi(os.Getenv("RAIL_RETRY_BACKOFF_MS")); err == nil && ms > 0 {
		cfg.RailBackoff = time.Duration(ms) * time.Millisecond
	}
	store := saga.NewStore(pool)
	orch := saga.NewOrchestrator(store, svc, rail, cfg)
	log.Printf("recovery policy for new transfers: %s", orch.Policy())

	// Every STUCK saga goes to the Redis Stream "stuck-sagas" for the recovery service.
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		q := stuckqueue.New(addr, getenv("STUCK_STREAM", stuckqueue.DefaultStream))
		defer q.Close()
		orch.SetNotifier(q)
	}

	var railStatus saga.RailStatus
	if rs, ok := rail.(saga.RailStatus); ok {
		railStatus = rs
	}
	auditor := audit.New(pool, store, svc, railStatus)

	// Finish transfers that were mid-way when the service last stopped.
	if n, err := orch.ResumeRunning(ctx); err != nil {
		log.Printf("resume sagas: %v", err)
	} else if n > 0 {
		log.Printf("resumed %d unfinished transfers", n)
	}

	health := func(w http.ResponseWriter, r *http.Request) {
		status, code := "ok", http.StatusOK
		if err := pool.Ping(r.Context()); err != nil {
			status, code = "db unreachable", http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(map[string]string{"service": "wallet", "status": status})
	}

	handler := logRequests(api.NewServer(svc, orch, auditor, health).WithRecoveryLog(recoverylog.New(events)))

	srv := &http.Server{Addr: ":" + port, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("wallet-service listening on :%s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
	log.Println("wallet-service stopped")
}

// logRequests prints one line per request: method, path, status, duration.
func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(rec, r)
		if r.URL.Path != "/health" {
			log.Printf("%s %s -> %d (%s)", r.Method, r.URL.Path, rec.status, time.Since(start).Round(time.Millisecond))
		}
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// connectWithRetry waits for Postgres to accept connections (up to ~30s).
func connectWithRetry(ctx context.Context, url string) (*pgxpool.Pool, error) {
	var lastErr error
	for i := 0; i < 15; i++ {
		pool, err := pgxpool.New(ctx, url)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return pool, nil
			}
			pool.Close()
		}
		lastErr = err
		log.Printf("waiting for database (%d/15): %v", i+1, err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, lastErr
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
