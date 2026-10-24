// fake-rail: a pretend external payment service that fails on purpose.
// See rail.go for what each fault mode does.
package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	cfg := DefaultConfig()
	cfg.Seed = envInt("SEED", cfg.Seed)
	cfg.FaultRate = envFloat("FAULT_RATE", cfg.FaultRate)
	cfg.TimeoutDelayMs = envInt("TIMEOUT_DELAY_MS", cfg.TimeoutDelayMs)
	cfg.ReversalDelayMs = envInt("REVERSAL_DELAY_MS", cfg.ReversalDelayMs)
	if w := os.Getenv("FAULT_WEIGHTS"); w != "" { // e.g. "timeout=2,reject=1,unknown=1"
		cfg.Weights = parseWeights(w)
	}

	// Every call is logged as one JSON line to stdout, and to LOG_FILE if set.
	var out io.Writer = os.Stdout
	if path := os.Getenv("LOG_FILE"); path != "" {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Fatalf("log file: %v", err)
		}
		defer f.Close()
		out = io.MultiWriter(os.Stdout, f)
	}

	rail := NewRail(cfg, out)
	port := getenv("PORT", "9000")
	srv := &http.Server{Addr: ":" + port, Handler: rail.Routes(), ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		log.Printf("fake-rail on :%s seed=%d fault_rate=%.2f weights=%v", port, cfg.Seed, cfg.FaultRate, cfg.Weights)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	srv.Shutdown(shutdown)
}

func parseWeights(s string) map[Mode]float64 {
	w := map[Mode]float64{}
	for _, part := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			log.Fatalf("FAULT_WEIGHTS: bad entry %q (want mode=number)", part)
		}
		m := Mode(strings.TrimSpace(k))
		if !validMode(m) || m == ModeNone {
			log.Fatalf("FAULT_WEIGHTS: unknown mode %q", k)
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil || f < 0 {
			log.Fatalf("FAULT_WEIGHTS: bad weight %q", v)
		}
		w[m] = f
	}
	return w
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int64) int64 {
	if v := os.Getenv(k); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			log.Fatalf("%s: %v", k, err)
		}
		return n
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			log.Fatalf("%s: %v", k, err)
		}
		return f
	}
	return def
}
