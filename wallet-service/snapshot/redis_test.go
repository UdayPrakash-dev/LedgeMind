package snapshot

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"

	"ledgemind/wallet-service/domain"
)

// Needs a running Redis: set REDIS_ADDR=localhost:6379 (docker compose up -d redis).
func newStore(t *testing.T) *RedisStore {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set; skipping Redis test")
	}
	s := NewRedisStore(addr)
	t.Cleanup(func() { s.Close() })
	if err := s.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return s
}

func TestPutGetAndNeverGoBackwards(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	id := uuid.NewString()

	if got, err := s.Get(ctx, id); err != nil || got != nil {
		t.Fatalf("empty get: %v %v", got, err)
	}

	snap := domain.Snapshot{WalletID: id, Owner: "u", Balance: 500, Version: 100,
		Ops: map[string]domain.Operation{"k1": {Type: domain.EventWalletCredited, Amount: 500}}}
	if err := s.Put(ctx, snap); err != nil {
		t.Fatalf("put: %v", err)
	}
	old := snap
	old.Version, old.Balance = 50, 1
	if err := s.Put(ctx, old); err != nil {
		t.Fatalf("put old: %v", err)
	}

	got, err := s.Get(ctx, id)
	if err != nil || got == nil {
		t.Fatalf("get: %v %v", got, err)
	}
	if got.Version != 100 || got.Balance != 500 || got.Ops["k1"].Amount != 500 {
		t.Fatalf("got %+v, want version 100 snapshot kept", got)
	}
}

func TestDeadRedisReturnsErrorQuickly(t *testing.T) {
	s := NewRedisStore("127.0.0.1:1") // nothing listens here
	defer s.Close()
	if _, err := s.Get(context.Background(), "x"); err == nil {
		t.Fatal("want error from dead Redis")
	}
}
