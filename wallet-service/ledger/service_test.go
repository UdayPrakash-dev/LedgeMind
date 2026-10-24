package ledger

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"ledgemind/wallet-service/domain"
	"ledgemind/wallet-service/eventstore"
	"ledgemind/wallet-service/internal/testdb"
)

// memSnaps is an in-memory SnapshotStore for tests.
type memSnaps struct {
	mu   sync.Mutex
	data map[string]domain.Snapshot
	puts int
}

func newMemSnaps() *memSnaps { return &memSnaps{data: map[string]domain.Snapshot{}} }

func (m *memSnaps) Get(_ context.Context, id string) (*domain.Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.data[id]
	if !ok {
		return nil, nil
	}
	return &s, nil
}

func (m *memSnaps) Put(_ context.Context, s domain.Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts++
	m.data[s.WalletID] = s
	return nil
}

// brokenSnaps simulates Redis being down.
type brokenSnaps struct{}

func (brokenSnaps) Get(context.Context, string) (*domain.Snapshot, error) {
	return nil, errors.New("redis down")
}
func (brokenSnaps) Put(context.Context, domain.Snapshot) error { return errors.New("redis down") }

func credits(t *testing.T, s *Service, id string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, _, err := s.Credit(context.Background(), id, 100, fmt.Sprintf("k-%d", i)); err != nil {
			t.Fatalf("credit %d: %v", i, err)
		}
	}
}

func TestSnapshotTakenEvery50AndLoadMatchesFullReplay(t *testing.T) {
	store := eventstore.New(testdb.New(t))
	snaps := newMemSnaps()
	cached := NewService(store, snaps, 50)
	plain := NewService(store, nil, 0)
	ctx := context.Background()

	w, err := cached.OpenWallet(ctx, "snap-test")
	if err != nil {
		t.Fatal(err)
	}
	credits(t, cached, w.ID, 120) // versions 2..121 -> snapshots at 50 and 100

	if snaps.puts != 2 {
		t.Fatalf("snapshots taken = %d, want 2", snaps.puts)
	}
	snap, _ := snaps.Get(ctx, w.ID)
	if snap.Version != 100 {
		t.Fatalf("latest snapshot version = %d, want 100", snap.Version)
	}

	before := cached.Stats.EventsReplayed.Load()
	a, err := cached.Get(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if n := cached.Stats.EventsReplayed.Load() - before; n != 21 {
		t.Fatalf("cached load replayed %d events, want 21 (versions 101..121)", n)
	}
	b, err := plain.Get(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if a.Balance != b.Balance || a.Version != b.Version || a.Balance != 12_000 {
		t.Fatalf("snapshot load %d/v%d != full replay %d/v%d", a.Balance, a.Version, b.Balance, b.Version)
	}
}

// The subtle bug snapshots can cause: forgetting idempotency keys from before the snapshot.
func TestIdempotencySurvivesSnapshot(t *testing.T) {
	store := eventstore.New(testdb.New(t))
	s := NewService(store, newMemSnaps(), 50)
	ctx := context.Background()

	w, _ := s.OpenWallet(ctx, "idem-snap")
	credits(t, s, w.ID, 60) // key k-0 is long before the snapshot at v50

	_, applied, err := s.Credit(ctx, w.ID, 100, "k-0")
	if err != nil || applied {
		t.Fatalf("repeat of k-0 after snapshot: applied=%v err=%v, want false,nil", applied, err)
	}
	got, _ := s.Get(ctx, w.ID)
	if got.Balance != 6_000 {
		t.Fatalf("balance = %d, want 6000", got.Balance)
	}
}

func TestRedisDownFallsBackToFullReplay(t *testing.T) {
	store := eventstore.New(testdb.New(t))
	s := NewService(store, brokenSnaps{}, 50)
	ctx := context.Background()

	w, err := s.OpenWallet(ctx, "no-redis")
	if err != nil {
		t.Fatal(err)
	}
	credits(t, s, w.ID, 55)
	got, err := s.Get(ctx, w.ID)
	if err != nil {
		t.Fatalf("get with Redis down: %v", err)
	}
	if got.Balance != 5_500 {
		t.Fatalf("balance = %d, want 5500", got.Balance)
	}
	if s.Stats.SnapshotErrors.Load() == 0 {
		t.Fatal("expected snapshot errors to be counted")
	}
}

func TestCorruptSnapshotIsIgnored(t *testing.T) {
	store := eventstore.New(testdb.New(t))
	snaps := newMemSnaps()
	s := NewService(store, snaps, 50)
	ctx := context.Background()

	w, _ := s.OpenWallet(ctx, "corrupt")
	credits(t, s, w.ID, 3)
	snaps.data[w.ID] = domain.Snapshot{WalletID: w.ID, Version: 0, Balance: -5} // garbage

	got, err := s.Get(ctx, w.ID)
	if err != nil || got.Balance != 300 {
		t.Fatalf("got %v err %v, want balance 300 via full replay", got, err)
	}
}

// Go benchmark for the paper: go test -bench Load -run ^$ ./ledger
func BenchmarkLoad(b *testing.B) {
	store := eventstore.New(testdb.New(b))
	ctx := context.Background()
	setup := NewService(store, newMemSnaps(), 50)
	w, _ := setup.OpenWallet(ctx, "bench")
	for i := 0; i < 1000; i++ {
		setup.Credit(ctx, w.ID, 1, fmt.Sprintf("b-%d", i))
	}
	snaps := setup.snaps

	b.Run("full-replay-1000-events", func(b *testing.B) {
		s := NewService(store, nil, 0)
		for i := 0; i < b.N; i++ {
			if _, err := s.Get(ctx, w.ID); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("snapshot-plus-tail", func(b *testing.B) {
		s := NewService(store, snaps, 50)
		for i := 0; i < b.N; i++ {
			if _, err := s.Get(ctx, w.ID); err != nil {
				b.Fatal(err)
			}
		}
	})
}
