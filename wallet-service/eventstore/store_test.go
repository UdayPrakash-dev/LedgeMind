package eventstore

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"ledgemind/wallet-service/internal/testdb"
)

// These tests need a running Postgres (docker compose up -d postgres) and
// DATABASE_URL set; see README. Without it they are skipped.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	return New(testdb.New(t))
}

// Each test uses a fresh random stream id, so tests never interfere.
func newStreamID() string { return "test-wallet-" + uuid.NewString() }

func TestAppendAndLoad(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	stream := newStreamID()

	v, err := s.AppendEvents(ctx, stream, 0, []NewEvent{
		{Type: "WalletOpened", Payload: map[string]any{"owner": "uday"}},
		{Type: "WalletCredited", Payload: map[string]any{"amount": 50000}},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if v != 2 {
		t.Fatalf("new version = %d, want 2", v)
	}

	all, err := s.LoadEvents(ctx, stream, 0)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(all) != 2 || all[0].Version != 1 || all[1].Version != 2 {
		t.Fatalf("unexpected events: %+v", all)
	}
	if all[0].Type != "WalletOpened" || all[1].Type != "WalletCredited" {
		t.Fatalf("wrong types: %s, %s", all[0].Type, all[1].Type)
	}

	tail, err := s.LoadEvents(ctx, stream, 1)
	if err != nil {
		t.Fatalf("load tail: %v", err)
	}
	if len(tail) != 1 || tail[0].Version != 2 {
		t.Fatalf("tail = %+v, want only version 2", tail)
	}
}

func TestStaleVersionIsRejected(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	stream := newStreamID()

	if _, err := s.AppendEvents(ctx, stream, 0, []NewEvent{{Type: "WalletOpened"}}); err != nil {
		t.Fatalf("first append: %v", err)
	}
	// Caller still thinks the stream is empty -> must be rejected.
	_, err := s.AppendEvents(ctx, stream, 0, []NewEvent{{Type: "WalletCredited"}})
	if !errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("err = %v, want ErrConcurrencyConflict", err)
	}
	// Caller skips ahead -> also rejected.
	_, err = s.AppendEvents(ctx, stream, 5, []NewEvent{{Type: "WalletCredited"}})
	if !errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("err = %v, want ErrConcurrencyConflict", err)
	}
}

// The Day 1 acceptance test: two writers race at the same version,
// exactly one wins. Repeated many times to make the race real.
func TestConcurrentWritersExactlyOneWins(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const rounds = 50
	for r := 0; r < rounds; r++ {
		stream := newStreamID()
		if _, err := s.AppendEvents(ctx, stream, 0, []NewEvent{{Type: "WalletOpened"}}); err != nil {
			t.Fatalf("setup: %v", err)
		}

		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make([]error, 2)
		for w := 0; w < 2; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				<-start // release both goroutines at the same moment
				_, errs[w] = s.AppendEvents(ctx, stream, 1, []NewEvent{
					{Type: "WalletDebited", Payload: map[string]any{"amount": 100, "writer": w}},
				})
			}(w)
		}
		close(start)
		wg.Wait()

		wins, conflicts := 0, 0
		for _, err := range errs {
			switch {
			case err == nil:
				wins++
			case errors.Is(err, ErrConcurrencyConflict):
				conflicts++
			default:
				t.Fatalf("round %d: unexpected error: %v", r, err)
			}
		}
		if wins != 1 || conflicts != 1 {
			t.Fatalf("round %d: wins=%d conflicts=%d, want 1 and 1", r, wins, conflicts)
		}

		got, err := s.LoadEvents(ctx, stream, 0)
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("round %d: stream has %d events, want 2", r, len(got))
		}
	}
}

func TestEventsAreAppendOnly(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	stream := newStreamID()

	if _, err := s.AppendEvents(ctx, stream, 0, []NewEvent{{Type: "WalletOpened"}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := s.db.Exec(ctx, `UPDATE events SET type = 'Hacked' WHERE stream_id = $1`, stream); err == nil {
		t.Fatal("UPDATE succeeded, want it blocked")
	}
	if _, err := s.db.Exec(ctx, `DELETE FROM events WHERE stream_id = $1`, stream); err == nil {
		t.Fatal("DELETE succeeded, want it blocked")
	}
}
