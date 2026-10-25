package recoverylog

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"ledgemind/wallet-service/eventstore"
	"ledgemind/wallet-service/internal/testdb"
)

func TestAppendAndListInOrder(t *testing.T) {
	l := New(eventstore.New(testdb.New(t)))
	ctx := context.Background()
	id := uuid.NewString()

	for _, typ := range []string{EventProposed, EventVerified, EventExecuted} {
		if _, err := l.Append(ctx, id, typ, map[string]any{"action": "retry_step"}); err != nil {
			t.Fatal(err)
		}
	}
	evs, err := l.List(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 3 || evs[0].Type != EventProposed || evs[2].Type != EventExecuted || evs[2].Version != 3 {
		t.Fatalf("events = %+v", evs)
	}
	var p map[string]any
	json.Unmarshal(evs[0].Payload, &p)
	if p["action"] != "retry_step" {
		t.Fatalf("payload = %v", p)
	}
}

func TestRejectsUnknownType(t *testing.T) {
	l := New(eventstore.New(testdb.New(t)))
	if _, err := l.Append(context.Background(), "x", "RecoveryHacked", nil); !errors.Is(err, ErrInvalidType) {
		t.Fatalf("err = %v", err)
	}
}

func TestConcurrentAppendsAllLand(t *testing.T) {
	l := New(eventstore.New(testdb.New(t)))
	id := uuid.NewString()
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := l.Append(context.Background(), id, EventProposed, nil); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	evs, _ := l.List(context.Background(), id)
	if len(evs) != 6 {
		t.Fatalf("%d events, want 6", len(evs))
	}
}
