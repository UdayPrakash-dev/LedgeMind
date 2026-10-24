package stuckqueue

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"ledgemind/wallet-service/saga"
)

func TestStuckSagaLandsInStream(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	stream := "test-stuck-" + uuid.NewString()
	q := New(addr, stream)
	defer q.Close()
	ctx := context.Background()
	defer q.client.Del(ctx, stream)

	sg := &saga.Saga{ID: uuid.NewString(), CurrentStep: saga.StepCallRail, Policy: saga.PolicyEscalate, Amount: 500,
		History: []saga.HistoryEntry{{At: time.Now(), Step: saga.StepCallRail, Outcome: "error", Error: "rail exploded"}}}
	if err := q.Stuck(ctx, sg); err != nil {
		t.Fatal(err)
	}
	msgs, err := q.client.XRange(ctx, stream, "-", "+").Result()
	if err != nil || len(msgs) != 1 {
		t.Fatalf("msgs=%v err=%v", msgs, err)
	}
	v := msgs[0].Values
	if v["saga_id"] != sg.ID || v["step"] != saga.StepCallRail || v["error"] != "rail exploded" || v["policy"] != "escalate" {
		t.Fatalf("message = %v", v)
	}
}
