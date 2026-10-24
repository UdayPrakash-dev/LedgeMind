// Package stuckqueue pushes every STUCK saga to the Redis Stream "stuck-sagas".
// The recovery service (Day 6) reads from it with a consumer group.
package stuckqueue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	"ledgemind/wallet-service/saga"
)

const DefaultStream = "stuck-sagas"

type Queue struct {
	client *redis.Client
	stream string
}

func New(addr, stream string) *Queue {
	if stream == "" {
		stream = DefaultStream
	}
	return &Queue{
		client: redis.NewClient(&redis.Options{Addr: addr, DialTimeout: time.Second,
			ReadTimeout: time.Second, WriteTimeout: time.Second}),
		stream: stream,
	}
}

// Stuck adds one message: saga_id, step, policy, error, history (JSON), amount, at.
func (q *Queue) Stuck(ctx context.Context, sg *saga.Saga) error {
	history, err := json.Marshal(sg.History)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream,
		MaxLen: 100_000,
		Approx: true,
		Values: map[string]any{
			"saga_id":      sg.ID,
			"step":         sg.CurrentStep,
			"policy":       sg.Policy,
			"error":        sg.LastError(),
			"amount_paise": sg.Amount,
			"history":      string(history),
			"at":           time.Now().UTC().Format(time.RFC3339Nano),
		},
	}).Err()
}

func (q *Queue) Close() error { return q.client.Close() }
