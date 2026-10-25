// Package recoverylog records every recovery decision as an event in the same
// append-only event store as the money, in stream "recovery:<transfer id>".
// The events cannot be changed afterwards, so the AI's decisions are auditable.
package recoverylog

import (
	"context"
	"errors"
	"fmt"

	"ledgemind/wallet-service/eventstore"
)

const (
	EventProposed = "RecoveryProposed" // a plan was suggested (by the AI or from memory)
	EventVerified = "RecoveryVerified" // the verifier approved it
	EventRejected = "RecoveryRejected" // the verifier blocked it
	EventExecuted = "RecoveryExecuted" // the executor ran it
)

var ErrInvalidType = errors.New("event type must be RecoveryProposed, RecoveryVerified, RecoveryRejected or RecoveryExecuted")

func ValidType(t string) bool {
	return t == EventProposed || t == EventVerified || t == EventRejected || t == EventExecuted
}

type Log struct {
	store *eventstore.Store
}

func New(store *eventstore.Store) *Log { return &Log{store: store} }

func StreamID(sagaID string) string { return "recovery:" + sagaID }

// Append adds one event, retrying if another writer appended at the same time.
func (l *Log) Append(ctx context.Context, sagaID, eventType string, data map[string]any) (int64, error) {
	if !ValidType(eventType) {
		return 0, ErrInvalidType
	}
	if data == nil {
		data = map[string]any{}
	}
	for attempt := 0; attempt < 10; attempt++ {
		existing, err := l.store.LoadEvents(ctx, StreamID(sagaID), 0)
		if err != nil {
			return 0, err
		}
		v, err := l.store.AppendEvents(ctx, StreamID(sagaID), int64(len(existing)),
			[]eventstore.NewEvent{{Type: eventType, Payload: data}})
		if err == nil {
			return v, nil
		}
		if !errors.Is(err, eventstore.ErrConcurrencyConflict) {
			return 0, err
		}
	}
	return 0, fmt.Errorf("recoverylog: too many concurrent writers for %s", sagaID)
}

// List returns all recovery events of a transfer, oldest first.
func (l *Log) List(ctx context.Context, sagaID string) ([]eventstore.Event, error) {
	return l.store.LoadEvents(ctx, StreamID(sagaID), 0)
}
