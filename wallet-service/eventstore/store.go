// Package eventstore saves and loads events in Postgres.
package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrConcurrencyConflict means someone else wrote to the stream first.
// The caller should reload the stream and try again.
var ErrConcurrencyConflict = errors.New("eventstore: concurrency conflict")

// NewEvent is an event that has not been saved yet.
type NewEvent struct {
	Type    string
	Payload any // anything that can be turned into JSON
}

// Event is an event read back from the store.
type Event struct {
	ID        uuid.UUID
	StreamID  string
	Version   int64
	Type      string
	Payload   json.RawMessage
	CreatedAt time.Time
}

// Store wraps a Postgres connection pool.
type Store struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Store { return &Store{db: db} }

// AppendEvents adds events to a stream.
//
// expectedVersion is the version the caller last saw (0 for a brand-new stream).
// If the stream is no longer at that version, nothing is written and
// ErrConcurrencyConflict is returned. On success it returns the new version.
func (s *Store) AppendEvents(ctx context.Context, streamID string, expectedVersion int64, events []NewEvent) (int64, error) {
	if streamID == "" {
		return 0, errors.New("eventstore: empty stream id")
	}
	if expectedVersion < 0 {
		return 0, errors.New("eventstore: negative expected version")
	}
	if len(events) == 0 {
		return expectedVersion, nil
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("eventstore: begin: %w", err)
	}
	defer tx.Rollback(ctx) // no-op after Commit

	// Fast check: is the stream still where the caller thinks it is?
	var current int64
	err = tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = $1`, streamID,
	).Scan(&current)
	if err != nil {
		return 0, fmt.Errorf("eventstore: read version: %w", err)
	}
	if current != expectedVersion {
		return 0, ErrConcurrencyConflict
	}

	// Insert each event with the next version number.
	// If another writer slipped in between our check and this insert,
	// the UNIQUE(stream_id, version) constraint rejects us.
	for i, e := range events {
		if e.Type == "" {
			return 0, errors.New("eventstore: event with empty type")
		}
		id, err := uuid.NewV7()
		if err != nil {
			return 0, fmt.Errorf("eventstore: new id: %w", err)
		}
		payload, err := json.Marshal(e.Payload)
		if err != nil {
			return 0, fmt.Errorf("eventstore: marshal %s: %w", e.Type, err)
		}
		if e.Payload == nil {
			payload = []byte("{}")
		}
		_, err = tx.Exec(ctx,
			`INSERT INTO events (id, stream_id, version, type, payload) VALUES ($1, $2, $3, $4, $5)`,
			id, streamID, expectedVersion+int64(i)+1, e.Type, payload,
		)
		if err != nil {
			if isUniqueViolation(err) {
				return 0, ErrConcurrencyConflict
			}
			return 0, fmt.Errorf("eventstore: insert: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		if isUniqueViolation(err) {
			return 0, ErrConcurrencyConflict
		}
		return 0, fmt.Errorf("eventstore: commit: %w", err)
	}
	return expectedVersion + int64(len(events)), nil
}

// LoadEvents returns all events in a stream with version > fromVersion, oldest first.
// Use fromVersion = 0 to load the whole stream.
func (s *Store) LoadEvents(ctx context.Context, streamID string, fromVersion int64) ([]Event, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, stream_id, version, type, payload, created_at
		   FROM events
		  WHERE stream_id = $1 AND version > $2
		  ORDER BY version`,
		streamID, fromVersion,
	)
	if err != nil {
		return nil, fmt.Errorf("eventstore: query: %w", err)
	}
	events, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Event, error) {
		var e Event
		err := r.Scan(&e.ID, &e.StreamID, &e.Version, &e.Type, &e.Payload, &e.CreatedAt)
		return e, err
	})
	if err != nil {
		return nil, fmt.Errorf("eventstore: scan: %w", err)
	}
	return events, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
