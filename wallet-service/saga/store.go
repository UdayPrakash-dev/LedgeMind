package saga

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store keeps saga rows in Postgres.
type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store { return &Store{db: db} }

const columns = `saga_id, idempotency_key, from_wallet, to_wallet, amount, current_step, status, history, created_at, updated_at`

// Create inserts a new saga. If the idempotency key already exists, it returns
// the existing saga with created=false instead.
func (s *Store) Create(ctx context.Context, sg *Saga) (*Saga, bool, error) {
	row := s.db.QueryRow(ctx, `
		INSERT INTO sagas (saga_id, idempotency_key, from_wallet, to_wallet, amount, current_step, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+columns,
		sg.ID, sg.IdempotencyKey, sg.FromWallet, sg.ToWallet, sg.Amount, sg.CurrentStep, sg.Status)
	created, err := scan(row)
	if err == nil {
		return created, true, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, false, err
	}
	existing, err := s.getBy(ctx, "idempotency_key", sg.IdempotencyKey)
	return existing, false, err
}

func (s *Store) Get(ctx context.Context, id string) (*Saga, error) {
	return s.getBy(ctx, "saga_id", id)
}

func (s *Store) getBy(ctx context.Context, column, value string) (*Saga, error) {
	// column is one of two fixed names above, never user input.
	return scan(s.db.QueryRow(ctx, `SELECT `+columns+` FROM sagas WHERE `+column+` = $1`, value))
}

// Advance moves a saga to a new step/status and appends history entries.
func (s *Store) Advance(ctx context.Context, id, step, status string, entries ...HistoryEntry) error {
	if entries == nil {
		entries = []HistoryEntry{}
	}
	raw, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	tag, err := s.db.Exec(ctx, `
		UPDATE sagas
		   SET current_step = $2, status = $3, history = history || $4::jsonb, updated_at = now()
		 WHERE saga_id = $1`, id, step, status, raw)
	if err != nil {
		return fmt.Errorf("saga advance: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrNotFound
	}
	return nil
}

// ListByStatus returns sagas with a given status, oldest first.
func (s *Store) ListByStatus(ctx context.Context, status string, limit int) ([]*Saga, error) {
	rows, err := s.db.Query(ctx, `SELECT `+columns+` FROM sagas WHERE status = $1 ORDER BY created_at LIMIT $2`, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Saga
	for rows.Next() {
		sg, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, sg)
	}
	return out, rows.Err()
}

func scan(row pgx.Row) (*Saga, error) {
	var sg Saga
	var history []byte
	err := row.Scan(&sg.ID, &sg.IdempotencyKey, &sg.FromWallet, &sg.ToWallet, &sg.Amount,
		&sg.CurrentStep, &sg.Status, &history, &sg.CreatedAt, &sg.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("saga scan: %w", err)
	}
	if err := json.Unmarshal(history, &sg.History); err != nil {
		return nil, fmt.Errorf("saga history: %w", err)
	}
	return &sg, nil
}
