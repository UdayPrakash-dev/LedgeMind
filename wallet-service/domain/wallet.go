// Package domain holds the wallet rules. It has no database or HTTP code:
// commands check the rules and return events; Apply replays events into state.
package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"ledgemind/wallet-service/eventstore"
)

// Event types stored in the event store.
const (
	EventWalletOpened   = "WalletOpened"
	EventWalletCredited = "WalletCredited"
	EventWalletDebited  = "WalletDebited"
)

// MaxAmount caps a single credit or debit: 10^12 paise = ₹1,000 crore.
const MaxAmount int64 = 1_000_000_000_000

var (
	ErrWalletNotFound      = errors.New("wallet not found")
	ErrAlreadyOpened       = errors.New("wallet already opened")
	ErrInvalidOwner        = errors.New("owner must be 1-100 characters")
	ErrInvalidAmount       = errors.New("amount must be between 1 and 10^12 paise")
	ErrInsufficientFunds   = errors.New("insufficient funds")
	ErrMissingKey          = errors.New("idempotency key is required")
	ErrIdempotencyMismatch = errors.New("idempotency key was already used for a different request")
)

// Event payloads. Money is always int64 paise.
type OpenedPayload struct {
	WalletID string `json:"wallet_id"`
	Owner    string `json:"owner"`
}

type MoneyPayload struct {
	Amount         int64  `json:"amount"`
	IdempotencyKey string `json:"idempotency_key"`
}

// Wallet is the aggregate: current state rebuilt from events.
type Wallet struct {
	ID      string
	Owner   string
	Balance int64 // paise
	Version int64 // version of the last applied event (0 = no events)

	opened bool
	ops    map[string]Operation // idempotency key -> what it did
}

func NewWallet(id string) *Wallet {
	return &Wallet{ID: id, ops: map[string]Operation{}}
}

func (w *Wallet) Exists() bool { return w.opened }

// Rehydrate rebuilds a wallet by replaying its events in order.
func Rehydrate(id string, events []eventstore.Event) (*Wallet, error) {
	w := NewWallet(id)
	for _, e := range events {
		if err := w.Apply(e); err != nil {
			return nil, err
		}
	}
	return w, nil
}

// ---------- commands: check rules, return new events (state is NOT changed) ----------

func (w *Wallet) Open(owner string) ([]eventstore.NewEvent, error) {
	if w.opened {
		return nil, ErrAlreadyOpened
	}
	owner = strings.TrimSpace(owner)
	if owner == "" || len([]rune(owner)) > 100 {
		return nil, ErrInvalidOwner
	}
	return []eventstore.NewEvent{{
		Type:    EventWalletOpened,
		Payload: OpenedPayload{WalletID: w.ID, Owner: owner},
	}}, nil
}

// Credit returns (nil, nil) when the key was already applied with the same amount:
// the request is a harmless repeat and nothing new should be written.
func (w *Wallet) Credit(amount int64, key string) ([]eventstore.NewEvent, error) {
	if err := w.checkMoneyCommand(EventWalletCredited, amount, key); err != nil {
		return nil, err
	}
	if w.alreadyApplied(EventWalletCredited, amount, key) {
		return nil, nil
	}
	if w.Balance > maxInt64-amount {
		return nil, ErrInvalidAmount
	}
	return []eventstore.NewEvent{{
		Type:    EventWalletCredited,
		Payload: MoneyPayload{Amount: amount, IdempotencyKey: key},
	}}, nil
}

func (w *Wallet) Debit(amount int64, key string) ([]eventstore.NewEvent, error) {
	if err := w.checkMoneyCommand(EventWalletDebited, amount, key); err != nil {
		return nil, err
	}
	// Check the repeat BEFORE the balance: a retried debit must still succeed
	// even if the balance has since dropped.
	if w.alreadyApplied(EventWalletDebited, amount, key) {
		return nil, nil
	}
	if amount > w.Balance {
		return nil, ErrInsufficientFunds
	}
	return []eventstore.NewEvent{{
		Type:    EventWalletDebited,
		Payload: MoneyPayload{Amount: amount, IdempotencyKey: key},
	}}, nil
}

// Op reports what an idempotency key was used for, if anything.
func (w *Wallet) Op(key string) (Operation, bool) {
	op, ok := w.ops[key]
	return op, ok
}

// IsRepeat reports whether key was already used for exactly this operation.
func (w *Wallet) IsRepeat(eventType string, amount int64, key string) bool {
	return w.alreadyApplied(eventType, amount, key)
}

func (w *Wallet) checkMoneyCommand(eventType string, amount int64, key string) error {
	if !w.opened {
		return ErrWalletNotFound
	}
	if amount <= 0 || amount > MaxAmount {
		return ErrInvalidAmount
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return ErrMissingKey
	}
	if op, seen := w.ops[key]; seen && (op.Type != eventType || op.Amount != amount) {
		return ErrIdempotencyMismatch
	}
	return nil
}

func (w *Wallet) alreadyApplied(eventType string, amount int64, key string) bool {
	op, seen := w.ops[key]
	return seen && op.Type == eventType && op.Amount == amount
}

// ---------- Apply: replay one stored event into state (never panics) ----------

func (w *Wallet) Apply(e eventstore.Event) error {
	if e.Version != w.Version+1 {
		return fmt.Errorf("wallet %s: event version %d after %d (stream corrupt?)", w.ID, e.Version, w.Version)
	}
	switch e.Type {
	case EventWalletOpened:
		var p OpenedPayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return fmt.Errorf("wallet %s: bad %s payload: %w", w.ID, e.Type, err)
		}
		if w.opened {
			return fmt.Errorf("wallet %s: opened twice", w.ID)
		}
		w.opened = true
		w.Owner = p.Owner

	case EventWalletCredited, EventWalletDebited:
		var p MoneyPayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return fmt.Errorf("wallet %s: bad %s payload: %w", w.ID, e.Type, err)
		}
		if !w.opened {
			return fmt.Errorf("wallet %s: %s before WalletOpened", w.ID, e.Type)
		}
		if p.Amount <= 0 {
			return fmt.Errorf("wallet %s: %s with amount %d", w.ID, e.Type, p.Amount)
		}
		if e.Type == EventWalletCredited {
			w.Balance += p.Amount
		} else {
			w.Balance -= p.Amount
		}
		if p.IdempotencyKey != "" {
			w.ops[p.IdempotencyKey] = Operation{Type: e.Type, Amount: p.Amount}
		}

	default:
		return fmt.Errorf("wallet %s: unknown event type %q", w.ID, e.Type)
	}
	w.Version = e.Version
	return nil
}

const maxInt64 = int64(^uint64(0) >> 1)
