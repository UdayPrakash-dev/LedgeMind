// Package saga runs money transfers as 3 steps with fixed compensations:
//
//	DEBIT_SENDER -> CALL_RAIL -> CREDIT_RECEIVER -> DONE (COMPLETED)
//	rail fails                 -> REFUND_SENDER -> DONE (COMPENSATED)
//	credit fails 1+3 times     -> REFUND_SENDER -> DONE (COMPENSATED)
//	debit rejected (no money)  -> DONE (FAILED, nothing moved)
//	refund fails 1+3 times     -> STUCK (needs recovery: later the AI's job)
package saga

import (
	"errors"
	"time"
)

// Steps
const (
	StepDebitSender    = "DEBIT_SENDER"
	StepCallRail       = "CALL_RAIL"
	StepCreditReceiver = "CREDIT_RECEIVER"
	StepRefundSender   = "REFUND_SENDER"
	StepDone           = "DONE"
)

// Statuses
const (
	StatusRunning     = "RUNNING"
	StatusCompleted   = "COMPLETED"
	StatusCompensated = "COMPENSATED"
	StatusFailed      = "FAILED"
	StatusStuck       = "STUCK"
)

var (
	ErrNotFound            = errors.New("transfer not found")
	ErrInvalidTransfer     = errors.New("invalid transfer")
	ErrIdempotencyMismatch = errors.New("idempotency key was already used for a different transfer")
)

type Saga struct {
	ID             string         `json:"id"`
	IdempotencyKey string         `json:"idempotency_key"`
	FromWallet     string         `json:"from_wallet"`
	ToWallet       string         `json:"to_wallet"`
	Amount         int64          `json:"amount_paise"`
	CurrentStep    string         `json:"current_step"`
	Status         string         `json:"status"`
	History        []HistoryEntry `json:"history"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

// HistoryEntry records one attempt at one step.
type HistoryEntry struct {
	At      time.Time `json:"at"`
	Step    string    `json:"step"`
	Attempt int       `json:"attempt"`
	Outcome string    `json:"outcome"` // "ok" or "error"
	Error   string    `json:"error,omitempty"`
}

// Idempotency keys for each money step. Re-running a step with the same key
// is always safe: the wallet applies it at most once.
func debitKey(sagaID string) string  { return "saga:" + sagaID + ":debit" }
func creditKey(sagaID string) string { return "saga:" + sagaID + ":credit" }
func refundKey(sagaID string) string { return "saga:" + sagaID + ":refund" }
