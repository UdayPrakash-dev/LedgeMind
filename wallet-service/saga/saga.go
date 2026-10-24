// Package saga runs money transfers as 3 steps:
//
//	DEBIT_SENDER -> CALL_RAIL -> CREDIT_RECEIVER -> DONE (COMPLETED)
//
// What happens when the rail call fails depends on the saga's policy:
//
//	refund   (baseline 1, fixed rules)  refund the sender         -> COMPENSATED
//	retry    (baseline 2)               retry after 1s, 2s, 4s    -> continue, or STUCK
//	escalate (for the AI strategies)    stop at once              -> STUCK
//
// Every STUCK saga is pushed to the stuck-sagas queue. Stuck sagas are only
// changed through the executor (executor.go).
package saga

import (
	"errors"
	"strings"
	"time"
)

// Steps
const (
	StepDebitSender    = "DEBIT_SENDER"
	StepCallRail       = "CALL_RAIL"
	StepCreditReceiver = "CREDIT_RECEIVER"
	StepRefundSender   = "REFUND_SENDER"
	StepReconcile      = "RECONCILE" // completed, but the rail later reversed the payment
	StepDone           = "DONE"
)

// Statuses
const (
	StatusRunning     = "RUNNING"
	StatusCompleted   = "COMPLETED"
	StatusCompensated = "COMPENSATED"
	StatusFailed      = "FAILED"
	StatusStuck       = "STUCK"
	StatusEscalated   = "ESCALATED" // handed to a human
)

// Policies: what to do when the rail call fails (or a credit keeps failing).
const (
	PolicyRefund   = "refund"
	PolicyRetry    = "retry"
	PolicyEscalate = "escalate"
)

func ValidPolicy(p string) bool {
	return p == PolicyRefund || p == PolicyRetry || p == PolicyEscalate
}

var (
	ErrNotFound            = errors.New("transfer not found")
	ErrInvalidTransfer     = errors.New("invalid transfer")
	ErrIdempotencyMismatch = errors.New("idempotency key was already used for a different transfer")
	ErrInvalidAction       = errors.New("invalid action")
	ErrNotActionable       = errors.New("transfer is not STUCK, so no action can be taken")
)

type Saga struct {
	ID             string         `json:"id"`
	IdempotencyKey string         `json:"idempotency_key"`
	FromWallet     string         `json:"from_wallet"`
	ToWallet       string         `json:"to_wallet"`
	Amount         int64          `json:"amount_paise"`
	CurrentStep    string         `json:"current_step"`
	Status         string         `json:"status"`
	Policy         string         `json:"policy"`
	History        []HistoryEntry `json:"history"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

// HistoryEntry records one attempt at one step, or one executor action.
type HistoryEntry struct {
	At      time.Time `json:"at"`
	Step    string    `json:"step"`
	Attempt int       `json:"attempt,omitempty"`
	Outcome string    `json:"outcome"` // "ok" or "error"
	Error   string    `json:"error,omitempty"`
	RailRef string    `json:"rail_ref,omitempty"` // which rail payment id was used
	Action  string    `json:"action,omitempty"`   // set for executor actions
	Actor   string    `json:"actor,omitempty"`    // who asked: ai, human, bench...
	Reason  string    `json:"reason,omitempty"`
}

// LastError returns the most recent error in the history, or "".
func (s *Saga) LastError() string {
	for i := len(s.History) - 1; i >= 0; i-- {
		if s.History[i].Error != "" {
			return s.History[i].Error
		}
	}
	return ""
}

// RailRefs lists every rail payment id this saga has used (original + reroutes).
func (s *Saga) RailRefs() []string {
	refs := []string{s.ID}
	for _, h := range s.History {
		if h.RailRef != "" && h.RailRef != s.ID && !contains(refs, h.RailRef) {
			refs = append(refs, h.RailRef)
		}
	}
	return refs
}

func (s *Saga) rerouteCount() int {
	n := 0
	for _, r := range s.RailRefs() {
		if strings.Contains(r, ":reroute:") {
			n++
		}
	}
	return n
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// Idempotency keys for each money movement. Re-running one with the same key
// is always safe: the wallet applies it at most once.
func debitKey(id string) string         { return "saga:" + id + ":debit" }
func creditKey(id string) string        { return "saga:" + id + ":credit" }
func refundKey(id string) string        { return "saga:" + id + ":refund" }
func reverseKey(id string) string       { return "saga:" + id + ":reverse" }        // take back a receiver credit
func partialRefundKey(id string) string { return "saga:" + id + ":partial-refund" } // executor partial_refund

// ParseMoneyKey splits "saga:<id>:<kind>" into id and kind.
func ParseMoneyKey(key string) (sagaID, kind string, ok bool) {
	rest, found := strings.CutPrefix(key, "saga:")
	if !found {
		return "", "", false
	}
	i := strings.LastIndex(rest, ":")
	if i <= 0 {
		return "", "", false
	}
	return rest[:i], rest[i+1:], true
}
