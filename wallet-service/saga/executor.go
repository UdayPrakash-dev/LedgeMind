package saga

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Executor actions. This is the ONLY way to change a STUCK saga; the AI, the
// benchmark and humans all go through here.
//
// The executor keeps the LEDGER safe (no negative balances, nothing applied
// twice), but it does NOT judge whether an action is wise: compensating a
// payment the rail actually made is allowed here. Catching that is the job of
// the verifier in the recovery service (Day 7), and of the invariant check.
const (
	ActionRetryStep     = "retry_step"     // run the failed step again
	ActionCompensate    = "compensate"     // undo everything: take back receiver credit, refund sender
	ActionReroute       = "reroute"        // pay through the rail again under a new payment id
	ActionPartialRefund = "partial_refund" // refund part of the amount to the sender
	ActionEscalate      = "escalate"       // hand over to a human
)

var allActions = []string{ActionRetryStep, ActionCompensate, ActionReroute, ActionPartialRefund, ActionEscalate}

type Action struct {
	Type   string `json:"action"`
	Rail   string `json:"rail,omitempty"`         // reroute: name of the alternate rail (simulated)
	Amount int64  `json:"amount_paise,omitempty"` // partial_refund
	Reason string `json:"reason,omitempty"`
	Actor  string `json:"actor,omitempty"` // ai, human, bench, ...
}

// MoneyState is what has actually happened in the ledger for one saga,
// read from the wallets' idempotency keys (the source of truth).
type MoneyState struct {
	Debited       bool  `json:"debited"`
	Credited      bool  `json:"credited"`
	Refunded      bool  `json:"refunded"`
	Reversed      bool  `json:"reversed"`
	PartialRefund int64 `json:"partial_refund_paise"`
}

func (o *Orchestrator) MoneyState(ctx context.Context, sg *Saga) (MoneyState, error) {
	var ms MoneyState
	from, err := o.wallets.Get(ctx, sg.FromWallet)
	if err != nil {
		return ms, err
	}
	to, err := o.wallets.Get(ctx, sg.ToWallet)
	if err != nil {
		return ms, err
	}
	_, ms.Debited = from.Op(debitKey(sg.ID))
	_, ms.Refunded = from.Op(refundKey(sg.ID))
	if op, ok := from.Op(partialRefundKey(sg.ID)); ok {
		ms.PartialRefund = op.Amount
	}
	_, ms.Credited = to.Op(creditKey(sg.ID))
	_, ms.Reversed = to.Op(reverseKey(sg.ID))
	return ms, nil
}

// Execute applies one action to a STUCK saga and returns the saga afterwards.
// After the action, the saga continues with policy "escalate": any new failure
// makes it STUCK again (and re-queued) instead of triggering automatic fixes.
func (o *Orchestrator) Execute(ctx context.Context, id string, a Action) (*Saga, error) {
	a.Type = strings.TrimSpace(a.Type)
	if a.Actor == "" {
		a.Actor = "unknown"
	}
	sg, err := o.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := validate(sg, a); err != nil {
		return nil, err
	}
	if sg.Status != StatusStuck {
		return nil, ErrNotActionable
	}

	// Claim: only one action at a time may act on a saga.
	sg, err = o.store.Claim(ctx, id, "", StatusStuck)
	if err != nil {
		return nil, err
	}
	record := HistoryEntry{At: time.Now().UTC(), Step: sg.CurrentStep, Outcome: "ok",
		Action: a.Type, Actor: a.Actor, Reason: a.Reason}

	runCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.config().RunLimit)
	defer cancel()

	switch a.Type {
	case ActionEscalate:
		err = o.advance(runCtx, id, sg.CurrentStep, StatusEscalated, record)

	case ActionRetryStep:
		if err = o.store.Advance(runCtx, id, sg.CurrentStep, StatusRunning, record); err == nil {
			err = o.run(runCtx, id, PolicyEscalate)
		}

	case ActionReroute:
		err = o.reroute(runCtx, sg, a, record)

	case ActionCompensate:
		err = o.compensate(runCtx, sg, record)

	case ActionPartialRefund:
		cfg := o.config()
		entries, perr := o.attempt(runCtx, StepRefundSender, cfg.Retries, cfg.Backoff, func() error {
			_, _, err := o.wallets.Credit(runCtx, sg.FromWallet, a.Amount, partialRefundKey(sg.ID))
			return err
		})
		entries = append([]HistoryEntry{record}, entries...)
		if perr == nil {
			err = o.advance(runCtx, id, StepDone, StatusCompensated, entries...)
		} else {
			err = o.advance(runCtx, id, sg.CurrentStep, StatusStuck, entries...)
		}
	}
	if err != nil {
		// Never leave the saga claimed as RUNNING by a failed action.
		o.advance(context.WithoutCancel(ctx), id, sg.CurrentStep, StatusStuck,
			entry(sg.CurrentStep, 0, fmt.Errorf("action %s failed: %w", a.Type, err)))
		return nil, err
	}
	return o.store.Get(ctx, id)
}

func validate(sg *Saga, a Action) error {
	bad := func(msg string) error { return fmt.Errorf("%w: %s", ErrInvalidAction, msg) }
	switch a.Type {
	case ActionRetryStep:
		if sg.CurrentStep == StepReconcile || sg.CurrentStep == StepDone {
			return bad("nothing to retry at step " + sg.CurrentStep)
		}
	case ActionReroute:
		if sg.CurrentStep != StepCallRail {
			return bad("reroute only applies when stuck at CALL_RAIL")
		}
	case ActionPartialRefund:
		if a.Amount <= 0 || a.Amount > sg.Amount {
			return bad("amount_paise must be between 1 and the transfer amount")
		}
	case ActionCompensate, ActionEscalate:
	default:
		return bad(fmt.Sprintf("unknown action %q (use one of %s)", a.Type, strings.Join(allActions, ", ")))
	}
	if len(a.Reason) > 2000 {
		return bad("reason too long")
	}
	return nil
}

// reroute pays again under a new rail payment id. If the original payment
// actually went through, this pays TWICE: the invariant check reports it.
func (o *Orchestrator) reroute(ctx context.Context, sg *Saga, a Action, record HistoryEntry) error {
	ref := sg.ID + ":reroute:" + strconv.Itoa(sg.rerouteCount()+1)
	rail := a.Rail
	if rail == "" {
		rail = "backup"
	}
	payErr := o.rail.Pay(ctx, PayRequest{SagaID: ref, From: sg.FromWallet, To: sg.ToWallet, Amount: sg.Amount})
	attempt := entry(StepCallRail, 1, payErr)
	attempt.RailRef = ref
	attempt.Reason = "rerouted via " + rail
	if payErr != nil {
		return o.advance(ctx, sg.ID, StepCallRail, StatusStuck, record, attempt)
	}
	if err := o.store.Advance(ctx, sg.ID, StepCreditReceiver, StatusRunning, record, attempt); err != nil {
		return err
	}
	return o.run(ctx, sg.ID, PolicyEscalate)
}

// compensate undoes whatever actually happened in the ledger:
// take back the receiver's credit (if any), then refund the sender (if debited).
func (o *Orchestrator) compensate(ctx context.Context, sg *Saga, record HistoryEntry) error {
	ms, err := o.MoneyState(ctx, sg)
	if err != nil {
		return err
	}
	cfg := o.config()
	entries := []HistoryEntry{record}

	if ms.Credited && !ms.Reversed {
		e, err := o.attempt(ctx, StepReconcile, cfg.Retries, cfg.Backoff, func() error {
			_, _, err := o.wallets.Debit(ctx, sg.ToWallet, sg.Amount, reverseKey(sg.ID))
			return err
		})
		entries = append(entries, e...)
		if err != nil { // e.g. receiver already spent the money
			return o.advance(ctx, sg.ID, sg.CurrentStep, StatusStuck, entries...)
		}
	}
	if !ms.Debited {
		return o.advance(ctx, sg.ID, StepDone, StatusFailed, entries...) // nothing ever left the sender
	}
	if !ms.Refunded {
		e, err := o.attempt(ctx, StepRefundSender, cfg.Retries, cfg.Backoff, func() error {
			_, _, err := o.wallets.Credit(ctx, sg.FromWallet, sg.Amount, refundKey(sg.ID))
			return err
		})
		entries = append(entries, e...)
		if err != nil {
			return o.advance(ctx, sg.ID, StepRefundSender, StatusStuck, entries...)
		}
	}
	return o.advance(ctx, sg.ID, StepDone, StatusCompensated, entries...)
}

// Reconcile looks for COMPLETED sagas whose rail payment was later reversed
// ("partial" faults). Each one is moved to STUCK at step RECONCILE and queued.
func (o *Orchestrator) Reconcile(ctx context.Context, since time.Time) ([]string, error) {
	status, ok := o.rail.(RailStatus)
	if !ok {
		return nil, nil
	}
	sagas, err := o.store.List(ctx, StatusCompleted, since, 0)
	if err != nil {
		return nil, err
	}
	var flagged []string
	for _, sg := range sagas {
		paid := false
		reversed := false
		for _, ref := range sg.RailRefs() {
			st, err := status.Status(ctx, ref)
			if err != nil {
				return flagged, err
			}
			paid = paid || st == "PAID"
			reversed = reversed || st == "REVERSED"
		}
		if paid || !reversed {
			continue
		}
		if _, err := o.store.Claim(ctx, sg.ID, StepReconcile, StatusCompleted); err != nil {
			continue // someone else changed it
		}
		note := HistoryEntry{At: time.Now().UTC(), Step: StepReconcile, Outcome: "error",
			Error: "rail reports the payment was REVERSED after the transfer completed"}
		if err := o.advance(ctx, sg.ID, StepReconcile, StatusStuck, note); err != nil {
			return flagged, err
		}
		flagged = append(flagged, sg.ID)
	}
	return flagged, nil
}
