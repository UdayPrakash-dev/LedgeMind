package audit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"ledgemind/wallet-service/eventstore"
	"ledgemind/wallet-service/internal/testdb"
	"ledgemind/wallet-service/ledger"
	"ledgemind/wallet-service/saga"
)

// rail remembers outcomes like the real fake-rail. failNext makes the next
// new payment fail while really ending in state failState.
type rail struct {
	mu        sync.Mutex
	states    map[string]string
	failNext  bool
	failState string
}

func (r *rail) Pay(_ context.Context, req saga.PayRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if st, ok := r.states[req.SagaID]; ok {
		if st == "PAID" {
			return nil
		}
		return errors.New(st)
	}
	if r.failNext {
		r.failNext = false
		r.states[req.SagaID] = r.failState
		return errors.New("rail error")
	}
	r.states[req.SagaID] = "PAID"
	return nil
}

func (r *rail) Status(_ context.Context, ref string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if st, ok := r.states[ref]; ok {
		return st, nil
	}
	return saga.RailStateNone, nil
}

func (r *rail) set(ref, st string) { r.mu.Lock(); r.states[ref] = st; r.mu.Unlock() }
func (r *rail) failOnce(st string) { r.mu.Lock(); r.failNext, r.failState = true, st; r.mu.Unlock() }

type env struct {
	led  *ledger.Service
	orch *saga.Orchestrator
	aud  *Auditor
	rail *rail
}

func newEnv(t *testing.T) *env {
	pool := testdb.New(t)
	led := ledger.NewService(eventstore.New(pool), nil, 0)
	r := &rail{states: map[string]string{}}
	store := saga.NewStore(pool)
	cfg := saga.Config{Policy: saga.PolicyRefund, Retries: 1, Backoff: time.Millisecond,
		RailRetries: 1, RailBackoff: time.Millisecond, RunLimit: 10 * time.Second}
	return &env{led, saga.NewOrchestrator(store, led, r, cfg), New(pool, store, led, r), r}
}

func (e *env) transfer(t *testing.T, policy string) *saga.Saga {
	t.Helper()
	ctx := context.Background()
	a, _ := e.led.OpenWallet(ctx, "a")
	b, _ := e.led.OpenWallet(ctx, "b")
	if _, _, err := e.led.Credit(ctx, a.ID, 10_000, "seed"); err != nil {
		t.Fatal(err)
	}
	e.orch.SetPolicy(policy)
	sg, _, err := e.orch.Start(ctx, saga.TransferRequest{From: a.ID, To: b.ID, Amount: 1_000, Key: uuid.NewString()})
	if err != nil {
		t.Fatal(err)
	}
	return sg
}

func (e *env) check(t *testing.T, sagaID string) *Report {
	t.Helper()
	r, err := e.aud.Check(context.Background(), Options{SagaID: sagaID, CheckRail: true})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func expectKinds(t *testing.T, r *Report, kinds ...string) {
	t.Helper()
	if len(kinds) == 0 {
		if !r.OK {
			t.Fatalf("want clean report, got %+v", r.Violations)
		}
		return
	}
	for _, k := range kinds {
		if r.ByKind[k] == 0 {
			t.Fatalf("want violation %s, got %+v", k, r.Violations)
		}
	}
}

func TestCleanTransferPasses(t *testing.T) {
	e := newEnv(t)
	sg := e.transfer(t, saga.PolicyRefund)
	expectKinds(t, e.check(t, sg.ID))
}

func TestCorrectRefundAfterRejectPasses(t *testing.T) {
	e := newEnv(t)
	e.rail.failOnce("REJECTED")
	sg := e.transfer(t, saga.PolicyRefund)
	if sg.Status != saga.StatusCompensated {
		t.Fatalf("status %s", sg.Status)
	}
	expectKinds(t, e.check(t, sg.ID))
}

// The Day 4 finding: fixed rules refund after a timeout, but the rail really paid.
func TestRefundAfterPaidTimeoutIsCaught(t *testing.T) {
	e := newEnv(t)
	e.rail.failOnce("PAID")
	sg := e.transfer(t, saga.PolicyRefund)
	expectKinds(t, e.check(t, sg.ID), "rail_paid_but_refunded")
}

func TestStuckSagaWithMoneyInFlightIsNotAViolation(t *testing.T) {
	e := newEnv(t)
	e.rail.failOnce("PAID")
	sg := e.transfer(t, saga.PolicyEscalate)
	r := e.check(t, sg.ID)
	expectKinds(t, r)
	if r.InFlight != 1_000 {
		t.Fatalf("in flight = %d, want 1000", r.InFlight)
	}
}

func TestRerouteAfterPaidIsDoublePayment(t *testing.T) {
	e := newEnv(t)
	e.rail.failOnce("PAID")
	sg := e.transfer(t, saga.PolicyEscalate)
	if _, err := e.orch.Execute(context.Background(), sg.ID, saga.Action{Type: saga.ActionReroute}); err != nil {
		t.Fatal(err)
	}
	expectKinds(t, e.check(t, sg.ID), "double_rail_payment")
}

func TestReversedButDeliveredIsCaught(t *testing.T) {
	e := newEnv(t)
	sg := e.transfer(t, saga.PolicyRefund)
	e.rail.set(sg.ID, "REVERSED")
	expectKinds(t, e.check(t, sg.ID), "delivered_but_rail_not_paid")
}

func TestMoneyCreatedIsCaught(t *testing.T) {
	e := newEnv(t)
	e.rail.failOnce("REJECTED")
	sg := e.transfer(t, saga.PolicyRefund) // refunded once, correctly
	// A buggy fixer refunds again under a different key.
	if _, _, err := e.led.Credit(context.Background(), sg.FromWallet, 1_000, "saga:"+sg.ID+":partial-refund"); err != nil {
		t.Fatal(err)
	}
	expectKinds(t, e.check(t, sg.ID), "money_created", "over_delivery", "unsettled_terminal_saga")
}

func TestGlobalReportIdentityHolds(t *testing.T) {
	e := newEnv(t)
	e.transfer(t, saga.PolicyRefund)
	r, err := e.aud.Check(context.Background(), Options{CheckRail: false})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"conservation", "snapshot_mismatch", "negative_balance"} {
		if r.ByKind[k] != 0 {
			t.Fatalf("global check found %s: %+v", k, r.Violations)
		}
	}
	if r.TotalBalance != r.ExternalNet-r.InFlight {
		t.Fatalf("identity broken: %d != %d - %d", r.TotalBalance, r.ExternalNet, r.InFlight)
	}
}

// Regression: the audit must not raise false alarms while money is moving.
// (Found when parallel test packages changed wallets between the event read
// and the wallet load.)
func TestCheckDuringConcurrentTrafficHasNoFalseAlarms(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	a, _ := e.led.OpenWallet(ctx, "busy-a")
	b, _ := e.led.OpenWallet(ctx, "busy-b")
	if _, _, err := e.led.Credit(ctx, a.ID, 1_000_000, "seed"); err != nil {
		t.Fatal(err)
	}
	e.orch.SetPolicy(saga.PolicyRefund)

	mine := map[string]bool{}
	var mu sync.Mutex
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 40; i++ {
			sg, _, err := e.orch.Start(ctx, saga.TransferRequest{From: a.ID, To: b.ID, Amount: 100, Key: uuid.NewString()})
			if err == nil {
				mu.Lock()
				mine[sg.ID] = true
				mu.Unlock()
			}
			e.led.Credit(ctx, b.ID, 7, uuid.NewString()) // unrelated deposits too
		}
	}()

	checks := 0
	for running := true; running; checks++ {
		select {
		case <-done:
			running = false
		default:
		}
		r, err := e.aud.Check(ctx, Options{CheckRail: false})
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range r.Violations {
			mu.Lock()
			ours := mine[v.SagaID]
			mu.Unlock()
			if v.Kind == "snapshot_mismatch" || v.Kind == "conservation" || v.Kind == "negative_balance" || ours {
				t.Fatalf("false alarm during traffic (check %d): %+v", checks, v)
			}
		}
	}
	if checks < 2 {
		t.Logf("only %d checks overlapped the traffic", checks)
	}
}
