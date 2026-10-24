package saga

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func (e *env) transferWith(t *testing.T, policy, from, to string, amount int64) *Saga {
	t.Helper()
	if err := e.orch.SetPolicy(policy); err != nil {
		t.Fatal(err)
	}
	return e.transfer(t, from, to, amount)
}

// stuck creates a transfer stuck at CALL_RAIL (policy escalate). failState says
// what the rail really did: "" transient (nothing), "PAID" or "REJECTED".
func (e *env) stuck(t *testing.T, failState string) (sg *Saga, from, to string) {
	t.Helper()
	from, to = e.wallet(t, 10_000), e.wallet(t, 0)
	e.rail.err, e.rail.failTimes, e.rail.failState = errors.New("rail exploded"), 1, failState
	sg = e.transferWith(t, PolicyEscalate, from, to, 1_000)
	expectState(t, sg, StepCallRail, StatusStuck)
	return sg, from, to
}

func act(t *testing.T, e *env, id string, a Action) *Saga {
	t.Helper()
	sg, err := e.orch.Execute(context.Background(), id, a)
	if err != nil {
		t.Fatalf("execute %s: %v", a.Type, err)
	}
	return sg
}

// ---------- policies ----------

func TestRetryPolicyRecoversFromTransientErrors(t *testing.T) {
	e := newEnv(t)
	a, b := e.wallet(t, 10_000), e.wallet(t, 0)
	e.rail.err, e.rail.failTimes = errors.New("503"), 2 // nothing recorded: transient

	sg := e.transferWith(t, PolicyRetry, a, b, 1_000)
	expectState(t, sg, StepDone, StatusCompleted)
	if e.rail.calls != 3 || sg.Policy != PolicyRetry {
		t.Fatalf("rail calls = %d policy = %s, want 3 and retry", e.rail.calls, sg.Policy)
	}
}

func TestRetryPolicyFixesATimeoutThatActuallyPaid(t *testing.T) {
	e := newEnv(t)
	a, b := e.wallet(t, 10_000), e.wallet(t, 0)
	e.rail.err, e.rail.failTimes, e.rail.failState = errors.New("timeout"), 1, "PAID"

	sg := e.transferWith(t, PolicyRetry, a, b, 1_000)
	expectState(t, sg, StepDone, StatusCompleted) // the retry sees the stored PAID
	if e.balance(t, b) != 1_000 {
		t.Fatalf("receiver = %d", e.balance(t, b))
	}
}

func TestRetryPolicyGivesUpAndQueues(t *testing.T) {
	e := newEnv(t)
	a, b := e.wallet(t, 10_000), e.wallet(t, 0)
	e.rail.err, e.rail.failState = &RailError{Status: 422, Body: "limit"}, "REJECTED"

	sg := e.transferWith(t, PolicyRetry, a, b, 1_000)
	expectState(t, sg, StepCallRail, StatusStuck)
	if e.rail.calls != 4 {
		t.Fatalf("rail calls = %d, want 4 (1 + 3 retries)", e.rail.calls)
	}
	if e.queue.count(sg.ID) != 1 {
		t.Fatalf("queued %d times, want 1", e.queue.count(sg.ID))
	}
	if e.balance(t, a) != 9_000 {
		t.Fatalf("sender = %d, want 9000 (money held while stuck)", e.balance(t, a))
	}
}

func TestEscalatePolicyStopsAtOnce(t *testing.T) {
	e := newEnv(t)
	sg, _, _ := e.stuck(t, "")
	if e.rail.calls != 1 || e.queue.count(sg.ID) != 1 {
		t.Fatalf("rail calls=%d queued=%d, want 1 and 1", e.rail.calls, e.queue.count(sg.ID))
	}
	if sg.LastError() == "" {
		t.Fatal("stuck saga should carry its last error")
	}
}

func TestEscalatePolicyCreditFailureGetsStuck(t *testing.T) {
	e := newEnv(t)
	a, b := e.wallet(t, 10_000), e.wallet(t, 0)
	e.wallets.failCreditsTo[b] = -1
	sg := e.transferWith(t, PolicyEscalate, a, b, 1_000)
	expectState(t, sg, StepCreditReceiver, StatusStuck)
}

func TestSetPolicyRejectsUnknown(t *testing.T) {
	e := newEnv(t)
	if err := e.orch.SetPolicy("yolo"); !errors.Is(err, ErrInvalidAction) {
		t.Fatalf("err = %v", err)
	}
}

// ---------- executor ----------

func TestExecutorRetryStep(t *testing.T) {
	e := newEnv(t)
	sg, _, to := e.stuck(t, "")
	e.rail.heal()

	sg = act(t, e, sg.ID, Action{Type: ActionRetryStep, Actor: "ai", Reason: "transient"})
	expectState(t, sg, StepDone, StatusCompleted)
	if e.balance(t, to) != 1_000 {
		t.Fatalf("receiver = %d", e.balance(t, to))
	}
	found := false
	for _, h := range sg.History {
		if h.Action == ActionRetryStep && h.Actor == "ai" && h.Reason == "transient" {
			found = true
		}
	}
	if !found {
		t.Fatalf("action not recorded in history: %+v", sg.History)
	}
}

func TestExecutorRetryStepFailsAgainAndRequeues(t *testing.T) {
	e := newEnv(t)
	sg, _, _ := e.stuck(t, "REJECTED")
	sg = act(t, e, sg.ID, Action{Type: ActionRetryStep})
	expectState(t, sg, StepCallRail, StatusStuck)
	if e.queue.count(sg.ID) != 2 {
		t.Fatalf("queued %d times, want 2", e.queue.count(sg.ID))
	}
}

func TestExecutorCompensate(t *testing.T) {
	e := newEnv(t)
	sg, from, to := e.stuck(t, "REJECTED")
	sg = act(t, e, sg.ID, Action{Type: ActionCompensate})
	expectState(t, sg, StepDone, StatusCompensated)
	if e.balance(t, from) != 10_000 || e.balance(t, to) != 0 {
		t.Fatalf("from=%d to=%d", e.balance(t, from), e.balance(t, to))
	}
}

func TestExecutorReroute(t *testing.T) {
	e := newEnv(t)
	sg, _, to := e.stuck(t, "REJECTED")
	sg = act(t, e, sg.ID, Action{Type: ActionReroute, Rail: "backup"})
	expectState(t, sg, StepDone, StatusCompleted)
	refs := sg.RailRefs()
	if len(refs) != 2 || refs[1] != sg.ID+":reroute:1" {
		t.Fatalf("rail refs = %v", refs)
	}
	if st, _ := e.rail.Status(context.Background(), refs[1]); st != "PAID" || e.balance(t, to) != 1_000 {
		t.Fatalf("reroute state %s, receiver %d", st, e.balance(t, to))
	}
}

func TestExecutorPartialRefund(t *testing.T) {
	e := newEnv(t)
	sg, from, _ := e.stuck(t, "")
	sg = act(t, e, sg.ID, Action{Type: ActionPartialRefund, Amount: 400})
	expectState(t, sg, StepDone, StatusCompensated)
	if e.balance(t, from) != 9_400 {
		t.Fatalf("sender = %d, want 9400", e.balance(t, from))
	}
}

func TestExecutorEscalate(t *testing.T) {
	e := newEnv(t)
	sg, _, _ := e.stuck(t, "")
	sg = act(t, e, sg.ID, Action{Type: ActionEscalate, Reason: "unsure"})
	expectState(t, sg, StepCallRail, StatusEscalated)
}

func TestExecutorRejectsBadActions(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	sg, _, _ := e.stuck(t, "")

	cases := []struct {
		a    Action
		want error
	}{
		{Action{Type: "delete_everything"}, ErrInvalidAction},
		{Action{Type: ActionPartialRefund, Amount: 0}, ErrInvalidAction},
		{Action{Type: ActionPartialRefund, Amount: 999_999}, ErrInvalidAction},
	}
	for _, c := range cases {
		if _, err := e.orch.Execute(ctx, sg.ID, c.a); !errors.Is(err, c.want) {
			t.Errorf("%+v: err = %v, want %v", c.a, err, c.want)
		}
	}
	if _, err := e.orch.Execute(ctx, uuid.NewString(), Action{Type: ActionEscalate}); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing saga: err = %v", err)
	}

	// A finished saga cannot be acted on.
	act(t, e, sg.ID, Action{Type: ActionCompensate})
	if _, err := e.orch.Execute(ctx, sg.ID, Action{Type: ActionCompensate}); !errors.Is(err, ErrNotActionable) {
		t.Errorf("second action: err = %v, want ErrNotActionable", err)
	}
}

func TestExecutorOnlyOneConcurrentActionWins(t *testing.T) {
	e := newEnv(t)
	sg, from, _ := e.stuck(t, "")
	var wg sync.WaitGroup
	errs := make([]error, 5)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = e.orch.Execute(context.Background(), sg.ID, Action{Type: ActionCompensate})
		}(i)
	}
	wg.Wait()
	ok := 0
	for _, err := range errs {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, ErrNotActionable):
		default:
			t.Fatalf("unexpected: %v", err)
		}
	}
	if ok != 1 || e.balance(t, from) != 10_000 {
		t.Fatalf("winners=%d sender=%d, want 1 and 10000", ok, e.balance(t, from))
	}
}

// A completed transfer whose rail payment is reversed later ("partial" fault).
func TestReconcileFlagsReversedPaymentAndCompensateUndoesIt(t *testing.T) {
	e := newEnv(t)
	a, b := e.wallet(t, 10_000), e.wallet(t, 0)
	sg := e.transferWith(t, PolicyEscalate, a, b, 1_000)
	expectState(t, sg, StepDone, StatusCompleted)

	e.rail.mu.Lock()
	e.rail.states[sg.ID] = "REVERSED" // the receiving bank sent it back
	e.rail.mu.Unlock()

	flagged, err := e.orch.Reconcile(context.Background(), sg.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(flagged) != 1 || flagged[0] != sg.ID {
		t.Fatalf("flagged = %v", flagged)
	}
	sg, _ = e.orch.Get(context.Background(), sg.ID)
	expectState(t, sg, StepReconcile, StatusStuck)
	if e.queue.count(sg.ID) != 1 {
		t.Fatal("reconciled saga should be queued")
	}

	sg = act(t, e, sg.ID, Action{Type: ActionCompensate})
	expectState(t, sg, StepDone, StatusCompensated)
	if e.balance(t, a) != 10_000 || e.balance(t, b) != 0 {
		t.Fatalf("a=%d b=%d, want 10000 and 0", e.balance(t, a), e.balance(t, b))
	}
	ms, _ := e.orch.MoneyState(context.Background(), sg)
	if !ms.Debited || !ms.Credited || !ms.Reversed || !ms.Refunded {
		t.Fatalf("money state = %+v", ms)
	}
}
