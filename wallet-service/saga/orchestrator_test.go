package saga

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"ledgemind/wallet-service/domain"
	"ledgemind/wallet-service/eventstore"
	"ledgemind/wallet-service/internal/testdb"
	"ledgemind/wallet-service/ledger"
)

// ---------- fakes ----------

// fakeRail behaves like the real fake-rail: it remembers each payment's outcome.
type fakeRail struct {
	mu        sync.Mutex
	err       error  // error to return on failing calls
	failTimes int    // 0 with err set = fail forever; N = fail the next N calls
	failState string // what a failing call really did: "" (nothing), "PAID" or "REJECTED"
	calls     int
	states    map[string]string
}

func (r *fakeRail) Pay(_ context.Context, req PayRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.states == nil {
		r.states = map[string]string{}
	}
	if st, seen := r.states[req.SagaID]; seen { // repeat: stored outcome
		if st == "PAID" {
			return nil
		}
		return &RailError{Status: 422, Body: st}
	}
	if r.err != nil && r.failTimes >= 0 {
		if r.failTimes > 0 {
			r.failTimes--
			if r.failTimes == 0 {
				r.failTimes = -1 // used up
			}
		}
		if r.failState != "" {
			r.states[req.SagaID] = r.failState
		}
		return r.err
	}
	r.states[req.SagaID] = "PAID"
	return nil
}

func (r *fakeRail) Status(_ context.Context, ref string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if st, ok := r.states[ref]; ok {
		return st, nil
	}
	return RailStateNone, nil
}

func (r *fakeRail) heal() { r.mu.Lock(); r.err = nil; r.mu.Unlock() }

// fakeQueue records stuck notifications.
type fakeQueue struct {
	mu  sync.Mutex
	ids []string
}

func (q *fakeQueue) Stuck(_ context.Context, sg *Saga) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.ids = append(q.ids, sg.ID)
	return nil
}

func (q *fakeQueue) count(id string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	for _, x := range q.ids {
		if x == id {
			n++
		}
	}
	return n
}

// flakyWallets wraps the real ledger and fails chosen calls.
type flakyWallets struct {
	*ledger.Service
	mu            sync.Mutex
	failCreditsTo map[string]int // wallet id -> how many credits to fail (-1 = forever)
	creditCalls   map[string]int
}

func (f *flakyWallets) Credit(ctx context.Context, id string, amount int64, key string) (*domain.Wallet, bool, error) {
	f.mu.Lock()
	f.creditCalls[id]++
	left := f.failCreditsTo[id]
	if left != 0 {
		if left > 0 {
			f.failCreditsTo[id] = left - 1
		}
		f.mu.Unlock()
		return nil, false, errors.New("simulated outage")
	}
	f.mu.Unlock()
	return f.Service.Credit(ctx, id, amount, key)
}

// ---------- setup ----------

type env struct {
	ledger  *ledger.Service
	wallets *flakyWallets
	rail    *fakeRail
	orch    *Orchestrator
	store   *Store
	queue   *fakeQueue
}

func newEnv(t *testing.T) *env {
	pool := testdb.New(t)
	led := ledger.NewService(eventstore.New(pool), nil, 0)
	w := &flakyWallets{Service: led, failCreditsTo: map[string]int{}, creditCalls: map[string]int{}}
	rail := &fakeRail{}
	store := NewStore(pool)
	cfg := Config{Policy: PolicyRefund, Retries: 3, Backoff: time.Millisecond,
		RailRetries: 3, RailBackoff: time.Millisecond, RunLimit: 10 * time.Second}
	orch := NewOrchestrator(store, w, rail, cfg)
	q := &fakeQueue{}
	orch.SetNotifier(q)
	return &env{led, w, rail, orch, store, q}
}

func (e *env) wallet(t *testing.T, balance int64) string {
	t.Helper()
	ctx := context.Background()
	w, err := e.ledger.OpenWallet(ctx, "t")
	if err != nil {
		t.Fatal(err)
	}
	if balance > 0 {
		if _, _, err := e.ledger.Credit(ctx, w.ID, balance, "seed"); err != nil {
			t.Fatal(err)
		}
	}
	return w.ID
}

func (e *env) balance(t *testing.T, id string) int64 {
	t.Helper()
	w, err := e.ledger.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return w.Balance
}

func (e *env) transfer(t *testing.T, from, to string, amount int64) *Saga {
	t.Helper()
	sg, created, err := e.orch.Start(context.Background(), TransferRequest{From: from, To: to, Amount: amount, Key: uuid.NewString()})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if !created {
		t.Fatal("expected a new saga")
	}
	return sg
}

func expectState(t *testing.T, sg *Saga, step, status string) {
	t.Helper()
	if sg.CurrentStep != step || sg.Status != status {
		t.Fatalf("saga at %s/%s, want %s/%s; history: %+v", sg.CurrentStep, sg.Status, step, status, sg.History)
	}
}

// ---------- tests ----------

func TestTransferHappyPath(t *testing.T) {
	e := newEnv(t)
	a, b := e.wallet(t, 50_000), e.wallet(t, 0)

	sg := e.transfer(t, a, b, 20_000)
	expectState(t, sg, StepDone, StatusCompleted)
	if e.balance(t, a) != 30_000 || e.balance(t, b) != 20_000 {
		t.Fatalf("balances a=%d b=%d", e.balance(t, a), e.balance(t, b))
	}
	if len(sg.History) != 3 || e.rail.calls != 1 {
		t.Fatalf("history=%d rail calls=%d, want 3 and 1", len(sg.History), e.rail.calls)
	}
}

func TestInsufficientFundsFailsWithoutMovingMoney(t *testing.T) {
	e := newEnv(t)
	a, b := e.wallet(t, 1_000), e.wallet(t, 0)

	sg := e.transfer(t, a, b, 5_000)
	expectState(t, sg, StepDone, StatusFailed)
	if e.balance(t, a) != 1_000 || e.balance(t, b) != 0 || e.rail.calls != 0 {
		t.Fatal("money moved or rail was called on a failed debit")
	}
	if len(sg.History) != 1 {
		t.Fatalf("insufficient funds should not be retried; history = %+v", sg.History)
	}
}

func TestRailFailureRefundsSender(t *testing.T) {
	e := newEnv(t)
	a, b := e.wallet(t, 10_000), e.wallet(t, 0)
	e.rail.err = &RailError{Status: 502, Body: "rail down"}

	sg := e.transfer(t, a, b, 4_000)
	expectState(t, sg, StepDone, StatusCompensated)
	if e.balance(t, a) != 10_000 || e.balance(t, b) != 0 {
		t.Fatalf("after refund a=%d b=%d, want 10000 and 0", e.balance(t, a), e.balance(t, b))
	}
}

func TestCreditRetriesThenSucceeds(t *testing.T) {
	e := newEnv(t)
	a, b := e.wallet(t, 10_000), e.wallet(t, 0)
	e.wallets.failCreditsTo[b] = 2 // fails twice, third attempt works

	sg := e.transfer(t, a, b, 3_000)
	expectState(t, sg, StepDone, StatusCompleted)
	if e.balance(t, b) != 3_000 || e.wallets.creditCalls[b] != 3 {
		t.Fatalf("b=%d credit calls=%d, want 3000 and 3", e.balance(t, b), e.wallets.creditCalls[b])
	}
}

func TestCreditFailsFourTimesThenRefund(t *testing.T) {
	e := newEnv(t)
	a, b := e.wallet(t, 10_000), e.wallet(t, 0)
	e.wallets.failCreditsTo[b] = -1 // receiver never accepts

	sg := e.transfer(t, a, b, 3_000)
	expectState(t, sg, StepDone, StatusCompensated)
	if e.wallets.creditCalls[b] != 4 {
		t.Fatalf("credit attempts = %d, want 4 (1 + 3 retries)", e.wallets.creditCalls[b])
	}
	if e.balance(t, a) != 10_000 || e.balance(t, b) != 0 {
		t.Fatalf("a=%d b=%d, want 10000 and 0", e.balance(t, a), e.balance(t, b))
	}
}

func TestRefundFailureLeavesSagaStuck(t *testing.T) {
	e := newEnv(t)
	a, b := e.wallet(t, 10_000), e.wallet(t, 0)
	e.rail.err = errors.New("timeout")
	e.wallets.failCreditsTo[a] = -1 // refunds to the sender fail too

	sg := e.transfer(t, a, b, 2_000)
	expectState(t, sg, StepRefundSender, StatusStuck)
	if e.balance(t, a) != 8_000 {
		t.Fatalf("sender = %d, want 8000 (money stuck in flight)", e.balance(t, a))
	}

	// The stuck saga can be finished later (Day 7: by the recovery service).
	e.wallets.failCreditsTo[a] = 0
	if err := e.store.Advance(context.Background(), sg.ID, StepRefundSender, StatusRunning); err != nil {
		t.Fatal(err)
	}
	if err := e.orch.Run(context.Background(), sg.ID); err != nil {
		t.Fatal(err)
	}
	sg, _ = e.orch.Get(context.Background(), sg.ID)
	expectState(t, sg, StepDone, StatusCompensated)
	if e.balance(t, a) != 10_000 {
		t.Fatalf("sender = %d after recovery, want 10000", e.balance(t, a))
	}
}

func TestSameKeyTwiceMovesMoneyOnce(t *testing.T) {
	e := newEnv(t)
	a, b := e.wallet(t, 10_000), e.wallet(t, 0)
	req := TransferRequest{From: a, To: b, Amount: 1_000, Key: "tx-" + uuid.NewString()}
	ctx := context.Background()

	first, created, err := e.orch.Start(ctx, req)
	if err != nil || !created {
		t.Fatalf("first: created=%v err=%v", created, err)
	}
	second, created, err := e.orch.Start(ctx, req)
	if err != nil || created || second.ID != first.ID {
		t.Fatalf("second: created=%v err=%v id match=%v", created, err, second.ID == first.ID)
	}
	if e.balance(t, a) != 9_000 || e.balance(t, b) != 1_000 {
		t.Fatalf("a=%d b=%d", e.balance(t, a), e.balance(t, b))
	}

	req.Amount = 2_000
	if _, _, err := e.orch.Start(ctx, req); !errors.Is(err, ErrIdempotencyMismatch) {
		t.Fatalf("reused key with new amount: err = %v", err)
	}
}

func TestInvalidRequests(t *testing.T) {
	e := newEnv(t)
	a := e.wallet(t, 1_000)
	ctx := context.Background()
	cases := []struct {
		req  TransferRequest
		want error
	}{
		{TransferRequest{a, a, 100, "k1"}, ErrInvalidTransfer},
		{TransferRequest{a, uuid.NewString(), 100, "k2"}, domain.ErrWalletNotFound},
		{TransferRequest{a, uuid.NewString(), 0, "k3"}, domain.ErrInvalidAmount},
		{TransferRequest{a, uuid.NewString(), 100, ""}, domain.ErrMissingKey},
	}
	for _, c := range cases {
		if _, _, err := e.orch.Start(ctx, c.req); !errors.Is(err, c.want) {
			t.Errorf("%+v: err = %v, want %v", c.req, err, c.want)
		}
	}
}

// Simulates a crash after the debit: the saga row says CALL_RAIL, RUNNING.
// ResumeRunning must finish it without debiting twice.
func TestResumeAfterCrash(t *testing.T) {
	e := newEnv(t)
	a, b := e.wallet(t, 10_000), e.wallet(t, 0)
	ctx := context.Background()

	id := uuid.NewString()
	if _, _, err := e.store.Create(ctx, &Saga{ID: id, IdempotencyKey: "crash-" + id, FromWallet: a, ToWallet: b,
		Amount: 2_500, CurrentStep: StepDebitSender, Status: StatusRunning}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.ledger.Debit(ctx, a, 2_500, debitKey(id)); err != nil { // the debit that happened before the crash
		t.Fatal(err)
	}
	// Row still says DEBIT_SENDER: the crash hit before the row was updated.
	// (Run on this one saga = what ResumeRunning does for every RUNNING saga at startup;
	// calling ResumeRunning here would also grab other test packages' sagas.)
	if err := e.orch.Run(ctx, id); err != nil {
		t.Fatal(err)
	}
	sg, _ := e.orch.Get(ctx, id)
	expectState(t, sg, StepDone, StatusCompleted)
	if e.balance(t, a) != 7_500 || e.balance(t, b) != 2_500 {
		t.Fatalf("a=%d b=%d, want 7500 and 2500 (debited once)", e.balance(t, a), e.balance(t, b))
	}
}
