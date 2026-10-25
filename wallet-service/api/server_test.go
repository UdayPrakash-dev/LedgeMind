package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"ledgemind/wallet-service/audit"
	"ledgemind/wallet-service/eventstore"
	"ledgemind/wallet-service/internal/testdb"
	"ledgemind/wallet-service/ledger"
	"ledgemind/wallet-service/recoverylog"
	"ledgemind/wallet-service/saga"
)

// End-to-end tests: real HTTP handlers + real Postgres. Needs DATABASE_URL.
func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	pool := testdb.New(t)
	led := ledger.NewService(eventstore.New(pool), nil, 0)
	cfg := saga.Config{Retries: 3, Backoff: time.Millisecond, RunLimit: 10 * time.Second}
	orch := saga.NewOrchestrator(saga.NewStore(pool), led, saga.AlwaysOKRail{}, cfg)
	srv := httptest.NewServer(NewServer(led, orch, audit.New(pool, saga.NewStore(pool), led, saga.AlwaysOKRail{}), nil))
	t.Cleanup(srv.Close)
	return srv
}

type result struct {
	Status   int
	Replayed bool
	Body     map[string]any
}

func call(t *testing.T, srv *httptest.Server, method, path, key, body string) result {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set(IdempotencyHeader, key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return result{resp.StatusCode, resp.Header.Get("Idempotent-Replayed") == "true", out}
}

func balance(r result) int64 { v, _ := r.Body["balance_paise"].(float64); return int64(v) }

func expect(t *testing.T, r result, status int, what string) {
	t.Helper()
	if r.Status != status {
		t.Fatalf("%s: status %d, want %d (body %v)", what, r.Status, status, r.Body)
	}
}

func TestWalletLifecycleEndToEnd(t *testing.T) {
	srv := newTestServer(t)

	// Open
	r := call(t, srv, "POST", "/wallets", "", `{"owner":"uday"}`)
	expect(t, r, 201, "open")
	id := r.Body["id"].(string)
	if balance(r) != 0 {
		t.Fatalf("new wallet balance = %d", balance(r))
	}

	// Credit ₹500 = 50000 paise
	r = call(t, srv, "POST", "/wallets/"+id+"/credit", "c-1", `{"amount_paise":50000}`)
	expect(t, r, 200, "credit")
	if balance(r) != 50_000 || r.Body["applied"] != true {
		t.Fatalf("after credit: %v", r.Body)
	}

	// Same key again -> applied once only
	r = call(t, srv, "POST", "/wallets/"+id+"/credit", "c-1", `{"amount_paise":50000}`)
	expect(t, r, 200, "repeat credit")
	if balance(r) != 50_000 || r.Body["applied"] != false || !r.Replayed {
		t.Fatalf("repeat credit changed state: %v", r.Body)
	}

	// Same key, different amount -> 409
	r = call(t, srv, "POST", "/wallets/"+id+"/credit", "c-1", `{"amount_paise":99}`)
	expect(t, r, 409, "key reuse")

	// Debit ₹200
	r = call(t, srv, "POST", "/wallets/"+id+"/debit", "d-1", `{"amount_paise":20000}`)
	expect(t, r, 200, "debit")
	if balance(r) != 30_000 {
		t.Fatalf("after debit: %v", r.Body)
	}

	// Too much -> 422
	r = call(t, srv, "POST", "/wallets/"+id+"/debit", "d-2", `{"amount_paise":30001}`)
	expect(t, r, 422, "overdraw")

	// Bad inputs
	expect(t, call(t, srv, "POST", "/wallets/"+id+"/credit", "x", `{"amount_paise":0}`), 400, "zero amount")
	expect(t, call(t, srv, "POST", "/wallets/"+id+"/credit", "x", `{"amount_paise":-5}`), 400, "negative amount")
	expect(t, call(t, srv, "POST", "/wallets/"+id+"/credit", "x", `{"amount_paise":1.5}`), 400, "float amount")
	expect(t, call(t, srv, "POST", "/wallets/"+id+"/credit", "", `{"amount_paise":10}`), 400, "missing key")
	expect(t, call(t, srv, "POST", "/wallets", "", `{"owner":""}`), 400, "empty owner")

	// Unknown wallets
	expect(t, call(t, srv, "GET", "/wallets/not-a-uuid", "", ""), 404, "bad id")
	expect(t, call(t, srv, "GET", "/wallets/01928c4e-0000-7000-8000-000000000000", "", ""), 404, "missing wallet")

	// Final state survives a fresh load
	r = call(t, srv, "GET", "/wallets/"+id, "", "")
	expect(t, r, 200, "get")
	if balance(r) != 30_000 || r.Body["version"].(float64) != 3 {
		t.Fatalf("final: %v (want balance 30000, version 3)", r.Body)
	}
}

// 10 identical requests at once (like a client retrying aggressively):
// money must move exactly once.
func TestConcurrentDuplicateRequestsApplyOnce(t *testing.T) {
	srv := newTestServer(t)
	id := call(t, srv, "POST", "/wallets", "", `{"owner":"race"}`).Body["id"].(string)

	var wg sync.WaitGroup
	applied := make([]bool, 10)
	for i := range applied {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := call(t, srv, "POST", "/wallets/"+id+"/credit", "same-key", `{"amount_paise":1000}`)
			if r.Status != 200 {
				t.Errorf("request %d: status %d %v", i, r.Status, r.Body)
			}
			applied[i] = r.Body["applied"] == true
		}(i)
	}
	wg.Wait()

	n := 0
	for _, a := range applied {
		if a {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d requests reported applied, want exactly 1", n)
	}
	if b := balance(call(t, srv, "GET", "/wallets/"+id, "", "")); b != 1000 {
		t.Fatalf("balance = %d, want 1000", b)
	}
}

// 8 different debits at once on ₹50: only 5 of ₹10 can succeed, balance never negative.
func TestConcurrentDebitsNeverOverdraw(t *testing.T) {
	srv := newTestServer(t)
	id := call(t, srv, "POST", "/wallets", "", `{"owner":"race2"}`).Body["id"].(string)
	expect(t, call(t, srv, "POST", "/wallets/"+id+"/credit", "seed", `{"amount_paise":5000}`), 200, "seed")

	var wg sync.WaitGroup
	statuses := make([]int, 8)
	for i := range statuses {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			statuses[i] = call(t, srv, "POST", "/wallets/"+id+"/debit", fmt.Sprintf("d-%d", i), `{"amount_paise":1000}`).Status
		}(i)
	}
	wg.Wait()

	ok, rejected := 0, 0
	for _, s := range statuses {
		switch s {
		case 200:
			ok++
		case 422:
			rejected++
		default:
			t.Fatalf("unexpected status %d", s)
		}
	}
	if ok != 5 || rejected != 3 {
		t.Fatalf("ok=%d rejected=%d, want 5 and 3", ok, rejected)
	}
	if b := balance(call(t, srv, "GET", "/wallets/"+id, "", "")); b != 0 {
		t.Fatalf("balance = %d, want 0", b)
	}
}

func TestTransferEndToEnd(t *testing.T) {
	srv := newTestServer(t)
	a := call(t, srv, "POST", "/wallets", "", `{"owner":"alice"}`).Body["id"].(string)
	b := call(t, srv, "POST", "/wallets", "", `{"owner":"bob"}`).Body["id"].(string)
	expect(t, call(t, srv, "POST", "/wallets/"+a+"/credit", "seed", `{"amount_paise":50000}`), 200, "seed")

	k := func(name string) string { return name + "-" + a } // transfer keys are global: keep them unique per run
	body := fmt.Sprintf(`{"from_wallet":%q,"to_wallet":%q,"amount_paise":20000}`, a, b)
	r := call(t, srv, "POST", "/transfers", k("tx-1"), body)
	expect(t, r, 201, "transfer")
	if r.Body["status"] != "COMPLETED" || r.Body["current_step"] != "DONE" {
		t.Fatalf("transfer: %v", r.Body)
	}
	id := r.Body["id"].(string)

	// Repeat: same transfer, no new money movement
	r = call(t, srv, "POST", "/transfers", k("tx-1"), body)
	expect(t, r, 200, "repeat transfer")
	if r.Body["id"] != id || !r.Replayed {
		t.Fatalf("repeat returned %v", r.Body)
	}

	expect(t, call(t, srv, "GET", "/transfers/"+id, "", ""), 200, "get transfer")
	if balance(call(t, srv, "GET", "/wallets/"+a, "", "")) != 30_000 ||
		balance(call(t, srv, "GET", "/wallets/"+b, "", "")) != 20_000 {
		t.Fatal("wrong balances after transfer")
	}

	// Not enough money: transfer is created but FAILED, nothing moves
	r = call(t, srv, "POST", "/transfers", k("tx-2"), fmt.Sprintf(`{"from_wallet":%q,"to_wallet":%q,"amount_paise":999999}`, a, b))
	expect(t, r, 201, "big transfer")
	if r.Body["status"] != "FAILED" {
		t.Fatalf("big transfer: %v", r.Body)
	}

	// Bad requests
	expect(t, call(t, srv, "POST", "/transfers", k("tx-1"), fmt.Sprintf(`{"from_wallet":%q,"to_wallet":%q,"amount_paise":5}`, a, b)), 409, "key reuse")
	expect(t, call(t, srv, "POST", "/transfers", k("tx-3"), fmt.Sprintf(`{"from_wallet":%q,"to_wallet":%q,"amount_paise":5}`, a, a)), 400, "self transfer")
	expect(t, call(t, srv, "POST", "/transfers", "", body), 400, "missing key")
	expect(t, call(t, srv, "POST", "/transfers", k("tx-4"), `{"from_wallet":"nope","to_wallet":"nope2","amount_paise":5}`), 404, "bad wallet ids")
	expect(t, call(t, srv, "GET", "/transfers/01928c4e-0000-7000-8000-000000000000", "", ""), 404, "missing transfer")
}

// toggleRail fails while broken is true, and remembers outcomes like the real rail.
type toggleRail struct {
	mu     sync.Mutex
	broken bool
	states map[string]string
}

func (r *toggleRail) Pay(_ context.Context, req saga.PayRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.broken {
		return &saga.RailError{Status: 503, Body: "rail down"} // transient: nothing recorded
	}
	r.states[req.SagaID] = "PAID"
	return nil
}

func (r *toggleRail) Status(_ context.Context, ref string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if st, ok := r.states[ref]; ok {
		return st, nil
	}
	return saga.RailStateNone, nil
}

func TestRecoveryEndpoints(t *testing.T) {
	pool := testdb.New(t)
	led := ledger.NewService(eventstore.New(pool), nil, 0)
	rail := &toggleRail{broken: true, states: map[string]string{}}
	store := saga.NewStore(pool)
	cfg := saga.Config{Policy: saga.PolicyRefund, Retries: 1, Backoff: time.Millisecond,
		RailRetries: 1, RailBackoff: time.Millisecond, RunLimit: 10 * time.Second}
	orch := saga.NewOrchestrator(store, led, rail, cfg)
	srv := httptest.NewServer(NewServer(led, orch, audit.New(pool, store, led, rail), nil))
	defer srv.Close()

	// Switch new transfers to "escalate"
	expect(t, call(t, srv, "POST", "/admin/policy", "", `{"policy":"escalate"}`), 200, "set policy")
	expect(t, call(t, srv, "POST", "/admin/policy", "", `{"policy":"nope"}`), 400, "bad policy")
	if r := call(t, srv, "GET", "/admin/policy", "", ""); r.Body["policy"] != "escalate" {
		t.Fatalf("policy = %v", r.Body)
	}

	a := call(t, srv, "POST", "/wallets", "", `{"owner":"a"}`).Body["id"].(string)
	b := call(t, srv, "POST", "/wallets", "", `{"owner":"b"}`).Body["id"].(string)
	call(t, srv, "POST", "/wallets/"+a+"/credit", "seed", `{"amount_paise":5000}`)

	r := call(t, srv, "POST", "/transfers", "stuck-"+a, fmt.Sprintf(`{"from_wallet":%q,"to_wallet":%q,"amount_paise":1000}`, a, b))
	expect(t, r, 201, "transfer")
	if r.Body["status"] != "STUCK" || r.Body["policy"] != "escalate" {
		t.Fatalf("transfer: %v", r.Body)
	}
	id := r.Body["id"].(string)

	// Tools: ledger state and rail state
	if m := call(t, srv, "GET", "/transfers/"+id+"/money", "", ""); m.Body["debited"] != true || m.Body["credited"] != false {
		t.Fatalf("money: %v", m.Body)
	}
	expect(t, call(t, srv, "GET", "/transfers/"+id+"/rail", "", ""), 200, "rail status")

	// It shows up in the STUCK list
	req, _ := http.NewRequest("GET", srv.URL+"/transfers?status=STUCK&limit=500", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var list []map[string]any
	json.NewDecoder(resp.Body).Decode(&list)
	resp.Body.Close()
	found := false
	for _, s := range list {
		if s["id"] == id {
			found = true
		}
	}
	if !found {
		t.Fatal("stuck transfer missing from GET /transfers?status=STUCK")
	}

	// Executor: bad action, then retry after the rail heals
	expect(t, call(t, srv, "POST", "/transfers/"+id+"/actions", "", `{"action":"explode"}`), 400, "bad action")
	rail.mu.Lock()
	rail.broken = false
	rail.mu.Unlock()
	r = call(t, srv, "POST", "/transfers/"+id+"/actions", "", `{"action":"retry_step","actor":"test","reason":"rail is back"}`)
	expect(t, r, 200, "retry_step")
	if r.Body["status"] != "COMPLETED" {
		t.Fatalf("after retry: %v", r.Body)
	}
	expect(t, call(t, srv, "POST", "/transfers/"+id+"/actions", "", `{"action":"compensate"}`), 409, "action on finished transfer")

	// Invariants for this transfer are clean
	inv := call(t, srv, "GET", "/admin/invariants?saga_id="+id, "", "")
	expect(t, inv, 200, "invariants")
	if inv.Body["ok"] != true {
		t.Fatalf("invariants: %v", inv.Body)
	}
	expect(t, call(t, srv, "GET", "/admin/invariants?since=yesterday", "", ""), 400, "bad since")
	expect(t, call(t, srv, "POST", "/admin/reconcile", "", ""), 200, "reconcile")
}

func TestRecoveryEventEndpoints(t *testing.T) {
	pool := testdb.New(t)
	led := ledger.NewService(eventstore.New(pool), nil, 0)
	store := saga.NewStore(pool)
	cfg := saga.Config{Policy: saga.PolicyRefund, Retries: 1, Backoff: time.Millisecond, RunLimit: 10 * time.Second}
	orch := saga.NewOrchestrator(store, led, saga.AlwaysOKRail{}, cfg)
	srv := httptest.NewServer(NewServer(led, orch, audit.New(pool, store, led, nil), nil).
		WithRecoveryLog(recoverylog.New(eventstore.New(pool))))
	defer srv.Close()

	a := call(t, srv, "POST", "/wallets", "", `{"owner":"a"}`).Body["id"].(string)
	b := call(t, srv, "POST", "/wallets", "", `{"owner":"b"}`).Body["id"].(string)
	call(t, srv, "POST", "/wallets/"+a+"/credit", "seed", `{"amount_paise":5000}`)
	id := call(t, srv, "POST", "/transfers", "ev-"+a, fmt.Sprintf(`{"from_wallet":%q,"to_wallet":%q,"amount_paise":100}`, a, b)).Body["id"].(string)

	r := call(t, srv, "POST", "/transfers/"+id+"/recovery-events", "", `{"type":"RecoveryProposed","data":{"action":"retry_step","source":"llm"}}`)
	expect(t, r, 201, "add event")
	expect(t, call(t, srv, "POST", "/transfers/"+id+"/recovery-events", "", `{"type":"RecoveryRejected","data":{"reasons":["rail PAID"]}}`), 201, "add event 2")
	expect(t, call(t, srv, "POST", "/transfers/"+id+"/recovery-events", "", `{"type":"Nope","data":{}}`), 400, "bad type")
	expect(t, call(t, srv, "POST", "/transfers/01928c4e-0000-7000-8000-000000000000/recovery-events", "", `{"type":"RecoveryProposed","data":{}}`), 404, "unknown transfer")

	req, _ := http.NewRequest("GET", srv.URL+"/transfers/"+id+"/recovery-events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var list []map[string]any
	json.NewDecoder(resp.Body).Decode(&list)
	resp.Body.Close()
	if len(list) != 2 || list[0]["type"] != "RecoveryProposed" || list[1]["type"] != "RecoveryRejected" {
		t.Fatalf("events = %v", list)
	}
}
