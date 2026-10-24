package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"ledgemind/wallet-service/eventstore"
	"ledgemind/wallet-service/ledger"
)

// End-to-end tests: real HTTP handlers + real Postgres. Needs DATABASE_URL.
func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping end-to-end tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	schema, err := os.ReadFile("../../db/init/001_events.sql")
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	if _, err := pool.Exec(ctx, string(schema)); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	srv := httptest.NewServer(NewServer(ledger.NewService(eventstore.New(pool)), nil))
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
