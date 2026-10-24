package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestRail(t *testing.T, cfg Config) (*Rail, *httptest.Server, *bytes.Buffer) {
	t.Helper()
	logs := &bytes.Buffer{}
	r := NewRail(cfg, logs)
	srv := httptest.NewServer(r.Routes())
	t.Cleanup(srv.Close)
	return r, srv, logs
}

func fastConfig() Config {
	c := DefaultConfig()
	c.TimeoutDelayMs = 300
	c.ReversalDelayMs = 100
	return c
}

func pay(t *testing.T, srv *httptest.Server, sagaID, mode string, client *http.Client) (int, string) {
	t.Helper()
	body := fmt.Sprintf(`{"saga_id":%q,"from_wallet":"a","to_wallet":"b","amount_paise":1000}`, sagaID)
	req, _ := http.NewRequest("POST", srv.URL+"/pay", strings.NewReader(body))
	if mode != "" {
		req.Header.Set("X-Fault-Mode", mode)
	}
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err.Error() // e.g. client timeout
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func state(t *testing.T, srv *httptest.Server, sagaID string) string {
	t.Helper()
	resp, err := http.Get(srv.URL + "/payments/" + sagaID)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return "NONE"
	}
	var p Payment
	json.NewDecoder(resp.Body).Decode(&p)
	return p.State
}

func TestEachModeReplyAndTruth(t *testing.T) {
	_, srv, _ := newTestRail(t, fastConfig())
	cases := []struct {
		mode       string
		wantStatus int
		wantState  string
	}{
		{"none", 200, StatePaid},
		{"reject", 422, StateRejected},
		{"duplicate", 409, StatePaid},
		{"partial", 200, StatePaid}, // becomes REVERSED later, tested below
	}
	for _, c := range cases {
		id := "s-" + c.mode
		status, body := pay(t, srv, id, c.mode, nil)
		if status != c.wantStatus {
			t.Errorf("%s: status %d (%s), want %d", c.mode, status, body, c.wantStatus)
		}
		if got := state(t, srv, id); got != c.wantState {
			t.Errorf("%s: state %s, want %s", c.mode, got, c.wantState)
		}
	}
}

func TestTimeoutClientGivesUpButPaymentHappened(t *testing.T) {
	_, srv, _ := newTestRail(t, fastConfig())
	client := &http.Client{Timeout: 50 * time.Millisecond} // like the wallet's 2s vs rail's 5s
	status, _ := pay(t, srv, "s-timeout", "timeout", client)
	if status != 0 {
		t.Fatalf("expected client timeout, got status %d", status)
	}
	if got := state(t, srv, "s-timeout"); got != StatePaid {
		t.Fatalf("state %s, want PAID (the reply was lost, not the payment)", got)
	}
}

func TestPartialIsReversedLater(t *testing.T) {
	_, srv, _ := newTestRail(t, fastConfig())
	if status, _ := pay(t, srv, "s-partial", "partial", nil); status != 200 {
		t.Fatalf("status %d", status)
	}
	deadline := time.Now().Add(2 * time.Second)
	for state(t, srv, "s-partial") != StateReversed {
		if time.Now().After(deadline) {
			t.Fatal("payment never reversed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// A repeat now reports the reversal.
	if status, _ := pay(t, srv, "s-partial", "", nil); status != 409 {
		t.Fatalf("repeat after reversal: status %d, want 409", status)
	}
}

func TestUnknownHidesTheTruthButStatusRevealsIt(t *testing.T) {
	_, srv, _ := newTestRail(t, fastConfig())
	seen := map[string]bool{}
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("s-unk-%d", i)
		status, body := pay(t, srv, id, "unknown", nil)
		if status != 500 || !strings.HasPrefix(body, "ERR_") {
			t.Fatalf("unknown: status %d body %q", status, body)
		}
		seen[state(t, srv, id)] = true
	}
	if !seen[StatePaid] || !seen[StateRejected] {
		t.Fatalf("over 20 unknown faults saw states %v, want both PAID and REJECTED", seen)
	}
}

func TestRepeatNeverChargesTwice(t *testing.T) {
	r, srv, _ := newTestRail(t, fastConfig())
	pay(t, srv, "s-dup", "duplicate", nil) // 409 but PAID
	status, _ := pay(t, srv, "s-dup", "reject", nil)
	if status != 200 {
		t.Fatalf("retry of a paid payment: status %d, want 200", status)
	}
	if n := len(r.payments); n != 1 {
		t.Fatalf("%d payments stored, want 1", n)
	}
	pay(t, srv, "s-rej", "reject", nil)
	if status, _ := pay(t, srv, "s-rej", "", nil); status != 422 {
		t.Fatalf("retry of a rejected payment: status %d, want 422", status)
	}
}

func TestQueueBeatsRandomAndHeaderBeatsQueue(t *testing.T) {
	_, srv, _ := newTestRail(t, fastConfig())
	resp, err := http.Post(srv.URL+"/admin/faults", "application/json", strings.NewReader(`{"modes":["reject","duplicate"]}`))
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("queue: %v %v", resp, err)
	}
	if s, _ := pay(t, srv, "q1", "", nil); s != 422 {
		t.Fatalf("first queued: %d, want 422", s)
	}
	if s, _ := pay(t, srv, "q2", "none", nil); s != 200 { // header wins, queue untouched
		t.Fatalf("header override: %d, want 200", s)
	}
	if s, _ := pay(t, srv, "q3", "", nil); s != 409 {
		t.Fatalf("second queued: %d, want 409", s)
	}
	if s, _ := pay(t, srv, "q4", "", nil); s != 200 { // queue empty, rate 0
		t.Fatalf("after queue: %d, want 200", s)
	}
}

// Same seed + same call order = same faults. This is what makes runs repeatable.
func TestSameSeedSameFaults(t *testing.T) {
	run := func(seed int64) []Mode {
		cfg := fastConfig()
		cfg.Seed, cfg.FaultRate = seed, 0.5
		cfg.Weights = map[Mode]float64{ModeReject: 1, ModeDuplicate: 1, ModeUnknown: 1} // no slow modes
		r, srv, _ := newTestRail(t, cfg)
		for i := 0; i < 40; i++ {
			pay(t, srv, fmt.Sprintf("p%d", i), "", nil)
		}
		var modes []Mode
		for i := 0; i < 40; i++ {
			modes = append(modes, r.payments[fmt.Sprintf("p%d", i)].Mode)
		}
		return modes
	}
	a, b, c := run(7), run(7), run(8)
	if fmt.Sprint(a) != fmt.Sprint(b) {
		t.Fatalf("same seed gave different faults:\n%v\n%v", a, b)
	}
	if fmt.Sprint(a) == fmt.Sprint(c) {
		t.Fatal("different seeds gave identical faults (suspicious)")
	}
	faults := 0
	for _, m := range a {
		if m != ModeNone {
			faults++
		}
	}
	if faults < 10 || faults > 30 {
		t.Fatalf("%d/40 faults at rate 0.5, expected roughly 20", faults)
	}
}

func TestConfigResetsSequence(t *testing.T) {
	r, srv, _ := newTestRail(t, fastConfig())
	pay(t, srv, "x", "", nil)
	resp, err := http.Post(srv.URL+"/admin/config", "application/json", strings.NewReader(`{"seed":99,"fault_rate":0.2}`))
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("config: %v %v", resp, err)
	}
	if r.seq != 0 || len(r.payments) != 0 || r.cfg.Seed != 99 || r.cfg.FaultRate != 0.2 {
		t.Fatalf("not reset: seq=%d payments=%d cfg=%+v", r.seq, len(r.payments), r.cfg)
	}
	resp, _ = http.Post(srv.URL+"/admin/config", "application/json", strings.NewReader(`{"fault_rate":2}`))
	if resp.StatusCode != 400 {
		t.Fatalf("bad rate accepted: %d", resp.StatusCode)
	}
}

func TestEveryCallIsLogged(t *testing.T) {
	_, srv, logs := newTestRail(t, fastConfig())
	pay(t, srv, "log-1", "reject", nil)
	pay(t, srv, "log-1", "", nil)
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("%d log lines, want 2: %s", len(lines), logs.String())
	}
	var first, second CallLog
	json.Unmarshal([]byte(lines[0]), &first)
	json.Unmarshal([]byte(lines[1]), &second)
	if first.SagaID != "log-1" || first.Mode != ModeReject || first.HTTPStatus != 422 || first.Source != "header" {
		t.Fatalf("first log: %+v", first)
	}
	if !second.Repeat || second.Source != "repeat" {
		t.Fatalf("second log: %+v", second)
	}
}

func TestBadRequests(t *testing.T) {
	_, srv, _ := newTestRail(t, fastConfig())
	for _, body := range []string{`{`, `{"saga_id":"","amount_paise":5}`, `{"saga_id":"x","amount_paise":0}`} {
		resp, _ := http.Post(srv.URL+"/pay", "application/json", strings.NewReader(body))
		if resp.StatusCode != 400 {
			t.Errorf("%s: status %d, want 400", body, resp.StatusCode)
		}
	}
}
