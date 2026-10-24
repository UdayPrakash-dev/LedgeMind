package domain

import (
	"encoding/json"
	"errors"
	"testing"

	"ledgemind/wallet-service/eventstore"
)

// apply turns new events into stored events and applies them, like the real store would.
func apply(t *testing.T, w *Wallet, evs []eventstore.NewEvent) {
	t.Helper()
	for _, ne := range evs {
		payload, err := json.Marshal(ne.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Apply(eventstore.Event{Version: w.Version + 1, Type: ne.Type, Payload: payload}); err != nil {
			t.Fatalf("apply %s: %v", ne.Type, err)
		}
	}
}

func mustDo(t *testing.T, w *Wallet, evs []eventstore.NewEvent, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("command failed: %v", err)
	}
	apply(t, w, evs)
}

func openWallet(t *testing.T) *Wallet {
	t.Helper()
	w := NewWallet("w1")
	evs, err := w.Open("uday")
	mustDo(t, w, evs, err)
	return w
}

func TestOpenCreditDebit(t *testing.T) {
	w := openWallet(t)
	evs, err := w.Credit(50_000, "k1")
	mustDo(t, w, evs, err)
	evs, err = w.Debit(20_000, "k2")
	mustDo(t, w, evs, err)

	if w.Balance != 30_000 {
		t.Fatalf("balance = %d, want 30000", w.Balance)
	}
	if w.Version != 3 {
		t.Fatalf("version = %d, want 3", w.Version)
	}
	if w.Owner != "uday" {
		t.Fatalf("owner = %q", w.Owner)
	}
}

func TestOpenRules(t *testing.T) {
	w := openWallet(t)
	if _, err := w.Open("again"); !errors.Is(err, ErrAlreadyOpened) {
		t.Fatalf("second open: err = %v", err)
	}
	if _, err := NewWallet("w2").Open("   "); !errors.Is(err, ErrInvalidOwner) {
		t.Fatalf("blank owner: err = %v", err)
	}
}

func TestInvalidAmounts(t *testing.T) {
	w := openWallet(t)
	for _, amt := range []int64{0, -1, -50_000, MaxAmount + 1} {
		if _, err := w.Credit(amt, "k"); !errors.Is(err, ErrInvalidAmount) {
			t.Errorf("credit %d: err = %v, want ErrInvalidAmount", amt, err)
		}
		if _, err := w.Debit(amt, "k"); !errors.Is(err, ErrInvalidAmount) {
			t.Errorf("debit %d: err = %v, want ErrInvalidAmount", amt, err)
		}
	}
}

func TestDebitMoreThanBalanceRejected(t *testing.T) {
	w := openWallet(t)
	evs, err := w.Credit(10_000, "k1")
	mustDo(t, w, evs, err)
	if _, err := w.Debit(10_001, "k2"); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("err = %v, want ErrInsufficientFunds", err)
	}
	// Exactly the balance is fine.
	evs, err = w.Debit(10_000, "k3")
	mustDo(t, w, evs, err)
	if w.Balance != 0 {
		t.Fatalf("balance = %d, want 0", w.Balance)
	}
}

func TestCommandsOnUnopenedWallet(t *testing.T) {
	w := NewWallet("ghost")
	if _, err := w.Credit(100, "k"); !errors.Is(err, ErrWalletNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestMissingKeyRejected(t *testing.T) {
	w := openWallet(t)
	if _, err := w.Credit(100, "  "); !errors.Is(err, ErrMissingKey) {
		t.Fatalf("err = %v", err)
	}
}

func TestSameKeyTwiceAppliesOnce(t *testing.T) {
	w := openWallet(t)
	evs, err := w.Credit(50_000, "pay-1")
	mustDo(t, w, evs, err)

	evs, err = w.Credit(50_000, "pay-1")
	if err != nil || evs != nil {
		t.Fatalf("repeat credit: evs=%v err=%v, want nil,nil", evs, err)
	}
	if w.Balance != 50_000 {
		t.Fatalf("balance = %d, want 50000", w.Balance)
	}
}

func TestRepeatedDebitSucceedsEvenAfterBalanceDrops(t *testing.T) {
	w := openWallet(t)
	evs, err := w.Credit(100, "c1")
	mustDo(t, w, evs, err)
	evs, err = w.Debit(100, "d1")
	mustDo(t, w, evs, err)

	// Balance is 0 now, but repeating d1 is a no-op, not "insufficient funds".
	evs, err = w.Debit(100, "d1")
	if err != nil || evs != nil {
		t.Fatalf("repeat debit: evs=%v err=%v, want nil,nil", evs, err)
	}
}

func TestSameKeyDifferentRequestRejected(t *testing.T) {
	w := openWallet(t)
	evs, err := w.Credit(100, "k1")
	mustDo(t, w, evs, err)

	if _, err := w.Credit(200, "k1"); !errors.Is(err, ErrIdempotencyMismatch) {
		t.Fatalf("different amount: err = %v", err)
	}
	if _, err := w.Debit(100, "k1"); !errors.Is(err, ErrIdempotencyMismatch) {
		t.Fatalf("different type: err = %v", err)
	}
}

func TestRehydrateMatchesLiveState(t *testing.T) {
	live := openWallet(t)
	var stored []eventstore.Event
	record := func(evs []eventstore.NewEvent, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		for _, ne := range evs {
			p, _ := json.Marshal(ne.Payload)
			e := eventstore.Event{Version: live.Version + 1, Type: ne.Type, Payload: p}
			stored = append(stored, e)
			if err := live.Apply(e); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Rebuild the opened event too.
	op, _ := json.Marshal(OpenedPayload{WalletID: "w1", Owner: "uday"})
	stored = append(stored, eventstore.Event{Version: 1, Type: EventWalletOpened, Payload: op})

	record(live.Credit(1_000, "a"))
	record(live.Credit(2_500, "b"))
	record(live.Debit(700, "c"))

	replayed, err := Rehydrate("w1", stored)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Balance != live.Balance || replayed.Version != live.Version || replayed.Owner != live.Owner {
		t.Fatalf("replayed %+v != live %+v", replayed, live)
	}
	if !replayed.IsRepeat(EventWalletCredited, 1_000, "a") {
		t.Fatal("replayed wallet forgot idempotency key a")
	}
}

func TestApplyRejectsBadEvents(t *testing.T) {
	cases := []struct {
		name string
		e    eventstore.Event
	}{
		{"unknown type", eventstore.Event{Version: 2, Type: "WalletExploded", Payload: []byte(`{}`)}},
		{"bad json", eventstore.Event{Version: 2, Type: EventWalletCredited, Payload: []byte(`{oops`)}},
		{"zero amount", eventstore.Event{Version: 2, Type: EventWalletCredited, Payload: []byte(`{"amount":0}`)}},
		{"version gap", eventstore.Event{Version: 5, Type: EventWalletCredited, Payload: []byte(`{"amount":1}`)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := openWallet(t)
			if err := w.Apply(c.e); err == nil {
				t.Fatal("want error, got nil")
			}
		})
	}
	// Money event before WalletOpened.
	if err := NewWallet("x").Apply(eventstore.Event{Version: 1, Type: EventWalletCredited, Payload: []byte(`{"amount":1}`)}); err == nil {
		t.Fatal("credit before open: want error")
	}
}
