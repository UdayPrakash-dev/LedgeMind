// Package audit checks that the money in the system still adds up.
//
// Checks (each failure is a Violation with a Kind):
//
//	negative_balance             a wallet's balance is below zero
//	snapshot_mismatch            loading a wallet (snapshot + events) disagrees with its raw events
//	money_created                a saga returned/delivered more than it took (in-flight < 0)
//	over_delivery                receiver got more than the amount, or sender got back more than was taken
//	unsettled_terminal_saga      a finished saga (COMPLETED/COMPENSATED/FAILED) still has money in flight
//	rail_paid_but_refunded       sender refunded although the rail really paid (money left twice)
//	delivered_but_rail_not_paid  receiver credited although the rail rejected/reversed/never got it
//	double_rail_payment          the rail paid more than once for one transfer
//
// Global identity, reported in the summary: sum(balances) = external_net - in_flight,
// where external_net is money added/removed outside sagas (plain credits/debits).
package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"ledgemind/wallet-service/domain"
	"ledgemind/wallet-service/saga"
)

type Violation struct {
	Kind     string `json:"kind"`
	SagaID   string `json:"saga_id,omitempty"`
	WalletID string `json:"wallet_id,omitempty"`
	Detail   string `json:"detail"`
}

type SagaFlow struct {
	Out       int64 `json:"out"`       // debited from sender
	Back      int64 `json:"back"`      // refunded to sender (full + partial)
	Delivered int64 `json:"delivered"` // credited to receiver minus reversals
}

func (f SagaFlow) InFlight() int64 { return f.Out - f.Back - f.Delivered }

type Report struct {
	OK                        bool                `json:"ok"`
	CheckedAt                 time.Time           `json:"checked_at"`
	Wallets                   int                 `json:"wallets"`
	Sagas                     int                 `json:"sagas"`
	TotalBalance              int64               `json:"total_balance_paise"`
	ExternalNet               int64               `json:"external_net_paise"`
	InFlight                  int64               `json:"in_flight_paise"`
	RailChecked               bool                `json:"rail_checked"`
	WalletsChangedDuringCheck int                 `json:"wallets_changed_during_check"`
	Violations                []Violation         `json:"violations"`
	ByKind                    map[string]int      `json:"violations_by_kind"`
	SagaStatuses              map[string]int      `json:"saga_statuses"`
	Flows                     map[string]SagaFlow `json:"-"`
}

type WalletLoader interface {
	Get(ctx context.Context, walletID string) (*domain.Wallet, error)
}

type Auditor struct {
	db      *pgxpool.Pool
	store   *saga.Store
	wallets WalletLoader
	rail    saga.RailStatus // may be nil: rail checks skipped
}

func New(db *pgxpool.Pool, store *saga.Store, wallets WalletLoader, rail saga.RailStatus) *Auditor {
	return &Auditor{db: db, store: store, wallets: wallets, rail: rail}
}

type Options struct {
	Since     time.Time // only sagas created at/after this (wallet checks are always global)
	SagaID    string    // only this saga (skips wallet-wide checks)
	CheckRail bool
}

// Check runs every invariant and returns a report. It never changes anything.
func (a *Auditor) Check(ctx context.Context, opt Options) (*Report, error) {
	r := &Report{OK: true, CheckedAt: time.Now().UTC(), ByKind: map[string]int{},
		SagaStatuses: map[string]int{}, Flows: map[string]SagaFlow{}, Violations: []Violation{}}
	add := func(v Violation) {
		r.Violations = append(r.Violations, v)
		r.ByKind[v.Kind]++
		r.OK = false
	}

	// The system may be running while we check. Read sagas FIRST, then events:
	// a saga's status is only updated after its money events are written, so a
	// saga we saw as finished has all its events in the (later) event read.
	var sagas []*saga.Saga
	if opt.SagaID != "" {
		sg, err := a.store.Get(ctx, opt.SagaID)
		if err != nil {
			return nil, err
		}
		sagas = []*saga.Saga{sg}
	} else {
		var err error
		if sagas, err = a.store.List(ctx, "", opt.Since, 0); err != nil {
			return nil, err
		}
	}

	// 1. Read every money event once (one statement = one consistent view).
	rows, err := a.db.Query(ctx, `
		SELECT stream_id, version, type, payload FROM events
		 WHERE stream_id LIKE 'wallet:%' AND type IN ($1, $2)`,
		domain.EventWalletCredited, domain.EventWalletDebited)
	if err != nil {
		return nil, err
	}
	balances := map[string]int64{}
	lastVersion := map[string]int64{}
	for rows.Next() {
		var stream, typ string
		var version int64
		var raw []byte
		if err := rows.Scan(&stream, &version, &typ, &raw); err != nil {
			rows.Close()
			return nil, err
		}
		var p domain.MoneyPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			rows.Close()
			return nil, fmt.Errorf("bad payload in %s: %w", stream, err)
		}
		wallet := strings.TrimPrefix(stream, "wallet:")
		signed := p.Amount
		if typ == domain.EventWalletDebited {
			signed = -p.Amount
		}
		balances[wallet] += signed
		if version > lastVersion[wallet] {
			lastVersion[wallet] = version
		}

		id, kind, isSaga := saga.ParseMoneyKey(p.IdempotencyKey)
		if !isSaga {
			r.ExternalNet += signed
			continue
		}
		f := r.Flows[id]
		switch kind {
		case "debit":
			f.Out += p.Amount
		case "refund", "partial-refund":
			f.Back += p.Amount
		case "credit":
			f.Delivered += p.Amount
		case "reverse":
			f.Delivered -= p.Amount
		}
		r.Flows[id] = f
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// 2. Wallet-wide checks.
	if opt.SagaID == "" {
		ids := make([]string, 0, len(balances))
		for id := range balances {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			bal := balances[id]
			r.TotalBalance += bal
			if bal < 0 {
				add(Violation{Kind: "negative_balance", WalletID: id, Detail: fmt.Sprintf("balance %d", bal)})
			}
			if a.wallets != nil {
				w, err := a.wallets.Get(ctx, id)
				if err != nil {
					return nil, err
				}
				// Only compare if the wallet did not change since we read its events.
				// (lastVersion ignores WalletOpened at version 1, so a wallet with
				// money events always has lastVersion >= 2.)
				if w.Version != lastVersion[id] {
					r.WalletsChangedDuringCheck++
				} else if w.Balance != bal {
					add(Violation{Kind: "snapshot_mismatch", WalletID: id,
						Detail: fmt.Sprintf("loaded balance %d, events say %d", w.Balance, bal)})
				}
			}
		}
		r.Wallets = len(ids)
	}

	// 3. Per-saga checks.
	r.Sagas = len(sagas)
	r.RailChecked = opt.CheckRail && a.rail != nil

	for _, sg := range sagas {
		r.SagaStatuses[sg.Status]++
		f := r.Flows[sg.ID]
		in := f.InFlight()
		r.InFlight += in
		bad := func(kind, format string, args ...any) {
			add(Violation{Kind: kind, SagaID: sg.ID, Detail: fmt.Sprintf(format, args...)})
		}
		if in < 0 {
			bad("money_created", "took %d, gave back %d, delivered %d", f.Out, f.Back, f.Delivered)
		}
		if f.Delivered > sg.Amount || f.Back > f.Out || f.Delivered < 0 {
			bad("over_delivery", "amount %d, took %d, gave back %d, delivered %d", sg.Amount, f.Out, f.Back, f.Delivered)
		}
		terminal := sg.Status == saga.StatusCompleted || sg.Status == saga.StatusCompensated || sg.Status == saga.StatusFailed
		if terminal && in != 0 {
			bad("unsettled_terminal_saga", "status %s but %d paise still in flight", sg.Status, in)
		}

		if !r.RailChecked || f.Out == 0 {
			continue
		}
		paid := 0
		var states []string
		for _, ref := range sg.RailRefs() {
			st, err := a.rail.Status(ctx, ref)
			if err != nil {
				return nil, fmt.Errorf("rail status %s: %w", ref, err)
			}
			states = append(states, ref+"="+st)
			if st == "PAID" {
				paid++
			}
		}
		if paid > 1 {
			bad("double_rail_payment", "rail paid %d times: %s", paid, strings.Join(states, ", "))
		}
		if f.Delivered > 0 && paid == 0 {
			bad("delivered_but_rail_not_paid", "receiver credited but rail says %s", strings.Join(states, ", "))
		}
		if f.Delivered == 0 && f.Back >= f.Out && paid > 0 {
			bad("rail_paid_but_refunded", "sender refunded but rail says %s", strings.Join(states, ", "))
		}
	}

	// Global identity. Use every saga flow we read (including sagas created after
	// the saga list was read), so concurrent traffic cannot cause false alarms.
	if opt.SagaID == "" && opt.Since.IsZero() {
		r.InFlight = 0
		for _, f := range r.Flows {
			r.InFlight += f.InFlight()
		}
	}
	if opt.SagaID == "" && opt.Since.IsZero() && r.TotalBalance != r.ExternalNet-r.InFlight {
		add(Violation{Kind: "conservation", Detail: fmt.Sprintf("balances %d != external %d - in flight %d",
			r.TotalBalance, r.ExternalNet, r.InFlight)})
	}
	return r, nil
}
