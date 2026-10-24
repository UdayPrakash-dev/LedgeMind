package domain

import "fmt"

// Snapshot is a wallet's full state at one version, so loading can skip
// replaying old events. It must include the idempotency keys: without them a
// repeated request from before the snapshot would be applied twice.
type Snapshot struct {
	WalletID string               `json:"wallet_id"`
	Owner    string               `json:"owner"`
	Balance  int64                `json:"balance"`
	Version  int64                `json:"version"`
	Ops      map[string]Operation `json:"ops"`
}

// Operation remembers what an idempotency key was used for.
type Operation struct {
	Type   string `json:"type"`
	Amount int64  `json:"amount"`
}

func (w *Wallet) Snapshot() Snapshot {
	ops := make(map[string]Operation, len(w.ops))
	for k, v := range w.ops {
		ops[k] = v
	}
	return Snapshot{WalletID: w.ID, Owner: w.Owner, Balance: w.Balance, Version: w.Version, Ops: ops}
}

// FromSnapshot restores a wallet; events after s.Version must be applied next.
func FromSnapshot(s Snapshot) (*Wallet, error) {
	if s.WalletID == "" || s.Version < 1 || s.Balance < 0 {
		return nil, fmt.Errorf("invalid snapshot: %+v", s)
	}
	w := NewWallet(s.WalletID)
	w.Owner = s.Owner
	w.Balance = s.Balance
	w.Version = s.Version
	w.opened = true
	for k, v := range s.Ops {
		w.ops[k] = v
	}
	return w, nil
}
