// Package ledger connects the wallet rules (domain) to the event store,
// with an optional snapshot cache in front of it.
package ledger

import (
	"context"
	"errors"
	"log"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"ledgemind/wallet-service/domain"
	"ledgemind/wallet-service/eventstore"
)

// ErrBusy means the wallet kept changing under us; the client should retry.
var ErrBusy = errors.New("wallet is busy, retry")

const maxAttempts = 10

// DefaultSnapshotEvery: take a snapshot each time a wallet passes another 50 events.
const DefaultSnapshotEvery = 50

// SnapshotStore is a cache. Errors are logged and ignored.
type SnapshotStore interface {
	Get(ctx context.Context, walletID string) (*domain.Snapshot, error)
	Put(ctx context.Context, s domain.Snapshot) error
}

// Stats count how wallets were loaded (useful for the paper's numbers).
type Stats struct {
	SnapshotHits   atomic.Int64 // loaded from snapshot + tail
	SnapshotMisses atomic.Int64 // no snapshot: full replay
	SnapshotErrors atomic.Int64 // Redis failed: full replay
	EventsReplayed atomic.Int64 // total events applied while loading
}

type Service struct {
	store         *eventstore.Store
	snaps         SnapshotStore // may be nil
	snapshotEvery int64
	Stats         Stats
}

// NewService creates a service. snaps may be nil (no cache). every <= 0 uses the default.
func NewService(store *eventstore.Store, snaps SnapshotStore, every int64) *Service {
	if every <= 0 {
		every = DefaultSnapshotEvery
	}
	return &Service{store: store, snaps: snaps, snapshotEvery: every}
}

func streamID(walletID string) string { return "wallet:" + walletID }

// Get loads a wallet: snapshot + newer events if possible, else full replay.
func (s *Service) Get(ctx context.Context, walletID string) (*domain.Wallet, error) {
	w, from := s.fromSnapshot(ctx, walletID)
	events, err := s.store.LoadEvents(ctx, streamID(walletID), from)
	if err != nil {
		return nil, err
	}
	for _, e := range events {
		if err := w.Apply(e); err != nil {
			return nil, err
		}
	}
	s.Stats.EventsReplayed.Add(int64(len(events)))
	if !w.Exists() {
		return nil, domain.ErrWalletNotFound
	}
	return w, nil
}

// fromSnapshot returns the starting wallet and the version to load events after.
func (s *Service) fromSnapshot(ctx context.Context, walletID string) (*domain.Wallet, int64) {
	empty := domain.NewWallet(walletID)
	if s.snaps == nil {
		return empty, 0
	}
	snap, err := s.snaps.Get(ctx, walletID)
	if err != nil {
		s.Stats.SnapshotErrors.Add(1)
		return empty, 0
	}
	if snap == nil {
		s.Stats.SnapshotMisses.Add(1)
		return empty, 0
	}
	w, err := domain.FromSnapshot(*snap)
	if err != nil || w.ID != walletID {
		log.Printf("ignoring bad snapshot for %s: %v", walletID, err)
		s.Stats.SnapshotErrors.Add(1)
		return empty, 0
	}
	s.Stats.SnapshotHits.Add(1)
	return w, w.Version
}

// maybeSnapshot saves a snapshot when the wallet crossed a multiple of snapshotEvery.
func (s *Service) maybeSnapshot(ctx context.Context, w *domain.Wallet, before int64) {
	if s.snaps == nil || w.Version/s.snapshotEvery == before/s.snapshotEvery {
		return
	}
	if err := s.snaps.Put(ctx, w.Snapshot()); err != nil {
		log.Printf("snapshot for %s skipped: %v", w.ID, err)
	}
}

// OpenWallet creates a new wallet with a fresh UUIDv7 id.
func (s *Service) OpenWallet(ctx context.Context, owner string) (*domain.Wallet, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	w := domain.NewWallet(id.String())
	evs, err := w.Open(owner)
	if err != nil {
		return nil, err
	}
	if _, err := s.store.AppendEvents(ctx, streamID(w.ID), 0, evs); err != nil {
		return nil, err
	}
	return s.Get(ctx, w.ID)
}

// Credit adds money. applied=false means this idempotency key was already used
// for the same request, so nothing new happened.
func (s *Service) Credit(ctx context.Context, walletID string, amount int64, key string) (*domain.Wallet, bool, error) {
	return s.execute(ctx, walletID, func(w *domain.Wallet) ([]eventstore.NewEvent, error) {
		return w.Credit(amount, key)
	})
}

// Debit removes money; same idempotency behaviour as Credit.
func (s *Service) Debit(ctx context.Context, walletID string, amount int64, key string) (*domain.Wallet, bool, error) {
	return s.execute(ctx, walletID, func(w *domain.Wallet) ([]eventstore.NewEvent, error) {
		return w.Debit(amount, key)
	})
}

// execute is the load -> decide -> append loop with optimistic concurrency.
// If another request wrote first, reload and decide again. That retry is also
// what makes concurrent duplicate requests safe: on reload the key is seen.
func (s *Service) execute(ctx context.Context, walletID string, decide func(*domain.Wallet) ([]eventstore.NewEvent, error)) (*domain.Wallet, bool, error) {
	for attempt := 0; attempt < maxAttempts; attempt++ {
		w, err := s.Get(ctx, walletID)
		if err != nil {
			return nil, false, err
		}
		evs, err := decide(w)
		if err != nil {
			return nil, false, err
		}
		if len(evs) == 0 { // repeat of an already-applied request
			return w, false, nil
		}
		before := w.Version
		_, err = s.store.AppendEvents(ctx, streamID(walletID), before, evs)
		if err == nil {
			w, err = s.Get(ctx, walletID)
			if err == nil {
				s.maybeSnapshot(ctx, w, before)
			}
			return w, true, err
		}
		if !errors.Is(err, eventstore.ErrConcurrencyConflict) {
			return nil, false, err
		}
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-time.After(time.Duration(1+rand.IntN(5*(attempt+1))) * time.Millisecond):
		}
	}
	return nil, false, ErrBusy
}
