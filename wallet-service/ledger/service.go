// Package ledger connects the wallet rules (domain) to the event store.
package ledger

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"

	"ledgemind/wallet-service/domain"
	"ledgemind/wallet-service/eventstore"
)

// ErrBusy means the wallet kept changing under us; the client should retry.
var ErrBusy = errors.New("wallet is busy, retry")

const maxAttempts = 10

type Service struct {
	store *eventstore.Store
}

func NewService(store *eventstore.Store) *Service { return &Service{store: store} }

func streamID(walletID string) string { return "wallet:" + walletID }

// Get loads a wallet by replaying its events.
func (s *Service) Get(ctx context.Context, walletID string) (*domain.Wallet, error) {
	events, err := s.store.LoadEvents(ctx, streamID(walletID), 0)
	if err != nil {
		return nil, err
	}
	w, err := domain.Rehydrate(walletID, events)
	if err != nil {
		return nil, err
	}
	if !w.Exists() {
		return nil, domain.ErrWalletNotFound
	}
	return w, nil
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
func (s *Service) Credit(ctx context.Context, walletID string, amount int64, key string) (w *domain.Wallet, applied bool, err error) {
	return s.execute(ctx, walletID, func(w *domain.Wallet) ([]eventstore.NewEvent, error) {
		return w.Credit(amount, key)
	})
}

// Debit removes money; same idempotency behaviour as Credit.
func (s *Service) Debit(ctx context.Context, walletID string, amount int64, key string) (w *domain.Wallet, applied bool, err error) {
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
		_, err = s.store.AppendEvents(ctx, streamID(walletID), w.Version, evs)
		if err == nil {
			w, err = s.Get(ctx, walletID)
			return w, true, err
		}
		if !errors.Is(err, eventstore.ErrConcurrencyConflict) {
			return nil, false, err
		}
		// Small random back-off so racing requests spread out.
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-time.After(time.Duration(1+rand.IntN(5*(attempt+1))) * time.Millisecond):
		}
	}
	return nil, false, ErrBusy
}
