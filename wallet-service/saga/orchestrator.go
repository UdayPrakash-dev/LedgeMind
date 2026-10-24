package saga

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"

	"ledgemind/wallet-service/domain"
)

// Wallets is what the saga needs from the ledger (ledger.Service fits).
type Wallets interface {
	Get(ctx context.Context, walletID string) (*domain.Wallet, error)
	Credit(ctx context.Context, walletID string, amount int64, key string) (*domain.Wallet, bool, error)
	Debit(ctx context.Context, walletID string, amount int64, key string) (*domain.Wallet, bool, error)
}

type Config struct {
	Retries  int           // extra attempts for debit, credit and refund (plan: 3)
	Backoff  time.Duration // wait before retry n is Backoff * 2^(n-1)
	RunLimit time.Duration // max time one saga run may take
}

func DefaultConfig() Config {
	return Config{Retries: 3, Backoff: 100 * time.Millisecond, RunLimit: 30 * time.Second}
}

type Orchestrator struct {
	store   *Store
	wallets Wallets
	rail    PaymentRail
	cfg     Config
}

func NewOrchestrator(store *Store, wallets Wallets, rail PaymentRail, cfg Config) *Orchestrator {
	return &Orchestrator{store: store, wallets: wallets, rail: rail, cfg: cfg}
}

type TransferRequest struct {
	From, To string
	Amount   int64
	Key      string
}

// Start creates a transfer and runs it to the end. A repeated key returns the
// existing transfer (created=false) without moving money again.
func (o *Orchestrator) Start(ctx context.Context, req TransferRequest) (*Saga, bool, error) {
	req.Key = strings.TrimSpace(req.Key)
	switch {
	case req.Key == "":
		return nil, false, domain.ErrMissingKey
	case req.Amount <= 0 || req.Amount > domain.MaxAmount:
		return nil, false, domain.ErrInvalidAmount
	case req.From == req.To:
		return nil, false, fmt.Errorf("%w: sender and receiver must differ", ErrInvalidTransfer)
	}
	for _, id := range []string{req.From, req.To} {
		if _, err := o.wallets.Get(ctx, id); err != nil {
			return nil, false, err
		}
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, false, err
	}
	sg, created, err := o.store.Create(ctx, &Saga{
		ID: id.String(), IdempotencyKey: req.Key,
		FromWallet: req.From, ToWallet: req.To, Amount: req.Amount,
		CurrentStep: StepDebitSender, Status: StatusRunning,
	})
	if err != nil {
		return nil, false, err
	}
	if !created {
		if sg.FromWallet != req.From || sg.ToWallet != req.To || sg.Amount != req.Amount {
			return nil, false, ErrIdempotencyMismatch
		}
		return sg, false, nil
	}

	// Keep running even if the HTTP client disconnects: a half-finished saga is worse.
	runCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.cfg.RunLimit)
	defer cancel()
	if err := o.Run(runCtx, sg.ID); err != nil {
		log.Printf("saga %s: run error: %v", sg.ID, err)
	}
	sg, err = o.store.Get(ctx, sg.ID)
	return sg, true, err
}

func (o *Orchestrator) Get(ctx context.Context, id string) (*Saga, error) {
	return o.store.Get(ctx, id)
}

// Run drives a saga from its current step until it is finished or stuck.
// Every step is idempotent, so Run can safely be called again after a crash.
func (o *Orchestrator) Run(ctx context.Context, id string) error {
	for {
		sg, err := o.store.Get(ctx, id)
		if err != nil {
			return err
		}
		if sg.Status != StatusRunning {
			return nil
		}

		var step, status string
		var entries []HistoryEntry

		switch sg.CurrentStep {
		case StepDebitSender:
			var err error
			entries, err = o.attempt(ctx, StepDebitSender, func() error {
				_, _, err := o.wallets.Debit(ctx, sg.FromWallet, sg.Amount, debitKey(sg.ID))
				return err
			})
			switch {
			case err == nil:
				step, status = StepCallRail, StatusRunning
			case permanent(err): // e.g. insufficient funds: nothing moved, nothing to undo
				step, status = StepDone, StatusFailed
			default:
				step, status = StepDebitSender, StatusStuck
			}

		case StepCallRail:
			// No retries here: the fixed rule is "rail failed -> refund".
			err := o.rail.Pay(ctx, PayRequest{SagaID: sg.ID, From: sg.FromWallet, To: sg.ToWallet, Amount: sg.Amount})
			entries = []HistoryEntry{entry(StepCallRail, 1, err)}
			if err == nil {
				step, status = StepCreditReceiver, StatusRunning
			} else {
				step, status = StepRefundSender, StatusRunning
			}

		case StepCreditReceiver:
			var err error
			entries, err = o.attempt(ctx, StepCreditReceiver, func() error {
				_, _, err := o.wallets.Credit(ctx, sg.ToWallet, sg.Amount, creditKey(sg.ID))
				return err
			})
			if err == nil {
				step, status = StepDone, StatusCompleted
			} else {
				step, status = StepRefundSender, StatusRunning
			}

		case StepRefundSender:
			var err error
			entries, err = o.attempt(ctx, StepRefundSender, func() error {
				_, _, err := o.wallets.Credit(ctx, sg.FromWallet, sg.Amount, refundKey(sg.ID))
				return err
			})
			if err == nil {
				step, status = StepDone, StatusCompensated
			} else {
				step, status = StepRefundSender, StatusStuck
			}

		default:
			return o.store.Advance(ctx, sg.ID, sg.CurrentStep, StatusStuck,
				entry(sg.CurrentStep, 1, fmt.Errorf("unknown step %q", sg.CurrentStep)))
		}

		if err := o.store.Advance(ctx, sg.ID, step, status, entries...); err != nil {
			return err
		}
	}
}

// attempt runs fn once plus up to cfg.Retries retries. Permanent errors are not retried.
func (o *Orchestrator) attempt(ctx context.Context, step string, fn func() error) ([]HistoryEntry, error) {
	var entries []HistoryEntry
	var err error
	for n := 1; n <= 1+o.cfg.Retries; n++ {
		if n > 1 {
			select {
			case <-ctx.Done():
				return append(entries, entry(step, n, ctx.Err())), ctx.Err()
			case <-time.After(o.cfg.Backoff * time.Duration(1<<(n-2))):
			}
		}
		err = fn()
		entries = append(entries, entry(step, n, err))
		if err == nil || permanent(err) {
			break
		}
	}
	return entries, err
}

// permanent errors will not go away by retrying.
func permanent(err error) bool {
	return errors.Is(err, domain.ErrInsufficientFunds) ||
		errors.Is(err, domain.ErrWalletNotFound) ||
		errors.Is(err, domain.ErrInvalidAmount) ||
		errors.Is(err, domain.ErrIdempotencyMismatch)
}

func entry(step string, attempt int, err error) HistoryEntry {
	e := HistoryEntry{At: time.Now().UTC(), Step: step, Attempt: attempt, Outcome: "ok"}
	if err != nil {
		e.Outcome, e.Error = "error", err.Error()
	}
	return e
}

// ResumeRunning finishes sagas left RUNNING by a crash or restart.
func (o *Orchestrator) ResumeRunning(ctx context.Context) (int, error) {
	sagas, err := o.store.ListByStatus(ctx, StatusRunning, 1000)
	if err != nil {
		return 0, err
	}
	for _, sg := range sagas {
		if err := o.Run(ctx, sg.ID); err != nil {
			log.Printf("resume saga %s: %v", sg.ID, err)
		}
	}
	return len(sagas), nil
}
