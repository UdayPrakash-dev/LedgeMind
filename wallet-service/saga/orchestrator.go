package saga

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
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

// Notifier is told about every saga that becomes STUCK (the stuck-sagas queue).
type Notifier interface {
	Stuck(ctx context.Context, sg *Saga) error
}

type Config struct {
	Policy      string        // default policy for new transfers: refund, retry or escalate
	Retries     int           // extra attempts for debit, credit and refund (3)
	Backoff     time.Duration // wait before retry n of a wallet step: Backoff * 2^(n-1)
	RailRetries int           // retry policy: extra rail attempts (3)
	RailBackoff time.Duration // retry policy: 1s -> waits 1s, 2s, 4s
	RunLimit    time.Duration // max time one saga run may take
}

func DefaultConfig() Config {
	return Config{
		Policy: PolicyRefund, Retries: 3, Backoff: 100 * time.Millisecond,
		RailRetries: 3, RailBackoff: time.Second, RunLimit: 60 * time.Second,
	}
}

type Orchestrator struct {
	store    *Store
	wallets  Wallets
	rail     PaymentRail
	notifier Notifier // may be nil

	mu  sync.RWMutex
	cfg Config
}

func NewOrchestrator(store *Store, wallets Wallets, rail PaymentRail, cfg Config) *Orchestrator {
	if !ValidPolicy(cfg.Policy) {
		cfg.Policy = PolicyRefund
	}
	return &Orchestrator{store: store, wallets: wallets, rail: rail, cfg: cfg}
}

func (o *Orchestrator) SetNotifier(n Notifier) { o.notifier = n }

// Policy / SetPolicy change the policy used by NEW transfers (the benchmark switches it per run).
func (o *Orchestrator) Policy() string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.cfg.Policy
}

func (o *Orchestrator) SetPolicy(p string) error {
	if !ValidPolicy(p) {
		return fmt.Errorf("%w: unknown policy %q (use refund, retry or escalate)", ErrInvalidAction, p)
	}
	o.mu.Lock()
	o.cfg.Policy = p
	o.mu.Unlock()
	return nil
}

func (o *Orchestrator) config() Config {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.cfg
}

// Rail exposes the rail for status checks (may not implement RailStatus).
func (o *Orchestrator) Rail() PaymentRail { return o.rail }

func (o *Orchestrator) Store() *Store { return o.store }

type TransferRequest struct {
	From, To string
	Amount   int64
	Key      string
}

// Start creates a transfer and runs it until it finishes or gets stuck. A repeated
// key returns the existing transfer (created=false) without moving money again.
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
		CurrentStep: StepDebitSender, Status: StatusRunning, Policy: o.Policy(),
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

	if err := o.runDetached(ctx, sg.ID, ""); err != nil {
		log.Printf("saga %s: run error: %v", sg.ID, err)
	}
	sg, err = o.store.Get(ctx, sg.ID)
	return sg, true, err
}

func (o *Orchestrator) Get(ctx context.Context, id string) (*Saga, error) {
	return o.store.Get(ctx, id)
}

// runDetached keeps running even if the HTTP client disconnects: a half-finished saga is worse.
func (o *Orchestrator) runDetached(ctx context.Context, id, policyOverride string) error {
	runCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.config().RunLimit)
	defer cancel()
	return o.run(runCtx, id, policyOverride)
}

// Run drives a saga from its current step until it is finished or stuck.
// Every step is idempotent, so Run can safely be called again after a crash.
func (o *Orchestrator) Run(ctx context.Context, id string) error { return o.run(ctx, id, "") }

func (o *Orchestrator) run(ctx context.Context, id, policyOverride string) error {
	cfg := o.config()
	for {
		sg, err := o.store.Get(ctx, id)
		if err != nil {
			return err
		}
		if sg.Status != StatusRunning {
			return nil
		}
		policy := sg.Policy
		if policyOverride != "" {
			policy = policyOverride
		}

		var step, status string
		var entries []HistoryEntry

		switch sg.CurrentStep {
		case StepDebitSender:
			entries, err = o.attempt(ctx, StepDebitSender, cfg.Retries, cfg.Backoff, func() error {
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
			retries := 0
			if policy == PolicyRetry {
				retries = cfg.RailRetries
			}
			entries, err = o.attempt(ctx, StepCallRail, retries, cfg.RailBackoff, func() error {
				return o.rail.Pay(ctx, PayRequest{SagaID: sg.ID, From: sg.FromWallet, To: sg.ToWallet, Amount: sg.Amount})
			})
			switch {
			case err == nil:
				step, status = StepCreditReceiver, StatusRunning
			case policy == PolicyRefund:
				step, status = StepRefundSender, StatusRunning
			default: // retry gave up, or escalate
				step, status = StepCallRail, StatusStuck
			}

		case StepCreditReceiver:
			entries, err = o.attempt(ctx, StepCreditReceiver, cfg.Retries, cfg.Backoff, func() error {
				_, _, err := o.wallets.Credit(ctx, sg.ToWallet, sg.Amount, creditKey(sg.ID))
				return err
			})
			switch {
			case err == nil:
				step, status = StepDone, StatusCompleted
			case policy == PolicyRefund:
				step, status = StepRefundSender, StatusRunning
			default:
				step, status = StepCreditReceiver, StatusStuck
			}

		case StepRefundSender:
			entries, err = o.attempt(ctx, StepRefundSender, cfg.Retries, cfg.Backoff, func() error {
				_, _, err := o.wallets.Credit(ctx, sg.FromWallet, sg.Amount, refundKey(sg.ID))
				return err
			})
			if err == nil {
				step, status = StepDone, StatusCompensated
			} else {
				step, status = StepRefundSender, StatusStuck
			}

		default:
			step, status = sg.CurrentStep, StatusStuck
			entries = []HistoryEntry{entry(sg.CurrentStep, 1, fmt.Errorf("unknown step %q", sg.CurrentStep))}
		}

		if err := o.advance(ctx, sg.ID, step, status, entries...); err != nil {
			return err
		}
	}
}

// advance saves the transition and, if the saga became STUCK, notifies the queue.
func (o *Orchestrator) advance(ctx context.Context, id, step, status string, entries ...HistoryEntry) error {
	if err := o.store.Advance(ctx, id, step, status, entries...); err != nil {
		return err
	}
	if status == StatusStuck && o.notifier != nil {
		sg, err := o.store.Get(ctx, id)
		if err == nil {
			err = o.notifier.Stuck(ctx, sg)
		}
		if err != nil {
			// The saga is still STUCK in Postgres; GET /transfers?status=STUCK finds it.
			log.Printf("saga %s: stuck notification failed: %v", id, err)
		}
	}
	return nil
}

// attempt runs fn once plus up to `retries` retries, waiting backoff * 2^(n-1)
// before retry n. Permanent errors are not retried.
func (o *Orchestrator) attempt(ctx context.Context, step string, retries int, backoff time.Duration, fn func() error) ([]HistoryEntry, error) {
	var entries []HistoryEntry
	var err error
	for n := 1; n <= 1+retries; n++ {
		if n > 1 {
			select {
			case <-ctx.Done():
				return append(entries, entry(step, n, ctx.Err())), ctx.Err()
			case <-time.After(backoff * time.Duration(1<<(n-2))):
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
	sagas, err := o.store.List(ctx, StatusRunning, time.Time{}, 1000)
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
