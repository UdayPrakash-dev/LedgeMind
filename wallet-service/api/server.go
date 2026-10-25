// Package api is the HTTP layer of the wallet service.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"ledgemind/wallet-service/audit"
	"ledgemind/wallet-service/domain"
	"ledgemind/wallet-service/ledger"
	"ledgemind/wallet-service/recoverylog"
	"ledgemind/wallet-service/saga"
)

const IdempotencyHeader = "Idempotency-Key"

type Server struct {
	svc     *ledger.Service
	orch    *saga.Orchestrator
	auditor *audit.Auditor
	reclog  *recoverylog.Log // may be nil: recovery-event endpoints answer 501
	mux     *http.ServeMux
}

// WithRecoveryLog enables POST/GET /transfers/{id}/recovery-events.
func (s *Server) WithRecoveryLog(l *recoverylog.Log) *Server { s.reclog = l; return s }

// NewServer registers all routes. health may be nil.
func NewServer(svc *ledger.Service, orch *saga.Orchestrator, auditor *audit.Auditor, health http.HandlerFunc) *Server {
	s := &Server{svc: svc, orch: orch, auditor: auditor, mux: http.NewServeMux()}
	if health != nil {
		s.mux.HandleFunc("GET /health", health)
	}
	s.mux.HandleFunc("POST /wallets", s.openWallet)
	s.mux.HandleFunc("GET /wallets/{id}", s.getWallet)
	s.mux.HandleFunc("POST /wallets/{id}/credit", s.money(s.svc.Credit))
	s.mux.HandleFunc("POST /wallets/{id}/debit", s.money(s.svc.Debit))
	s.mux.HandleFunc("POST /transfers", s.startTransfer)
	s.mux.HandleFunc("GET /transfers", s.listTransfers)
	s.mux.HandleFunc("GET /transfers/{id}", s.getTransfer)

	// Recovery: the executor and the tools a recovery strategy may use.
	s.mux.HandleFunc("POST /transfers/{id}/actions", s.executeAction)
	s.mux.HandleFunc("GET /transfers/{id}/money", s.transferMoney)
	s.mux.HandleFunc("GET /transfers/{id}/rail", s.transferRail)

	// Experiment control.
	s.mux.HandleFunc("GET /admin/policy", s.getPolicy)
	s.mux.HandleFunc("POST /admin/policy", s.setPolicy)
	s.mux.HandleFunc("GET /admin/invariants", s.invariants)
	s.mux.HandleFunc("POST /admin/reconcile", s.reconcile)
	s.mux.HandleFunc("GET /admin/stats", s.stats)

	// Audit trail of recovery decisions (stored in the event store).
	s.mux.HandleFunc("POST /transfers/{id}/recovery-events", s.addRecoveryEvent)
	s.mux.HandleFunc("GET /transfers/{id}/recovery-events", s.listRecoveryEvents)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// ---------- JSON shapes ----------

type walletResponse struct {
	ID           string `json:"id"`
	Owner        string `json:"owner"`
	BalancePaise int64  `json:"balance_paise"`
	Version      int64  `json:"version"`
}

type moneyResponse struct {
	walletResponse
	Applied bool `json:"applied"` // false = repeat of an earlier request, nothing changed
}

type openRequest struct {
	Owner string `json:"owner"`
}

type moneyRequest struct {
	AmountPaise int64 `json:"amount_paise"`
}

func toResponse(w *domain.Wallet) walletResponse {
	return walletResponse{ID: w.ID, Owner: w.Owner, BalancePaise: w.Balance, Version: w.Version}
}

// ---------- handlers ----------

func (s *Server) openWallet(w http.ResponseWriter, r *http.Request) {
	var req openRequest
	if !decode(w, r, &req) {
		return
	}
	wallet, err := s.svc.OpenWallet(r.Context(), req.Owner)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, toResponse(wallet))
}

func (s *Server) getWallet(w http.ResponseWriter, r *http.Request) {
	id, ok := walletID(w, r)
	if !ok {
		return
	}
	wallet, err := s.svc.Get(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toResponse(wallet))
}

type moneyFunc func(ctx context.Context, id string, amount int64, key string) (*domain.Wallet, bool, error)

// money builds the credit and debit handlers (they differ only in the service call).
func (s *Server) money(op moneyFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := walletID(w, r)
		if !ok {
			return
		}
		key := r.Header.Get(IdempotencyHeader)
		if key == "" {
			writeError(w, domain.ErrMissingKey)
			return
		}
		if len(key) > 200 {
			writeJSON(w, http.StatusBadRequest, errorBody{"idempotency key too long (max 200)"})
			return
		}
		var req moneyRequest
		if !decode(w, r, &req) {
			return
		}
		wallet, applied, err := op(r.Context(), id, req.AmountPaise, key)
		if err != nil {
			writeError(w, err)
			return
		}
		if !applied {
			w.Header().Set("Idempotent-Replayed", "true")
		}
		writeJSON(w, http.StatusOK, moneyResponse{walletResponse: toResponse(wallet), Applied: applied})
	}
}

type transferRequest struct {
	FromWallet  string `json:"from_wallet"`
	ToWallet    string `json:"to_wallet"`
	AmountPaise int64  `json:"amount_paise"`
}

// startTransfer runs the whole saga before replying. 201 = new transfer,
// 200 = repeat of an earlier request (same Idempotency-Key). The outcome is in "status".
func (s *Server) startTransfer(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get(IdempotencyHeader)
	if len(key) > 200 {
		writeJSON(w, http.StatusBadRequest, errorBody{"idempotency key too long (max 200)"})
		return
	}
	var req transferRequest
	if !decode(w, r, &req) {
		return
	}
	for _, id := range []string{req.FromWallet, req.ToWallet} {
		if _, err := uuid.Parse(id); err != nil {
			writeError(w, domain.ErrWalletNotFound)
			return
		}
	}
	sg, created, err := s.orch.Start(r.Context(), saga.TransferRequest{
		From: req.FromWallet, To: req.ToWallet, Amount: req.AmountPaise, Key: key,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
		w.Header().Set("Idempotent-Replayed", "true")
	}
	writeJSON(w, status, sg)
}

func (s *Server) getTransfer(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, saga.ErrNotFound)
		return
	}
	sg, err := s.orch.Get(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sg)
}

// ---------- recovery ----------

// executeAction is THE door for changing a STUCK transfer.
// Body: {"action":"retry_step|compensate|reroute|partial_refund|escalate",
//
//	"amount_paise":N, "rail":"backup", "reason":"...", "actor":"ai"}
func (s *Server) executeAction(w http.ResponseWriter, r *http.Request) {
	id, ok := transferID(w, r)
	if !ok {
		return
	}
	var a saga.Action
	if !decode(w, r, &a) {
		return
	}
	sg, err := s.orch.Execute(r.Context(), id, a)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sg)
}

// transferMoney: what actually happened in the ledger for this transfer.
func (s *Server) transferMoney(w http.ResponseWriter, r *http.Request) {
	id, ok := transferID(w, r)
	if !ok {
		return
	}
	sg, err := s.orch.Get(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	ms, err := s.orch.MoneyState(r.Context(), sg)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ms)
}

// transferRail: ask the payment rail what really happened to each payment of this transfer.
func (s *Server) transferRail(w http.ResponseWriter, r *http.Request) {
	id, ok := transferID(w, r)
	if !ok {
		return
	}
	sg, err := s.orch.Get(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	rs, ok := s.orch.Rail().(saga.RailStatus)
	if !ok {
		writeJSON(w, http.StatusNotImplemented, errorBody{"payment rail has no status API"})
		return
	}
	type refState struct {
		Ref   string `json:"rail_ref"`
		State string `json:"state"`
	}
	var out []refState
	for _, ref := range sg.RailRefs() {
		st, err := rs.Status(r.Context(), ref)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, errorBody{err.Error()})
			return
		}
		out = append(out, refState{ref, st})
	}
	writeJSON(w, http.StatusOK, out)
}

// listTransfers: GET /transfers?status=STUCK&since=2026-10-24T00:00:00Z&limit=50
func (s *Server) listTransfers(w http.ResponseWriter, r *http.Request) {
	since, ok := parseSince(w, r)
	if !ok {
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 100
	}
	list, err := s.orch.Store().List(r.Context(), r.URL.Query().Get("status"), since, limit)
	if err != nil {
		writeError(w, err)
		return
	}
	if list == nil {
		list = []*saga.Saga{}
	}
	writeJSON(w, http.StatusOK, list)
}

// ---------- experiment control ----------

func (s *Server) getPolicy(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"policy": s.orch.Policy()})
}

// setPolicy: {"policy":"refund|retry|escalate"}; applies to NEW transfers.
func (s *Server) setPolicy(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Policy string `json:"policy"`
	}
	if !decode(w, r, &body) {
		return
	}
	if err := s.orch.SetPolicy(body.Policy); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"policy": body.Policy})
}

// invariants: GET /admin/invariants?since=...&saga_id=...&rail=false
func (s *Server) invariants(w http.ResponseWriter, r *http.Request) {
	since, ok := parseSince(w, r)
	if !ok {
		return
	}
	opt := audit.Options{Since: since, SagaID: r.URL.Query().Get("saga_id"), CheckRail: r.URL.Query().Get("rail") != "false"}
	rep, err := s.auditor.Check(r.Context(), opt)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

// reconcile: find completed transfers whose rail payment was later reversed; mark them STUCK.
func (s *Server) reconcile(w http.ResponseWriter, r *http.Request) {
	since, ok := parseSince(w, r)
	if !ok {
		return
	}
	flagged, err := s.orch.Reconcile(r.Context(), since)
	if err != nil {
		writeError(w, err)
		return
	}
	if flagged == nil {
		flagged = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"flagged": flagged})
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	st := &s.svc.Stats
	writeJSON(w, http.StatusOK, map[string]any{
		"policy":          s.orch.Policy(),
		"snapshot_hits":   st.SnapshotHits.Load(),
		"snapshot_misses": st.SnapshotMisses.Load(),
		"snapshot_errors": st.SnapshotErrors.Load(),
		"events_replayed": st.EventsReplayed.Load(),
	})
}

// addRecoveryEvent: {"type":"RecoveryProposed|RecoveryVerified|RecoveryRejected|RecoveryExecuted","data":{...}}
func (s *Server) addRecoveryEvent(w http.ResponseWriter, r *http.Request) {
	if s.reclog == nil {
		writeJSON(w, http.StatusNotImplemented, errorBody{"recovery log not enabled"})
		return
	}
	id, ok := transferID(w, r)
	if !ok {
		return
	}
	var body struct {
		Type string         `json:"type"`
		Data map[string]any `json:"data"`
	}
	if !decode(w, r, &body) {
		return
	}
	if !recoverylog.ValidType(body.Type) {
		writeJSON(w, http.StatusBadRequest, errorBody{recoverylog.ErrInvalidType.Error()})
		return
	}
	if _, err := s.orch.Get(r.Context(), id); err != nil {
		writeError(w, err)
		return
	}
	v, err := s.reclog.Append(r.Context(), id, body.Type, body.Data)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"stream": recoverylog.StreamID(id), "version": v})
}

func (s *Server) listRecoveryEvents(w http.ResponseWriter, r *http.Request) {
	if s.reclog == nil {
		writeJSON(w, http.StatusNotImplemented, errorBody{"recovery log not enabled"})
		return
	}
	id, ok := transferID(w, r)
	if !ok {
		return
	}
	evs, err := s.reclog.List(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	type out struct {
		Version   int64           `json:"version"`
		Type      string          `json:"type"`
		Data      json.RawMessage `json:"data"`
		CreatedAt time.Time       `json:"created_at"`
	}
	list := make([]out, 0, len(evs))
	for _, e := range evs {
		list = append(list, out{e.Version, e.Type, e.Payload, e.CreatedAt})
	}
	writeJSON(w, http.StatusOK, list)
}

func transferID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, saga.ErrNotFound)
		return "", false
	}
	return id, true
}

func parseSince(w http.ResponseWriter, r *http.Request) (time.Time, bool) {
	v := r.URL.Query().Get("since")
	if v == "" {
		return time.Time{}, true
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{"since must be RFC3339, e.g. 2026-10-24T10:00:00Z"})
		return time.Time{}, false
	}
	return t, true
}

// ---------- helpers ----------

type errorBody struct {
	Error string `json:"error"`
}

func walletID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeJSON(w, http.StatusNotFound, errorBody{domain.ErrWalletNotFound.Error()})
		return "", false
	}
	return id, true
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{"invalid JSON body: " + err.Error()})
		return false
	}
	return true
}

// writeError maps domain errors to HTTP status codes.
func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, domain.ErrWalletNotFound),
		errors.Is(err, saga.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, saga.ErrInvalidTransfer),
		errors.Is(err, saga.ErrInvalidAction),
		errors.Is(err, domain.ErrInvalidAmount),
		errors.Is(err, domain.ErrInvalidOwner),
		errors.Is(err, domain.ErrMissingKey):
		status = http.StatusBadRequest
	case errors.Is(err, domain.ErrIdempotencyMismatch),
		errors.Is(err, saga.ErrIdempotencyMismatch),
		errors.Is(err, saga.ErrNotActionable):
		status = http.StatusConflict
	case errors.Is(err, domain.ErrInsufficientFunds):
		status = http.StatusUnprocessableEntity
	case errors.Is(err, ledger.ErrBusy):
		status = http.StatusServiceUnavailable
	}
	msg := err.Error()
	if status == http.StatusInternalServerError {
		log.Printf("internal error: %v", err)
		msg = "internal error"
	}
	writeJSON(w, status, errorBody{msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
