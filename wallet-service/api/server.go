// Package api is the HTTP layer of the wallet service.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/google/uuid"

	"ledgemind/wallet-service/domain"
	"ledgemind/wallet-service/ledger"
)

const IdempotencyHeader = "Idempotency-Key"

type Server struct {
	svc *ledger.Service
	mux *http.ServeMux
}

// NewServer registers all routes. health may be nil.
func NewServer(svc *ledger.Service, health http.HandlerFunc) *Server {
	s := &Server{svc: svc, mux: http.NewServeMux()}
	if health != nil {
		s.mux.HandleFunc("GET /health", health)
	}
	s.mux.HandleFunc("POST /wallets", s.openWallet)
	s.mux.HandleFunc("GET /wallets/{id}", s.getWallet)
	s.mux.HandleFunc("POST /wallets/{id}/credit", s.money(s.svc.Credit))
	s.mux.HandleFunc("POST /wallets/{id}/debit", s.money(s.svc.Debit))
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
	case errors.Is(err, domain.ErrWalletNotFound):
		status = http.StatusNotFound
	case errors.Is(err, domain.ErrInvalidAmount),
		errors.Is(err, domain.ErrInvalidOwner),
		errors.Is(err, domain.ErrMissingKey):
		status = http.StatusBadRequest
	case errors.Is(err, domain.ErrIdempotencyMismatch):
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
