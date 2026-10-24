package saga

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// PayRequest is what we send to the external payment rail.
// SagaID doubles as the rail's idempotency key.
type PayRequest struct {
	SagaID string `json:"saga_id"`
	From   string `json:"from_wallet"`
	To     string `json:"to_wallet"`
	Amount int64  `json:"amount_paise"`
}

// PaymentRail is the external service in step 2 (built for real on Day 4).
type PaymentRail interface {
	Pay(ctx context.Context, req PayRequest) error
}

// RailError is a failure reported by the rail.
type RailError struct {
	Status int
	Body   string
}

func (e *RailError) Error() string {
	return fmt.Sprintf("payment rail returned %d: %s", e.Status, e.Body)
}

// AlwaysOKRail is used when no RAIL_URL is configured.
type AlwaysOKRail struct{}

func (AlwaysOKRail) Pay(context.Context, PayRequest) error { return nil }

// HTTPRail calls POST {BaseURL}/pay.
type HTTPRail struct {
	BaseURL string
	Client  *http.Client
}

func NewHTTPRail(baseURL string, timeout time.Duration) *HTTPRail {
	return &HTTPRail{BaseURL: baseURL, Client: &http.Client{Timeout: timeout}}
}

func (r *HTTPRail) Pay(ctx context.Context, req PayRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, r.BaseURL+"/pay", bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", req.SagaID)
	resp, err := r.Client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("payment rail unreachable: %w", err) // includes timeouts
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	return &RailError{Status: resp.StatusCode, Body: string(bytes.TrimSpace(msg))}
}
