package saga

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// PayRequest is what we send to the external payment rail. SagaID is the
// rail's idempotency key: the saga id, or "<id>:reroute:N" for a reroute.
type PayRequest struct {
	SagaID string `json:"saga_id"`
	From   string `json:"from_wallet"`
	To     string `json:"to_wallet"`
	Amount int64  `json:"amount_paise"`
}

// PaymentRail is the external service in step 2.
type PaymentRail interface {
	Pay(ctx context.Context, req PayRequest) error
}

// RailStatus asks the rail what really happened to a payment.
// Returns PAID, REJECTED, REVERSED or NONE (never seen).
type RailStatus interface {
	Status(ctx context.Context, ref string) (string, error)
}

const RailStateNone = "NONE"

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

func (AlwaysOKRail) Status(context.Context, string) (string, error) { return "PAID", nil }

// HTTPRail calls the fake-rail service.
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

func (r *HTTPRail) Status(ctx context.Context, ref string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.BaseURL+"/payments/"+url.PathEscape(ref), nil)
	if err != nil {
		return "", err
	}
	resp, err := r.Client.Do(req)
	if err != nil {
		return "", fmt.Errorf("payment rail unreachable: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return RailStateNone, nil
	}
	if resp.StatusCode != http.StatusOK {
		return "", &RailError{Status: resp.StatusCode}
	}
	var p struct {
		State string `json:"state"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return "", err
	}
	return p.State, nil
}
