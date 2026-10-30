package main

// Fake payment rail with deterministic fault injection.
//
// What each mode does (the "ground truth" the benchmark scores against):
//
//	mode       HTTP reply             what really happened    correct recovery
//	none       200 PAID               PAID                    continue
//	reject     422 limit exceeded     REJECTED                refund sender
//	timeout    hangs past client      PAID (reply lost)       continue (do NOT refund)
//	duplicate  409 "duplicate"        PAID                    continue (do NOT refund)
//	partial    200 PAID               PAID, then REVERSED     refund sender later
//	unknown    500 strange text       PAID or REJECTED        check GET /payments/{id}, then decide
//
// A repeat call for the same saga_id never charges twice: it returns the
// stored outcome. So "retry the rail" or "check status" reveals the truth.

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Mode string

const (
	ModeNone      Mode = "none"
	ModeTimeout   Mode = "timeout"
	ModeReject    Mode = "reject"
	ModeDuplicate Mode = "duplicate"
	ModePartial   Mode = "partial"
	ModeUnknown   Mode = "unknown"
)

var faultModes = []Mode{ModeDuplicate, ModePartial, ModeReject, ModeTimeout, ModeUnknown} // sorted: keeps random picks deterministic

func validMode(m Mode) bool {
	return m == ModeNone || m == ModeTimeout || m == ModeReject || m == ModeDuplicate || m == ModePartial || m == ModeUnknown
}

// Payment states
const (
	StatePaid     = "PAID"
	StateRejected = "REJECTED"
	StateReversed = "REVERSED"
)

type Config struct {
	Seed            int64            `json:"seed"`
	FaultRate       float64          `json:"fault_rate"` // 0..1 chance a new payment gets a random fault
	Weights         map[Mode]float64 `json:"weights"`    // relative chance of each fault mode
	TimeoutDelayMs  int64            `json:"timeout_delay_ms"`
	ReversalDelayMs int64            `json:"reversal_delay_ms"`
}

func DefaultConfig() Config {
	return Config{
		Seed: 42, FaultRate: 0,
		Weights:        map[Mode]float64{ModeTimeout: 1, ModeReject: 1, ModeDuplicate: 1, ModePartial: 1, ModeUnknown: 1},
		TimeoutDelayMs: 5000, ReversalDelayMs: 3000,
	}
}

type PayRequest struct {
	SagaID string `json:"saga_id"`
	From   string `json:"from_wallet"`
	To     string `json:"to_wallet"`
	Amount int64  `json:"amount_paise"`
}

type Payment struct {
	SagaID    string    `json:"saga_id"`
	From      string    `json:"from_wallet"`
	To        string    `json:"to_wallet"`
	Amount    int64     `json:"amount_paise"`
	Mode      Mode      `json:"mode"`
	State     string    `json:"state"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type CallLog struct {
	At         time.Time `json:"at"`
	Seq        int64     `json:"seq"`
	SagaID     string    `json:"saga_id"`
	Amount     int64     `json:"amount_paise"`
	Mode       Mode      `json:"mode"`
	Source     string    `json:"source"` // header, queue, random, repeat
	Repeat     bool      `json:"repeat"`
	HTTPStatus int       `json:"http_status"`
	State      string    `json:"state"`
	LatencyMs  int64     `json:"latency_ms"`
}

const maxCallLog = 10_000

type Rail struct {
	mu       sync.Mutex
	cfg      Config
	rng      *rand.Rand
	seq      int64
	queue    []Mode
	byAmount map[int64]Mode // amount -> fault for the FIRST payment with that amount (benchmark)
	payments map[string]*Payment
	calls    []CallLog
	logOut   io.Writer // one JSON line per call; may be nil
	now      func() time.Time
}

func NewRail(cfg Config, logOut io.Writer) *Rail {
	r := &Rail{logOut: logOut, now: time.Now}
	r.reset(cfg)
	return r
}

// reset clears all state and restarts the random sequence from cfg.Seed. Caller holds no lock.
func (r *Rail) reset(cfg Config) {
	r.cfg = cfg
	r.rng = rand.New(rand.NewSource(cfg.Seed))
	r.seq = 0
	r.queue = nil
	r.byAmount = map[int64]Mode{}
	r.payments = map[string]*Payment{}
	r.calls = nil
}

func (r *Rail) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]string{"service": "fake-rail", "status": "ok"})
	})
	mux.HandleFunc("POST /pay", r.handlePay)
	mux.HandleFunc("GET /payments/{saga_id}", r.handleGetPayment)
	mux.HandleFunc("GET /admin/config", r.handleGetConfig)
	mux.HandleFunc("POST /admin/config", r.handleSetConfig)
	mux.HandleFunc("POST /admin/faults", r.handleQueueFaults)
	mux.HandleFunc("POST /admin/reset", r.handleReset)
	mux.HandleFunc("GET /admin/calls", r.handleCalls)
	mux.HandleFunc("GET /admin/stats", r.handleStats)
	return mux
}

// ---------- POST /pay ----------

func (r *Rail) handlePay(w http.ResponseWriter, req *http.Request) {
	start := r.now()
	var body PayRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, req.Body, 1<<16)).Decode(&body); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if body.SagaID == "" || body.Amount <= 0 {
		writeJSON(w, 400, map[string]string{"error": "saga_id and a positive amount_paise are required"})
		return
	}

	r.mu.Lock()
	r.seq++
	seq := r.seq

	// Repeat call: return the stored outcome, never charge twice.
	if p, ok := r.payments[body.SagaID]; ok {
		state := p.State
		r.mu.Unlock()
		status := replyForState(w, state, body.SagaID)
		r.logCall(CallLog{At: start, Seq: seq, SagaID: body.SagaID, Amount: body.Amount, Mode: p.Mode,
			Source: "repeat", Repeat: true, HTTPStatus: status, State: state, LatencyMs: r.since(start)})
		return
	}

	mode, source := r.chooseMode(Mode(req.Header.Get("X-Fault-Mode")), body.Amount)
	state := StatePaid
	switch mode {
	case ModeReject:
		state = StateRejected
	case ModeUnknown:
		if r.rng.Float64() >= 0.5 { // the hidden truth behind the strange error
			state = StateRejected
		}
	}
	now := r.now()
	r.payments[body.SagaID] = &Payment{SagaID: body.SagaID, From: body.From, To: body.To, Amount: body.Amount,
		Mode: mode, State: state, CreatedAt: now, UpdatedAt: now}
	timeoutDelay := time.Duration(r.cfg.TimeoutDelayMs) * time.Millisecond
	reversalDelay := time.Duration(r.cfg.ReversalDelayMs) * time.Millisecond
	r.mu.Unlock()

	if mode == ModePartial {
		sagaID := body.SagaID
		time.AfterFunc(reversalDelay, func() { r.reverse(sagaID) })
	}

	var status int
	switch mode {
	case ModeNone, ModePartial:
		status = 200
		writeJSON(w, status, map[string]string{"status": StatePaid, "saga_id": body.SagaID})
	case ModeReject:
		status = 422
		writeJSON(w, status, map[string]string{"error": "limit exceeded: sender daily transfer limit reached"})
	case ModeDuplicate:
		status = 409
		writeJSON(w, status, map[string]string{"error": "DUPLICATE_TXN: request flagged as duplicate"})
	case ModeUnknown:
		status = 500
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(status)
		fmt.Fprintf(w, "ERR_%04d: settlement node ack mismatch (ref=0x%X, shard=%d)", 7000+seq%1000, seq*2654435761%0xFFFFFF, seq%7)
	case ModeTimeout:
		status = 200 // sent only if the client is still waiting, which it normally is not
		select {
		case <-time.After(timeoutDelay):
			writeJSON(w, status, map[string]string{"status": StatePaid, "saga_id": body.SagaID})
		case <-req.Context().Done():
			status = 0 // client gave up
		}
	}
	r.logCall(CallLog{At: start, Seq: seq, SagaID: body.SagaID, Amount: body.Amount, Mode: mode,
		Source: source, HTTPStatus: status, State: state, LatencyMs: r.since(start)})
}

// chooseMode: header beats queue beats seeded random. Caller holds r.mu.
func (r *Rail) chooseMode(header Mode, amount int64) (Mode, string) {
	if header != "" && validMode(header) {
		return header, "header"
	}
	// By amount: used once, so later payments for the same transfer (e.g. a reroute)
	// do not get the fault again and do not shift the faults of other transfers.
	if m, ok := r.byAmount[amount]; ok {
		delete(r.byAmount, amount)
		return m, "amount"
	}
	if len(r.queue) > 0 {
		m := r.queue[0]
		r.queue = r.queue[1:]
		return m, "queue"
	}
	if r.cfg.FaultRate <= 0 || r.rng.Float64() >= r.cfg.FaultRate {
		return ModeNone, "random"
	}
	total := 0.0
	for _, m := range faultModes {
		total += r.cfg.Weights[m]
	}
	if total <= 0 {
		return ModeNone, "random"
	}
	pick := r.rng.Float64() * total
	for _, m := range faultModes {
		pick -= r.cfg.Weights[m]
		if pick < 0 {
			return m, "random"
		}
	}
	return faultModes[len(faultModes)-1], "random"
}

func (r *Rail) reverse(sagaID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.payments[sagaID]; ok && p.State == StatePaid {
		p.State = StateReversed
		p.UpdatedAt = r.now()
		r.logLocked(CallLog{At: p.UpdatedAt, SagaID: sagaID, Amount: p.Amount, Mode: ModePartial,
			Source: "reversal", State: StateReversed})
	}
}

func replyForState(w http.ResponseWriter, state, sagaID string) int {
	switch state {
	case StatePaid:
		writeJSON(w, 200, map[string]any{"status": StatePaid, "saga_id": sagaID, "repeat": true})
		return 200
	case StateRejected:
		writeJSON(w, 422, map[string]string{"error": "limit exceeded: sender daily transfer limit reached"})
		return 422
	default: // REVERSED
		writeJSON(w, 409, map[string]string{"error": "payment was reversed by the receiving bank"})
		return 409
	}
}

// ---------- status + admin ----------

func (r *Rail) handleGetPayment(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	p, ok := r.payments[req.PathValue("saga_id")]
	var cp Payment
	if ok {
		cp = *p
	}
	r.mu.Unlock()
	if !ok {
		writeJSON(w, 404, map[string]string{"error": "no payment for this saga_id"})
		return
	}
	writeJSON(w, 200, cp)
}

func (r *Rail) handleGetConfig(w http.ResponseWriter, _ *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	writeJSON(w, 200, map[string]any{"config": r.cfg, "queued_faults": r.queue, "faults_by_amount": len(r.byAmount), "calls_so_far": r.seq})
}

// POST /admin/config replaces the config AND resets all state, so a run starts clean.
func (r *Rail) handleSetConfig(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	cfg := r.cfg
	r.mu.Unlock()
	if err := json.NewDecoder(req.Body).Decode(&cfg); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if cfg.FaultRate < 0 || cfg.FaultRate > 1 {
		writeJSON(w, 400, map[string]string{"error": "fault_rate must be between 0 and 1"})
		return
	}
	for m := range cfg.Weights {
		if !validMode(m) || m == ModeNone {
			writeJSON(w, 400, map[string]string{"error": "unknown fault mode in weights: " + string(m)})
			return
		}
	}
	r.mu.Lock()
	r.reset(cfg)
	r.mu.Unlock()
	writeJSON(w, 200, map[string]any{"config": cfg, "reset": true})
}

// POST /admin/faults
//
//	{"modes":["timeout","none","reject"]}  the next payments get these modes in order
//	{"by_amount":{"1001":"timeout"}}        the first payment of exactly 1001 paise gets "timeout"
func (r *Rail) handleQueueFaults(w http.ResponseWriter, req *http.Request) {
	var body struct {
		Modes    []Mode          `json:"modes"`
		ByAmount map[string]Mode `json:"by_amount"`
	}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	for _, m := range body.Modes {
		if !validMode(m) {
			writeJSON(w, 400, map[string]string{"error": "unknown mode: " + string(m)})
			return
		}
	}
	amounts := map[int64]Mode{}
	for k, m := range body.ByAmount {
		a, err := strconv.ParseInt(k, 10, 64)
		if err != nil || a <= 0 || !validMode(m) {
			writeJSON(w, 400, map[string]string{"error": "by_amount needs positive integer amounts and known modes: " + k})
			return
		}
		amounts[a] = m
	}
	r.mu.Lock()
	r.queue = append(r.queue, body.Modes...)
	for a, m := range amounts {
		r.byAmount[a] = m
	}
	n, na := len(r.queue), len(r.byAmount)
	r.mu.Unlock()
	writeJSON(w, 200, map[string]any{"queued": n, "by_amount": na})
}

func (r *Rail) handleReset(w http.ResponseWriter, _ *http.Request) {
	r.mu.Lock()
	r.reset(r.cfg)
	r.mu.Unlock()
	writeJSON(w, 200, map[string]any{"reset": true})
}

func (r *Rail) handleCalls(w http.ResponseWriter, req *http.Request) {
	limit, _ := strconv.Atoi(req.URL.Query().Get("limit"))
	r.mu.Lock()
	calls := r.calls
	if limit > 0 && limit < len(calls) {
		calls = calls[len(calls)-limit:]
	}
	out := append([]CallLog(nil), calls...)
	r.mu.Unlock()
	writeJSON(w, 200, out)
}

func (r *Rail) handleStats(w http.ResponseWriter, _ *http.Request) {
	r.mu.Lock()
	byMode := map[Mode]int{}
	byState := map[string]int{}
	for _, p := range r.payments {
		byMode[p.Mode]++
		byState[p.State]++
	}
	total := r.seq
	r.mu.Unlock()
	writeJSON(w, 200, map[string]any{"calls": total, "payments_by_mode": byMode, "payments_by_state": byState})
}

// ---------- logging ----------

func (r *Rail) logCall(c CallLog) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logLocked(c)
}

func (r *Rail) logLocked(c CallLog) {
	r.calls = append(r.calls, c)
	if len(r.calls) > maxCallLog {
		r.calls = r.calls[len(r.calls)-maxCallLog:]
	}
	if r.logOut != nil {
		line, _ := json.Marshal(c)
		r.logOut.Write(append(line, '\n'))
	}
}

func (r *Rail) since(t time.Time) int64 { return r.now().Sub(t).Milliseconds() }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
