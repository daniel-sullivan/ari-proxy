package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/CyCoreSystems/ari-proxy/v5/proxy"
	"github.com/CyCoreSystems/ari/v5"
	"github.com/CyCoreSystems/ari/v5/client/native"
)

// livenessEventName is the ARI user event the watchdog sends to its own
// application to prove the event websocket still delivers.
const livenessEventName = "ari-proxy-liveness"

// ErrApplicationNotRegistered reports that Asterisk no longer knows this
// proxy's ARI application: the event registration is lost.
var ErrApplicationNotRegistered = errors.New("ARI application not registered")

// LivenessProber asks Asterisk to deliver one liveness user event, carrying
// nonce, to this proxy's application over the event websocket.
type LivenessProber interface {
	Probe(ctx context.Context, nonce string) error
}

// ariUserEventProber posts the probe to ARI's /events/user endpoint. The
// native client's UserEvent always names a channel source, so the probe is
// sent directly with the proxy's own ARI connection settings.
type ariUserEventProber struct {
	baseURL     string
	application string
	username    string
	password    string
	client      *http.Client
}

// newUserEventProber builds the prober from ARI options that the native
// client has already resolved (defaults and ARI_* environment settings).
func newUserEventProber(opts *native.Options, application string) *ariUserEventProber {
	return &ariUserEventProber{
		baseURL:     opts.URL,
		application: application,
		username:    opts.Username,
		password:    opts.Password,
		client:      &http.Client{},
	}
}

func (p *ariUserEventProber) Probe(ctx context.Context, nonce string) error {
	target := fmt.Sprintf("%s/events/user/%s?application=%s",
		strings.TrimRight(p.baseURL, "/"), livenessEventName, url.QueryEscape(p.application))
	body, err := json.Marshal(map[string]any{"variables": map[string]string{"nonce": nonce}})
	if err != nil {
		return fmt.Errorf("encode liveness probe: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build liveness probe: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if p.username != "" {
		req.SetBasicAuth(p.username, p.password)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("send liveness probe: %w", err)
	}
	defer resp.Body.Close() // nolint: errcheck
	if resp.StatusCode == http.StatusNotFound {
		// ARI answers 404 when the target application is not registered.
		return fmt.Errorf("send liveness probe: %w", ErrApplicationNotRegistered)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("send liveness probe: ARI status %d", resp.StatusCode)
	}
	return nil
}

// livenessDeliveryWindow is how long after Asterisk accepts a probe the
// event websocket has to deliver it.
var livenessDeliveryWindow = proxy.EntityCheckInterval

type probeState int

const (
	probeIdle     probeState = iota // no probe outstanding: send one
	probeAwaiting                   // accepted, not yet delivered, window open
	probeLost                       // accepted and not delivered within the window
)

// livenessTracker records the outstanding probe nonce, when Asterisk accepted
// it, and whether the event websocket has delivered it.
type livenessTracker struct {
	mu          sync.Mutex
	outstanding string
	acceptedAt  time.Time
	seen        bool
}

// expect starts a new probe round. It is called before the probe is sent so
// a delivery that races the send's reply is still recorded.
func (l *livenessTracker) expect(nonce string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.outstanding, l.acceptedAt, l.seen = nonce, time.Time{}, false
}

// accepted records when Asterisk accepted the probe; the delivery window
// starts here.
func (l *livenessTracker) accepted(nonce string, at time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if nonce == l.outstanding {
		l.acceptedAt = at
	}
}

// clear abandons the outstanding probe, e.g. when it could not be sent.
func (l *livenessTracker) clear() {
	l.expect("")
}

// observe records a delivered probe event.
func (l *livenessTracker) observe(nonce string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if nonce != "" && nonce == l.outstanding {
		l.seen = true
	}
}

// state reports the outstanding probe's state at now.
func (l *livenessTracker) state(now time.Time, window time.Duration) probeState {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch {
	case l.outstanding == "" || l.seen || l.acceptedAt.IsZero():
		return probeIdle
	case now.Sub(l.acceptedAt) < window:
		return probeAwaiting
	default:
		return probeLost
	}
}

// livenessNonce extracts the nonce of a liveness probe event; ok is false for
// any other event.
func livenessNonce(e ari.Event) (nonce string, ok bool) {
	ue, isUser := e.(*ari.ChannelUserevent)
	if !isUser || ue.Eventname != livenessEventName {
		return "", false
	}
	vars, _ := ue.Userevent.(map[string]interface{})
	nonce, _ = vars["nonce"].(string)
	return nonce, true
}
