package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/CyCoreSystems/ari/v5"
)

// livenessEventName is the ARI user event the watchdog sends to its own
// application to prove the event websocket still delivers.
const livenessEventName = "ari-proxy-liveness"

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
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("send liveness probe: ARI status %d", resp.StatusCode)
	}
	return nil
}

// livenessTracker records the outstanding probe nonce and whether the event
// websocket has delivered it.
type livenessTracker struct {
	mu          sync.Mutex
	outstanding string
	seen        bool
}

// expect starts a new probe round.
func (l *livenessTracker) expect(nonce string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.outstanding, l.seen = nonce, false
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

// answered reports whether the outstanding probe, if any, was delivered.
func (l *livenessTracker) answered() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.outstanding == "" || l.seen
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
