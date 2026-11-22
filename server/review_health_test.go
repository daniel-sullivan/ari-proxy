package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/CyCoreSystems/ari-proxy/v5/server/dialog"
	"github.com/CyCoreSystems/ari/v5"
	"github.com/CyCoreSystems/ari/v5/client/arimocks"
	"github.com/CyCoreSystems/ari/v5/client/native"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClock is a manually advanced clock.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// fakeProber records probes. onProbe, when set, runs inside Probe, before
// Asterisk would reply, to model what happens while the request is in flight.
type fakeProber struct {
	mu      sync.Mutex
	nonces  []string
	err     error
	onProbe func(nonce string)
}

func (p *fakeProber) Probe(_ context.Context, nonce string) error {
	p.mu.Lock()
	p.nonces = append(p.nonces, nonce)
	hook, err := p.onProbe, p.err
	p.mu.Unlock()
	if hook != nil {
		hook(nonce)
	}
	return err
}

func (p *fakeProber) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.nonces)
}

func (p *fakeProber) last() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.nonces[len(p.nonces)-1]
}

func probeEvent(nonce string) ari.Event {
	return &ari.ChannelUserevent{Eventname: livenessEventName, Userevent: map[string]interface{}{"nonce": nonce}}
}

type healthFixture struct {
	s     *Server
	bus   *recordingBus
	clock *fakeClock
}

func newHealthFixture(t *testing.T, entityID string, prober LivenessProber, connected bool) healthFixture {
	t.Helper()
	ast := &arimocks.Asterisk{}
	info := &ari.AsteriskInfo{}
	info.SystemInfo.EntityID = entityID
	ast.On("Info", (*ari.Key)(nil)).Return(info, nil)
	native := &arimocks.Client{}
	native.On("Asterisk").Return(ast)
	native.On("Connected").Return(connected)
	bus := &recordingBus{}
	s := testServer(native, bus)
	s.Dialog = dialog.NewMemManager()
	if prober != nil {
		s.Prober = prober
	}
	clock := &fakeClock{now: time.Unix(1_800_000_000, 0)}
	s.now = clock.Now
	return healthFixture{s: s, bus: bus, clock: clock}
}

// tick advances the clock by one watchdog interval and runs the check.
func (f healthFixture) tick() bool {
	f.clock.Advance(livenessDeliveryWindow)
	return f.s.checkHealth(context.Background())
}

func TestCheckHealthLiveness(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "supplied client without prober: connected is healthy", run: func(t *testing.T) {
			f := newHealthFixture(t, testNode, nil, true)
			assert.True(t, f.tick())
		}},
		{name: "supplied client without prober: disconnected is a failure", run: func(t *testing.T) {
			f := newHealthFixture(t, testNode, nil, false)
			assert.False(t, f.tick())
		}},
		{name: "entity change is a failure", run: func(t *testing.T) {
			f := newHealthFixture(t, "other-node", &fakeProber{}, true)
			assert.False(t, f.tick())
		}},
		{name: "delivered probes keep the proxy healthy and stay off the bus", run: func(t *testing.T) {
			p := &fakeProber{}
			f := newHealthFixture(t, testNode, p, true)
			for i := 0; i < 3; i++ {
				require.True(t, f.tick())
				f.s.handleEvent(probeEvent(p.last()))
			}
			assert.Equal(t, 3, p.count())
			assert.Empty(t, f.bus.eventSubjects())
		}},
		{name: "undelivered probe is lost once its window from acceptance has passed", run: func(t *testing.T) {
			p := &fakeProber{}
			f := newHealthFixture(t, testNode, p, true)
			require.True(t, f.tick())
			f.s.handleEvent(&ari.StasisStart{Channel: ari.ChannelData{ID: "c1"}}) // other traffic proves nothing
			assert.False(t, f.tick())
		}},
		{name: "probe accepted just before the next tick gets its full window", run: func(t *testing.T) {
			p := &fakeProber{}
			f := newHealthFixture(t, testNode, p, true)
			// Asterisk accepts the probe just before the next tick is due.
			p.onProbe = func(string) { f.clock.Advance(livenessDeliveryWindow - time.Nanosecond) }
			require.True(t, f.tick())
			p.onProbe = nil

			f.clock.Advance(time.Nanosecond - livenessDeliveryWindow) // the next tick lands on schedule
			assert.True(t, f.tick(), "the window is measured from acceptance")
			assert.Equal(t, 1, p.count(), "no new probe while the first is awaited")

			f.s.handleEvent(probeEvent(p.last()))
			assert.True(t, f.tick())
			assert.Equal(t, 2, p.count())
		}},
		{name: "late-accepted probe still fails when its own window passes", run: func(t *testing.T) {
			p := &fakeProber{}
			f := newHealthFixture(t, testNode, p, true)
			p.onProbe = func(string) { f.clock.Advance(livenessDeliveryWindow - time.Nanosecond) }
			require.True(t, f.tick())
			p.onProbe = nil
			f.clock.Advance(time.Nanosecond - livenessDeliveryWindow)
			require.True(t, f.tick())
			assert.False(t, f.tick())
		}},
		{name: "delivery that races the probe reply counts", run: func(t *testing.T) {
			p := &fakeProber{}
			f := newHealthFixture(t, testNode, p, true)
			p.onProbe = func(nonce string) { f.s.handleEvent(probeEvent(nonce)) }
			require.True(t, f.tick())
			assert.True(t, f.tick())
			assert.Equal(t, 2, p.count())
		}},
		{name: "stale nonce is not delivery", run: func(t *testing.T) {
			p := &fakeProber{}
			f := newHealthFixture(t, testNode, p, true)
			require.True(t, f.tick())
			first := p.last()
			f.s.handleEvent(probeEvent(first))
			require.True(t, f.tick())
			f.s.handleEvent(probeEvent(first))
			assert.False(t, f.tick())
		}},
		{name: "unregistered application is a failure", run: func(t *testing.T) {
			p := &fakeProber{err: fmt.Errorf("send liveness probe: %w", ErrApplicationNotRegistered)}
			f := newHealthFixture(t, testNode, p, true)
			assert.False(t, f.tick())
		}},
		{name: "transport failure is not counted and the next tick probes again", run: func(t *testing.T) {
			p := &fakeProber{err: errors.New("dial tcp: connection refused")}
			f := newHealthFixture(t, testNode, p, true)
			assert.True(t, f.tick())
			assert.True(t, f.tick())
			assert.Equal(t, 2, p.count())
		}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestEventSubscriptionCloseEndsProcess(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		cancel   bool
		wantCode []int
	}{
		{name: "closed while running", wantCode: []int{1}},
		{name: "closed during stop", cancel: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			events := make(chan ari.Event)
			close(events)
			sub := &arimocks.Subscription{}
			sub.On("Events").Return((<-chan ari.Event)(events))
			sub.On("Cancel").Return()
			bus := &arimocks.Bus{}
			bus.On("Subscribe", (*ari.Key)(nil), ari.Events.All).Return(sub)
			native := &arimocks.Client{}
			native.On("Bus").Return(bus)
			s := testServer(native, &recordingBus{})
			var codes []int
			s.exit = func(code int) { codes = append(codes, code) }

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.cancel {
				cancel()
			}
			s.runEventHandler(ctx)

			assert.Equal(t, tt.wantCode, codes)
		})
	}
}

func TestARIUserEventProber(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		status         int
		closed         bool
		wantErr        bool
		wantUnregister bool
	}{
		{name: "accepted", status: http.StatusNoContent},
		{name: "application not registered", status: http.StatusNotFound, wantErr: true, wantUnregister: true},
		{name: "server error", status: http.StatusInternalServerError, wantErr: true},
		{name: "transport failure", closed: true, wantErr: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/ari/events/user/"+livenessEventName, r.URL.Path)
				assert.Equal(t, testApp, r.URL.Query().Get("application"))
				user, pass, ok := r.BasicAuth()
				assert.True(t, ok)
				assert.Equal(t, "u", user)
				assert.Equal(t, "p", pass)
				var body struct {
					Variables map[string]string `json:"variables"`
				}
				assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				assert.Equal(t, "n1", body.Variables["nonce"])
				w.WriteHeader(tt.status)
			}))
			p := &ariUserEventProber{baseURL: srv.URL + "/ari/", application: testApp, username: "u", password: "p", client: srv.Client()}
			if tt.closed {
				srv.Close()
			} else {
				defer srv.Close()
			}

			err := p.Probe(context.Background(), "n1")

			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.wantUnregister, errors.Is(err, ErrApplicationNotRegistered))
		})
	}
}

// TestUserEventProberUsesResolvedOptions covers Listen with nil ARI options:
// Listen allocates an empty options struct, native.New resolves defaults and
// ARI_* environment settings into it, and the prober is built from that same
// struct.
func TestUserEventProberUsesResolvedOptions(t *testing.T) {
	t.Setenv("ARI_URL", "http://asterisk.test:8088/ari")
	t.Setenv("ARI_USERNAME", "env-user")
	t.Setenv("ARI_PASSWORD", "env-pass")
	t.Setenv("ARI_APPLICATION", testApp)

	opts := &native.Options{}
	native.New(opts)
	p := newUserEventProber(opts, testApp)

	assert.Equal(t, "http://asterisk.test:8088/ari", p.baseURL)
	assert.Equal(t, testApp, p.application)
	assert.Equal(t, "env-user", p.username)
	assert.Equal(t, "env-pass", p.password)
	assert.NotNil(t, p.client)
}
