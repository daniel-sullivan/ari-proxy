package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/CyCoreSystems/ari-proxy/v5/proxy"
	"github.com/CyCoreSystems/ari-proxy/v5/server/dialog"
	"github.com/CyCoreSystems/ari/v5"
	"github.com/CyCoreSystems/ari/v5/client/arimocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (b *recordingBus) PublishEvent(subject string, _ ari.Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, subject)
	return nil
}

func (b *recordingBus) eventSubjects() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.events...)
}

func TestMissingPayloadIsDenied(t *testing.T) {
	t.Parallel()
	for kind, op := range managerOperations {
		if op.payload == "" {
			continue
		}
		kind, op := kind, op
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			got, err := testProfile(fakeResolver{}).Authorize(context.Background(), nodeRoute(op.class), &proxy.Request{Kind: kind, Key: nodeKey("", "")})
			assert.Error(t, err)
			assert.Nil(t, got)
		})
	}
}

func TestHandleEventDeliversBeforeUnbinding(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		bind         map[string][2]string // dialog -> kind, id
		event        ari.Event
		wantSubjects []string
		wantBound    map[string][2]string // still bound after the event
		wantUnbound  [][2]string
	}{
		{
			name:         "stasis end reaches the channel dialog, then unbinds it",
			bind:         map[string][2]string{"d1": {ari.ChannelKey, "c1"}},
			event:        &ari.StasisEnd{Channel: ari.ChannelData{ID: "c1"}},
			wantSubjects: []string{"ari.event.etla.node-a", "ari.dialogevent.d1"},
			wantUnbound:  [][2]string{{ari.ChannelKey, "c1"}},
		},
		{
			name:         "channel destroyed reaches the channel dialog, then unbinds it",
			bind:         map[string][2]string{"d1": {ari.ChannelKey, "c1"}},
			event:        &ari.ChannelDestroyed{Channel: ari.ChannelData{ID: "c1"}},
			wantSubjects: []string{"ari.event.etla.node-a", "ari.dialogevent.d1"},
			wantUnbound:  [][2]string{{ari.ChannelKey, "c1"}},
		},
		{
			name: "bridge destroyed unbinds only the bridge",
			bind: map[string][2]string{
				"d2": {ari.BridgeKey, "b1"},
				"d3": {ari.ChannelKey, "c2"},
			},
			event:        &ari.BridgeDestroyed{Bridge: ari.BridgeData{ID: "b1", ChannelIDs: []string{"c2"}}},
			wantSubjects: []string{"ari.event.etla.node-a", "ari.dialogevent.d2", "ari.dialogevent.d3"},
			wantBound:    map[string][2]string{"d3": {ari.ChannelKey, "c2"}},
			wantUnbound:  [][2]string{{ari.BridgeKey, "b1"}},
		},
		{
			name:         "a live event keeps the binding",
			bind:         map[string][2]string{"d1": {ari.ChannelKey, "c1"}},
			event:        &ari.StasisStart{Channel: ari.ChannelData{ID: "c1"}},
			wantSubjects: []string{"ari.event.etla.node-a", "ari.dialogevent.d1"},
			wantBound:    map[string][2]string{"d1": {ari.ChannelKey, "c1"}},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bus := &recordingBus{}
			s := testServer(nil, bus)
			s.Dialog = dialog.NewMemManager()
			for d, b := range tt.bind {
				s.Dialog.Bind(d, b[0], b[1])
			}

			s.handleEvent(tt.event)

			assert.ElementsMatch(t, tt.wantSubjects, bus.eventSubjects())
			for d, b := range tt.wantBound {
				assert.Equal(t, []string{d}, s.Dialog.List(b[0], b[1]))
			}
			for _, b := range tt.wantUnbound {
				assert.Empty(t, s.Dialog.List(b[0], b[1]))
			}
		})
	}
}

type fakeProber struct {
	mu     sync.Mutex
	nonces []string
	err    error
}

func (p *fakeProber) Probe(_ context.Context, nonce string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nonces = append(p.nonces, nonce)
	return p.err
}

func (p *fakeProber) last() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.nonces[len(p.nonces)-1]
}

func probeEvent(nonce string) ari.Event {
	return &ari.ChannelUserevent{Eventname: livenessEventName, Userevent: map[string]interface{}{"nonce": nonce}}
}

func healthServer(t *testing.T, entityID string, prober LivenessProber) (*Server, *recordingBus) {
	t.Helper()
	ast := &arimocks.Asterisk{}
	info := &ari.AsteriskInfo{}
	info.SystemInfo.EntityID = entityID
	ast.On("Info", (*ari.Key)(nil)).Return(info, nil)
	native := &arimocks.Client{}
	native.On("Asterisk").Return(ast)
	bus := &recordingBus{}
	s := testServer(native, bus)
	s.Dialog = dialog.NewMemManager()
	if prober != nil {
		s.prober = prober
	}
	return s, bus
}

func TestCheckHealthLiveness(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "no prober only checks the entity", run: func(t *testing.T) {
			s, _ := healthServer(t, testNode, nil)
			assert.True(t, s.checkHealth(ctx))
			assert.True(t, s.checkHealth(ctx))
		}},
		{name: "entity change is a failure", run: func(t *testing.T) {
			s, _ := healthServer(t, "other-node", &fakeProber{})
			assert.False(t, s.checkHealth(ctx))
		}},
		{name: "delivered probes keep the proxy healthy", run: func(t *testing.T) {
			p := &fakeProber{}
			s, bus := healthServer(t, testNode, p)
			for i := 0; i < 3; i++ {
				require.True(t, s.checkHealth(ctx))
				s.handleEvent(probeEvent(p.last()))
			}
			assert.Len(t, p.nonces, 3)
			assert.Empty(t, bus.eventSubjects(), "liveness probes must not reach the bus")
		}},
		{name: "undelivered probe is a confirmed failure", run: func(t *testing.T) {
			p := &fakeProber{}
			s, _ := healthServer(t, testNode, p)
			require.True(t, s.checkHealth(ctx))
			s.handleEvent(&ari.StasisStart{Channel: ari.ChannelData{ID: "c1"}}) // other traffic proves nothing
			assert.False(t, s.checkHealth(ctx))
		}},
		{name: "stale nonce is not delivery", run: func(t *testing.T) {
			p := &fakeProber{}
			s, _ := healthServer(t, testNode, p)
			require.True(t, s.checkHealth(ctx))
			first := p.last()
			s.handleEvent(probeEvent(first))
			require.True(t, s.checkHealth(ctx))
			s.handleEvent(probeEvent(first))
			assert.False(t, s.checkHealth(ctx))
		}},
		{name: "unsent probe is not counted", run: func(t *testing.T) {
			p := &fakeProber{err: errors.New("ARI status 503")}
			s, _ := healthServer(t, testNode, p)
			assert.True(t, s.checkHealth(ctx))
			assert.True(t, s.checkHealth(ctx))
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
		name    string
		status  int
		wantErr bool
	}{
		{name: "accepted", status: http.StatusNoContent},
		{name: "rejected", status: http.StatusNotFound, wantErr: true},
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
			defer srv.Close()

			p := &ariUserEventProber{baseURL: srv.URL + "/ari/", application: testApp, username: "u", password: "p", client: srv.Client()}
			err := p.Probe(context.Background(), "n1")
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
