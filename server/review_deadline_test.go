package server

import (
	"context"
	"testing"
	"time"

	"github.com/CyCoreSystems/ari-proxy/v5/server/dialog"
	"github.com/CyCoreSystems/ari/v5"
	"github.com/CyCoreSystems/ari/v5/client/arimocks"
	"github.com/CyCoreSystems/ari/v5/client/native"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// neverFires is a timer that does not fire, so a test proves its outcome by
// ordering alone.
func neverFires(time.Duration) <-chan time.Time { return nil }

// infoFixture returns a health fixture whose first Asterisk info requests
// (answered of them) succeed and whose later ones block until the test ends.
func infoFixture(t *testing.T, answered int, prober LivenessProber) (healthFixture, *arimocks.Asterisk) {
	t.Helper()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	info := &ari.AsteriskInfo{}
	info.SystemInfo.EntityID = testNode
	ast := &arimocks.Asterisk{}
	if answered > 0 {
		ast.On("Info", (*ari.Key)(nil)).Return(info, nil).Times(answered)
	}
	ast.On("Info", (*ari.Key)(nil)).Run(func(mock.Arguments) { <-release }).Return(info, nil)
	native := &arimocks.Client{}
	native.On("Asterisk").Return(ast)
	s := testServer(native, &recordingBus{})
	s.Dialog = dialog.NewMemManager()
	s.Prober = prober
	clock := &fakeClock{now: time.Unix(1_800_000_000, 0)}
	s.now = clock.Now
	s.after = neverFires
	return healthFixture{s: s, clock: clock}, ast
}

func TestCheckHealthInfoDeadline(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "hung info request is a failure when the liveness window closes", run: func(t *testing.T) {
			p := &fakeProber{}
			f, _ := infoFixture(t, 0, p)
			var asked []time.Duration
			f.s.after = func(d time.Duration) <-chan time.Time {
				asked = append(asked, d)
				fired := make(chan time.Time, 1)
				fired <- time.Time{}
				return fired
			}
			assert.False(t, f.tick())
			assert.Equal(t, []time.Duration{livenessDeliveryWindow}, asked, "the deadline is the liveness window")
			assert.Equal(t, 0, p.count(), "no probe after a failed check")
		}},
		{name: "lost probe is judged before the info request", run: func(t *testing.T) {
			p := &fakeProber{}
			f, ast := infoFixture(t, 1, p)
			require.True(t, f.tick())
			// The next info request would block for ever and the timer never
			// fires, so only the earlier probe check can end this tick.
			assert.False(t, f.tick())
			ast.AssertNumberOfCalls(t, "Info", 1)
		}},
		{name: "info answered within the window keeps the watchdog healthy", run: func(t *testing.T) {
			p := &fakeProber{}
			f, _ := infoFixture(t, 2, p)
			require.True(t, f.tick())
			f.s.handleEvent(probeEvent(p.last()))
			assert.True(t, f.tick())
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

func TestListenOnProber(t *testing.T) {
	t.Parallel()
	t.Run("non-native client needs a prober", func(t *testing.T) {
		t.Parallel()
		client := &arimocks.Client{}
		s := New()
		assert.Error(t, s.ListenOn(context.Background(), client, nil))
		assert.Empty(t, client.Calls, "nothing reaches the supplied client")
	})
	t.Run("native client gets a prober from its own settings", func(t *testing.T) {
		t.Parallel()
		client := native.New(&native.Options{Application: testApp, URL: "http://asterisk.test:8088/ari", Username: "u", Password: "p"})

		got, err := proberFor(client)
		require.NoError(t, err)

		p, ok := got.(*ariUserEventProber)
		require.True(t, ok)
		assert.Equal(t, "http://asterisk.test:8088/ari", p.baseURL)
		assert.Equal(t, testApp, p.application)
		assert.Equal(t, "u", p.username)
		assert.Equal(t, "p", p.password)
	})
}
