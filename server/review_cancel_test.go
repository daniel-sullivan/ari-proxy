package server

import (
	"context"
	"testing"
	"time"

	"github.com/CyCoreSystems/ari-proxy/v5/server/dialog"
	"github.com/CyCoreSystems/ari/v5"
	"github.com/CyCoreSystems/ari/v5/client/arimocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestWatchdogTickWithInfoInFlight(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		cancel    bool
		wantCodes []int
	}{
		{name: "stopped server does not end the embedding process", cancel: true},
		{name: "running server ends the process when the window closes", wantCodes: []int{1}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f, _ := infoFixture(t, 0, &fakeProber{}, true)
			window := make(chan time.Time, 1)
			f.s.after = func(time.Duration) <-chan time.Time { return window }
			var codes []int
			f.s.exit = func(code int) { codes = append(codes, code) }

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan bool)
			go func() { done <- f.s.watchdogTick(ctx) }()

			if tt.cancel {
				cancel()
			}
			// The window closes as well, so the tick always ends; with a
			// cancelled context neither event may end the process.
			window <- time.Time{}

			assert.False(t, <-done)
			assert.Equal(t, tt.wantCodes, codes)
		})
	}
}

// TestWatchdogTickCancelWakesInfoWait keeps the liveness window open, so only
// cancellation can wake the info wait.
func TestWatchdogTickCancelWakesInfoWait(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	ast := &arimocks.Asterisk{}
	ast.On("Info", (*ari.Key)(nil)).Run(func(mock.Arguments) {
		close(entered)
		<-release
	}).Return(&ari.AsteriskInfo{}, nil).Once()
	native := &arimocks.Client{}
	native.On("Asterisk").Return(ast)
	s := testServer(native, &recordingBus{})
	s.Dialog = dialog.NewMemManager()
	s.Prober = &fakeProber{}
	s.now = (&fakeClock{now: time.Unix(1_800_000_000, 0)}).Now
	s.after = neverFires
	var codes []int
	s.exit = func(code int) { codes = append(codes, code) }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan bool)
	go func() { done <- s.watchdogTick(ctx) }()

	<-entered
	cancel()

	assert.False(t, <-done)
	assert.Empty(t, codes)
}
