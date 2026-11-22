package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
