package messagebus

import (
	"errors"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func TestSubscriptionReconnectStopsOnClose(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		cancelBefore bool
		closeBus     bool
	}{
		{name: "unsubscribe before reconnect", cancelBefore: true},
		{name: "unsubscribe during retry wait"},
		{name: "close bus before reconnect", cancelBefore: true, closeBus: true},
		{name: "close bus during retry wait", closeBus: true},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &RabbitmqBus{done: make(chan struct{})}
			sub := &RmqSubscription{done: make(chan struct{})}
			t.Cleanup(func() {
				if err := sub.Unsubscribe(); err != nil {
					t.Error(err)
				}
			})
			cancel := func() {
				if tc.closeBus {
					bus.Close()
				} else if err := sub.Unsubscribe(); err != nil {
					t.Fatal(err)
				}
			}
			failed := make(chan struct{}, 1)
			finished := make(chan struct{})
			attempts := 0
			if tc.cancelBefore {
				cancel()
			}
			go func() {
				defer close(finished)
				sub.retrySubscription(bus.done, func() (<-chan amqp091.Delivery, error) {
					attempts++
					return nil, errors.New("disconnected")
				}, func(error) {
					if attempts == 1 {
						failed <- struct{}{}
					}
				})
			}()
			if !tc.cancelBefore {
				select {
				case <-failed:
				case <-time.After(time.Second):
					t.Fatal("first reconnect attempt did not fail")
				}
				cancel()
			}
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("reconnect did not stop")
			}
			wantAttempts := 0
			if !tc.cancelBefore {
				wantAttempts = 1
			}
			if attempts != wantAttempts {
				t.Fatalf("reconnect attempts = %d, want %d", attempts, wantAttempts)
			}
		})
	}
}
