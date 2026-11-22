package messagebus

import (
	"errors"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func TestSubscriptionCancelStopsReconnect(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		cancelBefore bool
	}{
		{name: "before reconnect", cancelBefore: true},
		{name: "during retry wait"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sub := &RmqSubscription{done: make(chan struct{})}
			attempted := make(chan struct{}, 1)
			finished := make(chan struct{})
			if tc.cancelBefore {
				if err := sub.Unsubscribe(); err != nil {
					t.Fatal(err)
				}
			}
			go func() {
				defer close(finished)
				sub.retrySubscription(func() (<-chan amqp091.Delivery, error) {
					attempted <- struct{}{}
					return nil, errors.New("disconnected")
				}, func(error) {})
			}()
			if !tc.cancelBefore {
				select {
				case <-attempted:
				case <-time.After(time.Second):
					t.Fatal("reconnect did not start")
				}
				if err := sub.Unsubscribe(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("reconnect did not stop")
			}
			select {
			case <-attempted:
				t.Fatal("unexpected reconnect attempt")
			default:
			}
		})
	}
}
