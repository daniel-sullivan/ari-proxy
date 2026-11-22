package server

import (
	"context"
	"errors"
	"testing"

	"github.com/CyCoreSystems/ari-proxy/v5/messagebus"
	"github.com/CyCoreSystems/ari/v5"
	"github.com/CyCoreSystems/ari/v5/client/arimocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeSubscription struct {
	unsubscribed int
	err          error
}

func (f *fakeSubscription) Unsubscribe() error {
	f.unsubscribed++
	return f.err
}

// subscribeBus fails the subscription at position failAt (0 ping, 1 requests,
// 2 first create) and records every subscription it handed out.
type subscribeBus struct {
	messagebus.Server
	failAt         int
	calls          int
	subs           []*fakeSubscription
	unsubscribeErr error
	subscribeErr   error
}

func (b *subscribeBus) next() (messagebus.Subscription, error) {
	n := b.calls
	b.calls++
	if n == b.failAt {
		return nil, b.subscribeErr
	}
	sub := &fakeSubscription{err: b.unsubscribeErr}
	b.subs = append(b.subs, sub)
	return sub, nil
}

func (b *subscribeBus) SubscribePing(string, messagebus.PingHandler) (messagebus.Subscription, error) {
	return b.next()
}

func (b *subscribeBus) SubscribeRequests([]string, messagebus.RequestHandler) (messagebus.Subscription, error) {
	return b.next()
}

func (b *subscribeBus) SubscribeCreateRequest(string, string, messagebus.RequestHandler) (messagebus.Subscription, error) {
	return b.next()
}

func TestListenEarlyReturnIsClean(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		infoErr  error
		failAt   int
		wantSubs int
	}{
		{name: "asterisk info fails before any subscription", infoErr: errors.New("connection refused"), failAt: -1},
		{name: "ping subscription fails", failAt: 0},
		{name: "request subscription fails after ping", failAt: 1, wantSubs: 1},
		{name: "create subscription fails after ping and requests", failAt: 2, wantSubs: 2},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			info := &ari.AsteriskInfo{}
			info.SystemInfo.EntityID = testNode
			ast := &arimocks.Asterisk{}
			ast.On("Info", (*ari.Key)(nil)).Return(info, tt.infoErr)
			native := &arimocks.Client{}
			native.On("Asterisk").Return(ast)
			native.On("ApplicationName").Return(testApp)
			startupErr := errors.New("subscribe failed")
			bus := &subscribeBus{failAt: tt.failAt, subscribeErr: startupErr, unsubscribeErr: errors.New("unsubscribe failed")}
			s := testServer(native, bus)

			var err error
			require.NotPanics(t, func() { err = s.listen(context.Background()) })

			assert.Error(t, err)
			if tt.infoErr != nil {
				assert.ErrorIs(t, err, tt.infoErr)
			} else {
				assert.ErrorIs(t, err, startupErr)
			}
			require.Len(t, bus.subs, tt.wantSubs)
			for i, sub := range bus.subs {
				assert.Equal(t, 1, sub.unsubscribed, "subscription %d is unsubscribed once", i)
			}
		})
	}
}
