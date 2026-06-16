package messagebus

import (
	"testing"

	"github.com/inconshreveable/log15"
	"github.com/stretchr/testify/assert"
)

func TestConnectOptions(t *testing.T) {
	n := &NatsBus{Log: log15.New()}
	opts := n.connectOptions()

	// We pass MaxReconnects, ReconnectWait, ReconnectJitter, PingInterval,
	// MaxPingsOutstanding, and three connection handlers (Disconnect/Reconnect/
	// Closed) — eight options in total. This guards against an option being
	// dropped in a future edit.
	assert.Len(t, opts, 8, "expected the full set of resilience + handler options")
	for i, o := range opts {
		assert.NotNil(t, o, "option %d must not be nil", i)
	}
}

func TestIsConnectedNilConn(t *testing.T) {
	// IsConnected must be safe before Connect() and report false rather than
	// panicking on a nil connection.
	n := &NatsBus{}
	assert.False(t, n.IsConnected(), "a NatsBus with no connection is not connected")
}
