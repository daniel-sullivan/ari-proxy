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

func TestCloseMarksClosing(t *testing.T) {
	// Close() must set the closing flag (before touching the conn) so the
	// ClosedHandler skips its os.Exit(1) on a graceful teardown. conn is nil
	// here, so Close() is a no-op beyond setting the flag.
	n := &NatsBus{}
	assert.False(t, n.closing.Load(), "closing must start false")
	n.Close()
	assert.True(t, n.closing.Load(), "Close() must set closing=true so the ClosedHandler skips os.Exit(1)")
}
