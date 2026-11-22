package server

import (
	"context"
	"errors"
	"github.com/CyCoreSystems/ari-proxy/v5/proxy"
	"testing"

	"time"

	"github.com/CyCoreSystems/ari-proxy/v5/client"
	"github.com/CyCoreSystems/ari/v5"
	"github.com/CyCoreSystems/ari/v5/rid"
	"github.com/nats-io/nats.go"
)

// allowAll admits every request unchanged; integration tests only.
type allowAll struct{}

func (allowAll) Authorize(_ context.Context, _ Route, req *proxy.Request) (*proxy.Request, error) {
	return req, nil
}

// unsentProber stands in for the ARI liveness probe in the integration
// suite, whose mock clients have no event websocket. A probe that is never
// sent is not counted, so the watchdog stays quiet.
type unsentProber struct{}

func (unsentProber) Probe(context.Context, string) error {
	return errors.New("integration suite: liveness probe not sent")
}

type srv struct {
	s *Server
}

func (s *srv) Start(ctx context.Context, t *testing.T, mockClient ari.Client, nc *nats.EncodedConn, completeCh chan struct{}) (ari.Client, error) {
	s.s = New()
	// The integration suite exercises the whole ARI surface.
	s.s.Authorizer = allowAll{}
	s.s.Prober = unsentProber{}
	// tests may run in parallel so we don't want two separate proxy servers to conflict.
	s.s.MBPrefix = rid.New("") + "."
	s.s.Application = "asdf"

	go func() {
		if err := s.s.ListenOn(ctx, mockClient, nc); err != nil {
			if err != context.Canceled {
				t.Errorf("Failed to start server: %s", err)
			}
		}
		close(completeCh)
	}()

	select {
	case <-s.s.Ready():
	case <-time.After(600 * time.Millisecond):
		return nil, errors.New("Timeout waiting for server ready")
	}

	cl, err := client.New(ctx, client.WithTimeoutRetries(4), client.WithPrefix(s.s.MBPrefix), client.WithApplication("asdf"))
	if err != nil {
		return nil, err
	}

	return cl, nil
}

func (s *srv) Ready() <-chan struct{} {
	return s.s.Ready()
}

func (s *srv) Close() error {
	return nil
}
