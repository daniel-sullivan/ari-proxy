package server

import (
	"context"
	"testing"

	"github.com/CyCoreSystems/ari-proxy/v5/proxy"
	"github.com/CyCoreSystems/ari-proxy/v5/server/dialog"
	"github.com/CyCoreSystems/ari/v5"
	"github.com/stretchr/testify/assert"
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
