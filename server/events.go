package server

import "github.com/CyCoreSystems/ari/v5"

func (s *Server) dialogsForEvent(e ari.Event) (ret []string) {
	for _, k := range e.Keys() {
		if k == nil {
			s.Log.Warn("received nil key for event", "event", e)
			continue
		}
		ret = append(ret, s.Dialog.List(k.Kind, k.ID)...)
	}
	return
}

// cleanupDialogsForEvent unbinds the dialogs of the one entity an end-of-life
// event reports as ended. Other entities named by the event, such as the
// channels still in a destroyed bridge, keep their bindings.
func (s *Server) cleanupDialogsForEvent(e ari.Event) {
	k := endedEntity(e)
	if k == nil || k.ID == "" {
		return
	}
	s.Log.Debug("unbinding dialogs for ended entity", "kind", k.Kind, "id", k.ID)
	s.Dialog.Unbind(k.Kind, k.ID)
}

// endedEntity returns the key of the entity an event reports as ended, or nil.
func endedEntity(e ari.Event) *ari.Key {
	switch v := e.(type) {
	case *ari.StasisEnd:
		return v.Key(ari.ChannelKey, v.Channel.ID)
	case *ari.ChannelDestroyed:
		return v.Key(ari.ChannelKey, v.Channel.ID)
	case *ari.BridgeDestroyed:
		return v.Key(ari.BridgeKey, v.Bridge.ID)
	case *ari.PlaybackFinished:
		return v.Key(ari.PlaybackKey, v.Playback.ID)
	case *ari.RecordingFinished:
		return v.Key(ari.LiveRecordingKey, v.Recording.Name)
	default:
		return nil
	}
}
