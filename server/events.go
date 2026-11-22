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

// cleanupDialogsForEvent unbinds dialog bindings for entities which have reached
// end-of-life. It is conservative by design: it only unbinds on definitive
// "destroyed"/"ended" events, using the keys the ARI event itself reports for the
// destroyed entity, so a live dialog is never unbound. This is what keeps a
// vanished manager's bindings from leaking when running 2+ managers.
func (s *Server) cleanupDialogsForEvent(e ari.Event) {
	switch e.(type) {
	case *ari.StasisEnd,
		*ari.ChannelDestroyed,
		*ari.BridgeDestroyed,
		*ari.PlaybackFinished,
		*ari.RecordingFinished:
		for _, k := range e.Keys() {
			if k == nil || k.Kind == "" || k.ID == "" {
				continue
			}
			s.Log.Debug("unbinding dialogs for ended entity", "kind", k.Kind, "id", k.ID)
			s.Dialog.Unbind(k.Kind, k.ID)
		}
	}
}
