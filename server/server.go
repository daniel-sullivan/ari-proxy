package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/CyCoreSystems/ari-proxy/v5/messagebus"
	"github.com/CyCoreSystems/ari-proxy/v5/proxy"
	"github.com/CyCoreSystems/ari-proxy/v5/server/dialog"
	"github.com/CyCoreSystems/ari/v5"
	"github.com/CyCoreSystems/ari/v5/client/native"
	"github.com/nats-io/nats.go"
	"github.com/rotisserie/eris"

	"github.com/inconshreveable/log15"
)

// Server describes the asterisk-facing ARI proxy server
type Server struct {
	// Application is the name of the ARI application of this server
	Application string

	// AsteriskID is the unique identifier for the Asterisk box
	// to which this server is connected.
	AsteriskID string

	// MBPrefix is the string which should be prepended to all MessageBus subjects, sending and receiving.  It defaults to "ari.".
	MBPrefix string

	// ari is the native Asterisk ARI client by which this proxy is directly connected
	ari ari.Client

	// Dialog is the dialog manager
	Dialog dialog.Manager

	readyCh chan struct{}

	// lastEventAt holds the UnixNano timestamp of the most recently received ARI
	// event. It is stamped by runEventHandler and read by runEntityChecker as part
	// of the event-plane liveness watchdog. Accessed atomically.
	lastEventAt atomic.Int64

	// cancel is the context cancel function, by which all subtended subscriptions may be terminated
	cancel context.CancelFunc

	// Log is the log15.Logger for the service.  You may replace or call SetHandler() on this at any time to change the logging of the service.
	Log log15.Logger

	mbus messagebus.Server
}

// New returns a new Server
func New() *Server {
	log := log15.New()
	log.SetHandler(log15.DiscardHandler())

	return &Server{
		MBPrefix: "ari.",
		readyCh:  make(chan struct{}),
		Dialog:   dialog.NewMemManager(),
		Log:      log,
	}
}

// Listen runs the given server, listening to ARI and MessageBus, as specified
func (s *Server) Listen(ctx context.Context, ariOpts *native.Options, messagebusURL string) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	// Connect to ARI
	s.ari, err = native.Connect(ariOpts)
	if err != nil {
		return eris.Wrap(err, "failed to connect to ARI")
	}
	defer s.ari.Close()

	mbtype := messagebus.GetType(messagebusURL)
	switch mbtype {
	case messagebus.TypeRabbitmq:
		s.mbus = &messagebus.RabbitmqBus{
			Config: messagebus.Config{URL: messagebusURL},
			Log:    s.Log,
		}
	case messagebus.TypeNats:
		s.mbus = &messagebus.NatsBus{
			Config: messagebus.Config{URL: messagebusURL},
			Log:    s.Log,
		}
	default:
		return errors.New("Unkwnon url for MessageBus: " + messagebusURL)
	}

	// Connect to MessageBus
	err = s.mbus.Connect()
	if err != nil {
		return eris.Wrap(err, "failed to connect to MessageBus")
	}
	defer s.mbus.Close()

	return s.listen(ctx)
}

// ListenOn runs the given server, listening on the provided ARI and NATS connections
func (s *Server) ListenOn(ctx context.Context, a ari.Client, n *nats.EncodedConn) error {
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	s.ari = a
	s.mbus = messagebus.NewNatsBus(
		messagebus.Config{},
		messagebus.WithNatsConn(n),
	)

	return s.listen(ctx)
}

// Ready returns a channel which is closed when the Server is ready
func (s *Server) Ready() <-chan struct{} {
	if s.readyCh == nil {
		s.readyCh = make(chan struct{})
	}
	return s.readyCh
}

// nolint: gocyclo
func (s *Server) listen(ctx context.Context) error {
	s.Log.Debug("starting listener")

	var wg closeGroup
	defer func() {
		select {
		case <-wg.Done():
		case <-time.After(500 * time.Millisecond):
			panic("timeout waiting for shutdown of sub components")
		}
	}()

	// First, get the Asterisk ID

	ret, err := s.ari.Asterisk().Info(nil)
	if err != nil {
		return eris.Wrap(err, "failed to get Asterisk ID")
	}

	s.AsteriskID = ret.SystemInfo.EntityID
	if s.AsteriskID == "" {
		return eris.New("empty Asterisk ID")
	}

	// Store the ARI application name for top-level access
	s.Application = s.ari.ApplicationName()

	//
	// Listen on the initial MessageBus subjects
	//

	// ping handler
	testPingSub, err := s.mbus.SubscribePing(proxy.PingSubject(s.MBPrefix), s.pingHandler)
	if err != nil {
		return eris.Wrap(err, "failed to subscribe to pings")
	}
	defer wg.Add(testPingSub.Unsubscribe)

	// get a contextualized request handler
	requestHandler := s.newRequestHandler(ctx)

	subjects := []string{
		proxy.Subject(s.MBPrefix, "get", "", ""),
		proxy.Subject(s.MBPrefix, "get", s.Application, ""),
		proxy.Subject(s.MBPrefix, "get", s.Application, s.AsteriskID),
		proxy.Subject(s.MBPrefix, "data", "", ""),
		proxy.Subject(s.MBPrefix, "data", s.Application, ""),
		proxy.Subject(s.MBPrefix, "data", s.Application, s.AsteriskID),
		proxy.Subject(s.MBPrefix, "command", "", ""),
		proxy.Subject(s.MBPrefix, "command", s.Application, ""),
		proxy.Subject(s.MBPrefix, "command", s.Application, s.AsteriskID),
	}
	// get / data / command handlers
	requestsSub, err := s.mbus.SubscribeRequests(subjects, requestHandler)
	if err != nil {
		s.Log.Error("%v", err)
		return eris.Wrap(err, "failed to create requests subscription")
	}
	defer wg.Add(requestsSub.Unsubscribe)()

	// create handlers
	allCreate, err := s.mbus.SubscribeCreateRequest(proxy.Subject(s.MBPrefix, "create", "", ""), "ariproxy", requestHandler)
	if err != nil {
		return eris.Wrap(err, "failed to create create-all subscription")
	}
	defer wg.Add(allCreate.Unsubscribe)()
	appCreate, err := s.mbus.SubscribeCreateRequest(proxy.Subject(s.MBPrefix, "create", s.Application, ""), "ariproxy", requestHandler)
	if err != nil {
		return eris.Wrap(err, "failed to create create-app subscription")
	}
	defer wg.Add(appCreate.Unsubscribe)()
	idCreate, err := s.mbus.SubscribeCreateRequest(proxy.Subject(s.MBPrefix, "create", s.Application, s.AsteriskID), "ariproxy", requestHandler)
	if err != nil {
		return eris.Wrap(err, "failed to create create-id subscription")
	}
	defer wg.Add(idCreate.Unsubscribe)()

	// Run the periodic announcer
	go s.runAnnouncer(ctx)

	// Run the event handler
	go s.runEventHandler(ctx)

	// Run the entity check handler
	go s.runEntityChecker(ctx)

	// Run the dialog cleanup routine: unbind dialogs for entities which have
	// signalled end-of-life (StasisEnd / *Destroyed), so a vanished manager's
	// dialog bindings do not leak forever. Required hygiene before running 2+
	// managers against the same proxy.
	go s.runDialogCleaner(ctx)

	// Run the health endpoint (readiness probe target). It reports 200 only
	// while BOTH the native ARI event plane and the message bus are connected,
	// so a pod whose NATS session has silently died goes NotReady and the
	// kamailio dispatcher / k8s endpoints drop it (fail-closed).
	go s.runHealthServer(ctx)

	// Close the readyChannel to indicate that we are operational
	if s.readyCh != nil {
		close(s.readyCh)
	}

	// Wait for context closure to exit
	<-ctx.Done()
	return ctx.Err()
}

// eventPlaneFailureTolerance is the number of consecutive watchdog ticks the
// native ARI event websocket may report as disconnected before runEntityChecker
// gives up and exits. At proxy.EntityCheckInterval (10s) this is ~30s of
// sustained loss, which lets the native client's own re-dial loop recover from a
// single transient blip without us crash-looping the sidecar.
const eventPlaneFailureTolerance = 3

// runEntityChecker runs the periodic check against the Asterisk entity id and
// acts as the liveness watchdog for the native ARI event plane.
//
// The HTTP control plane (Asterisk().Info) and the event websocket are
// independent: the event socket can silently half-die (no StasisStart events
// reach NATS) while Info() keeps succeeding, with no recovery, until the sidecar
// is restarted. To guard that, in addition to verifying the entity id we treat a
// lost event-websocket connection (native Client.Connected() == false) as a
// failure. We only os.Exit(1) after eventPlaneFailureTolerance consecutive
// failing ticks so a single transient blip — which the native client's own
// re-dial loop will heal — does not crash-loop the process. On exit, Kubernetes
// (or systemd with Restart=on-failure) restarts the sidecar, which re-opens the
// websocket and resumes publishing.
func (s *Server) runEntityChecker(ctx context.Context) {
	ticker := time.NewTicker(proxy.EntityCheckInterval)
	defer ticker.Stop()

	var eventPlaneFailures int

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			info, err := s.ari.Asterisk().Info(nil)
			if err != nil {
				s.Log.Error("failed to get info from Asterisk", "error", err)
				continue
			}
			if s.AsteriskID != info.SystemInfo.EntityID {
				s.Log.Warn("system entitiy id changed", "old", s.AsteriskID, "new", info.SystemInfo.EntityID)
				// We need to exit with non-zero to make sure systemd restarts when service defined with Restart=on-failure
				os.Exit(1)
			}

			// Event-plane liveness check. Connected() reflects the health of the
			// native event websocket; when it goes false the event pipeline is
			// down and no events are reaching NATS.
			if s.ari.Connected() {
				if eventPlaneFailures > 0 {
					s.Log.Info("ARI event websocket recovered", "failures", eventPlaneFailures)
				}
				eventPlaneFailures = 0
				continue
			}

			eventPlaneFailures++
			s.Log.Warn("ARI event websocket reports disconnected",
				"failures", eventPlaneFailures,
				"tolerance", eventPlaneFailureTolerance,
				"lastEventAgo", s.timeSinceLastEvent())
			if eventPlaneFailures >= eventPlaneFailureTolerance {
				s.Log.Error("ARI event plane down for sustained period; exiting so the sidecar is restarted",
					"failures", eventPlaneFailures)
				// Non-zero exit so Kubernetes/systemd restarts the sidecar, which
				// re-opens the event websocket and resumes publishing to NATS.
				os.Exit(1)
			}
		}
	}
}

// timeSinceLastEvent returns how long ago the last ARI event was received, for
// diagnostic logging. Returns a sentinel when no event has been seen yet.
func (s *Server) timeSinceLastEvent() time.Duration {
	last := s.lastEventAt.Load()
	if last == 0 {
		return -1
	}
	return time.Since(time.Unix(0, last))
}

// dialogCleanInterval is how often runDialogCleaner runs its sweep.
var dialogCleanInterval = time.Minute

// runDialogCleaner is the periodic dialog-binding cleanup routine. The bulk of
// cleanup is event-driven (see cleanupDialogsForEvent), which unbinds an entity's
// dialogs the moment that entity is destroyed; that is the conservative,
// never-unbind-a-live-dialog path. This routine is the periodic backstop hook.
//
// The dialog.Manager interface exposes no way to enumerate existing bindings, so
// a time-based GC of "orphaned" bindings cannot be implemented safely without
// extending that interface — and unbinding by guesswork would risk severing a
// live dialog, which is explicitly worse than a leaked binding. We therefore keep
// this routine conservative: it runs on the same lifecycle as the other
// background workers and is the single place to extend should the Manager later
// gain enumeration. It performs no unbinding itself today.
func (s *Server) runDialogCleaner(ctx context.Context) {
	ticker := time.NewTicker(dialogCleanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.Log.Debug("dialog cleaner sweep (event-driven unbind is primary)")
		}
	}
}

// healthAddr is the listen address for the readiness/liveness health endpoint.
const healthAddr = ":8089"

// runHealthServer serves a tiny HTTP health endpoint on healthAddr. GET /healthz
// returns 200 only when BOTH the native ARI event websocket (s.ari.Connected())
// AND the message bus (s.mbus.IsConnected()) are connected; otherwise 503. This
// is the target for a Kubernetes readiness probe so a pod with a half-dead NATS
// session — which the watchdogs would otherwise let linger — is marked NotReady
// and removed from service endpoints (fail-closed).
func (s *Server) runHealthServer(ctx context.Context) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		ariOK := s.ari != nil && s.ari.Connected()
		busOK := s.mbus != nil && s.mbus.IsConnected()
		if ariOK && busOK {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, "unhealthy: ari=%t bus=%t", ariOK, busOK)
	})

	srv := &http.Server{
		Addr:              healthAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Shut the server down when the listener context is cancelled.
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	s.Log.Info("starting health endpoint", "addr", healthAddr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.Log.Error("health endpoint failed", "error", err)
	}
}

// runAnnouncer runs the periodic discovery announcer
func (s *Server) runAnnouncer(ctx context.Context) {
	ticker := time.NewTicker(proxy.AnnouncementInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.announce()
		}
	}
}

// announce publishes the presence of this server to the cluster
func (s *Server) announce() {
	s.publishAnnounce(proxy.AnnouncementSubject(s.MBPrefix), &proxy.Announcement{
		Node:        s.AsteriskID,
		Application: s.Application,
	})
}

// runEventHandler processes events which are received from ARI.
//
// If the underlying subscription channel is ever closed (which can happen if the
// event websocket is torn down), we re-subscribe in place rather than spinning on
// a permanently-closed channel. This is the preferred low-risk self-heal for the
// event plane; the runEntityChecker watchdog is the backstop for a websocket that
// half-dies without ever closing this channel.
func (s *Server) runEventHandler(ctx context.Context) {
	sub := s.ari.Bus().Subscribe(nil, ari.Events.All)
	defer func() { sub.Cancel() }()

	for {
		s.Log.Debug("listening for events", "application", s.Application)
		select {
		case <-ctx.Done():
			return
		case e, ok := <-sub.Events():
			if !ok {
				// The subscription channel was closed; re-subscribe so we keep
				// publishing events instead of busy-looping on a dead channel.
				s.Log.Warn("ARI event subscription channel closed; re-subscribing")
				sub.Cancel()
				sub = s.ari.Bus().Subscribe(nil, ari.Events.All)
				continue
			}
			s.lastEventAt.Store(time.Now().UnixNano())
			s.Log.Debug("event received", "kind", e.GetType())

			// Publish event to canonical destination
			s.publishEvent(fmt.Sprintf("%sevent.%s.%s", s.MBPrefix, s.Application, s.AsteriskID), e)

			// Unbind dialogs once their entity has ended, so bindings do not leak.
			s.cleanupDialogsForEvent(e)

			// Publish event to any associated dialogs
			for _, d := range s.dialogsForEvent(e) {
				de := e
				de.SetDialog(d)
				s.publishEvent(fmt.Sprintf("%sdialogevent.%s", s.MBPrefix, d), de)
			}
		}
	}
}

// pingHandler publishes the server's presence
func (s *Server) pingHandler() {
	if s.ari.Connected() {
		s.announce()
	}
}

// publish sends a message out over MessageBus, logging any error
func (s *Server) publish(subject string, msg *proxy.Response) {
	if err := s.mbus.PublishResponse(subject, msg); err != nil {
		s.Log.Warn("failed to publish MessageBus message", "subject", subject, "data", msg, "error", err)
	}
}

// publishAnnounce sends a message out over MessageBus, logging any error
func (s *Server) publishAnnounce(subject string, msg *proxy.Announcement) {
	if err := s.mbus.PublishAnnounce(subject, msg); err != nil {
		s.Log.Warn("failed to publish MessageBus message", "subject", subject, "data", msg, "error", err)
	}
}

// publishEvent sends a message out over MessageBus, logging any error
func (s *Server) publishEvent(subject string, msg ari.Event) {
	if err := s.mbus.PublishEvent(subject, msg); err != nil {
		s.Log.Warn("failed to publish MessageBus message", "subject", subject, "data", msg, "error", err)
	}
}

// newRequestHandler returns a context-wrapped Handler to handle requests
func (s *Server) newRequestHandler(ctx context.Context) func(subject string, reply string, req *proxy.Request) {
	return func(subject string, reply string, req *proxy.Request) {
		if !s.ari.Connected() {
			s.sendError(reply, eris.New("ARI connection is down"))
			return
		}
		go s.dispatchRequest(ctx, reply, req)
	}
}

// TODO: see if there is a more programmatic approach to this
// nolint: gocyclo
func (s *Server) dispatchRequest(ctx context.Context, reply string, req *proxy.Request) {
	var f func(context.Context, string, *proxy.Request)

	s.Log.Debug("received request", "kind", req.Kind)
	switch req.Kind {
	case "ApplicationData":
		f = s.applicationData
	case "ApplicationGet":
		f = s.applicationGet
	case "ApplicationList":
		f = s.applicationList
	case "ApplicationSubscribe":
		f = s.applicationSubscribe
	case "ApplicationUnsubscribe":
		f = s.applicationUnsubscribe
	case "AsteriskConfigData":
		f = s.asteriskConfigData
	case "AsteriskConfigDelete":
		f = s.asteriskConfigDelete
	case "AsteriskConfigUpdate":
		f = s.asteriskConfigUpdate
	case "AsteriskLoggingCreate":
		f = s.asteriskLoggingCreate
	case "AsteriskLoggingData":
		f = s.asteriskLoggingData
	case "AsteriskLoggingDelete":
		f = s.asteriskLoggingDelete
	case "AsteriskLoggingGet":
		f = s.asteriskLoggingGet
	case "AsteriskLoggingList":
		f = s.asteriskLoggingList
	case "AsteriskLoggingRotate":
		f = s.asteriskLoggingRotate
	case "AsteriskModuleData":
		f = s.asteriskModuleData
	case "AsteriskModuleGet":
		f = s.asteriskModuleGet
	case "AsteriskModuleLoad":
		f = s.asteriskModuleLoad
	case "AsteriskModuleList":
		f = s.asteriskModuleList
	case "AsteriskModuleReload":
		f = s.asteriskModuleReload
	case "AsteriskModuleUnload":
		f = s.asteriskModuleUnload
	case "AsteriskInfo":
		f = s.asteriskInfo
	case "AsteriskVariableGet":
		f = s.asteriskVariableGet
	case "AsteriskVariableSet":
		f = s.asteriskVariableSet
	case "BridgeAddChannel":
		f = s.bridgeAddChannel
	case "BridgeCreate":
		f = s.bridgeCreate
	case "BridgeStageCreate":
		f = s.bridgeStageCreate
	case "BridgeData":
		f = s.bridgeData
	case "BridgeDelete":
		f = s.bridgeDelete
	case "BridgeGet":
		f = s.bridgeGet
	case "BridgeList":
		f = s.bridgeList
	case "BridgeMOH":
		f = s.bridgeMOH
	case "BridgeStopMOH":
		f = s.bridgeStopMOH
	case "BridgePlay":
		f = s.bridgePlay
	case "BridgeStagePlay":
		f = s.bridgeStagePlay
	case "BridgeRecord":
		f = s.bridgeRecord
	case "BridgeStageRecord":
		f = s.bridgeStageRecord
	case "BridgeRemoveChannel":
		f = s.bridgeRemoveChannel
	case "BridgeSubscribe":
		f = s.bridgeSubscribe
	case "BridgeUnsubscribe":
		f = s.bridgeUnsubscribe
	case "BridgeVideoSource":
		f = s.bridgeVideoSource
	case "BridgeVideoSourceDelete":
		f = s.bridgeVideoSourceDelete
	case "ChannelAnswer":
		f = s.channelAnswer
	case "ChannelBusy":
		f = s.channelBusy
	case "ChannelCongestion":
		f = s.channelCongestion
	case "ChannelCreate":
		f = s.channelCreate
	case "ChannelContinue":
		f = s.channelContinue
	case "ChannelData":
		f = s.channelData
	case "ChannelDial":
		f = s.channelDial
	case "ChannelGet":
		f = s.channelGet
	case "ChannelHangup":
		f = s.channelHangup
	case "ChannelHold":
		f = s.channelHold
	case "ChannelList":
		f = s.channelList
	case "ChannelMOH":
		f = s.channelMOH
	case "ChannelMute":
		f = s.channelMute
	case "ChannelOriginate":
		f = s.channelOriginate
	case "ChannelStageOriginate":
		f = s.channelStageOriginate
	case "ChannelPlay":
		f = s.channelPlay
	case "ChannelStagePlay":
		f = s.channelStagePlay
	case "ChannelRecord":
		f = s.channelRecord
	case "ChannelStageRecord":
		f = s.channelStageRecord
	case "ChannelRing":
		f = s.channelRing
	case "ChannelSendDTMF":
		f = s.channelSendDTMF
	case "ChannelSilence":
		f = s.channelSilence
	case "ChannelSnoop":
		f = s.channelSnoop
	case "ChannelStageSnoop":
		f = s.channelStageSnoop
	case "ChannelExternalMedia":
		f = s.channelExternalMedia
	case "ChannelStageExternalMedia":
		f = s.channelStageExternalMedia
	case "ChannelStopHold":
		f = s.channelStopHold
	case "ChannelStopMOH":
		f = s.channelStopMOH
	case "ChannelStopRing":
		f = s.channelStopRing
	case "ChannelStopSilence":
		f = s.channelStopSilence
	case "ChannelSubscribe":
		f = s.channelSubscribe
	case "ChannelUnmute":
		f = s.channelUnmute
	case "ChannelVariableGet":
		f = s.channelVariableGet
	case "ChannelVariableSet":
		f = s.channelVariableSet
	case "DeviceStateData":
		f = s.deviceStateData
	case "DeviceStateDelete":
		f = s.deviceStateDelete
	case "DeviceStateGet":
		f = s.deviceStateGet
	case "DeviceStateList":
		f = s.deviceStateList
	case "DeviceStateUpdate":
		f = s.deviceStateUpdate
	case "EndpointData":
		f = s.endpointData
	case "EndpointGet":
		f = s.endpointGet
	case "EndpointList":
		f = s.endpointList
	case "EndpointListByTech":
		f = s.endpointListByTech
	case "MailboxData":
		f = s.mailboxData
	case "MailboxDelete":
		f = s.mailboxDelete
	case "MailboxGet":
		f = s.mailboxGet
	case "MailboxList":
		f = s.mailboxList
	case "MailboxUpdate":
		f = s.mailboxUpdate
	case "PlaybackControl":
		f = s.playbackControl
	case "PlaybackData":
		f = s.playbackData
	case "PlaybackGet":
		f = s.playbackGet
	case "PlaybackStop":
		f = s.playbackStop
	case "PlaybackSubscribe":
		f = s.playbackSubscribe
	case "RecordingStoredCopy":
		f = s.recordingStoredCopy
	case "RecordingStoredData":
		f = s.recordingStoredData
	case "RecordingStoredDelete":
		f = s.recordingStoredDelete
	case "RecordingStoredGet":
		f = s.recordingStoredGet
	case "RecordingStoredList":
		f = s.recordingStoredList
	case "RecordingLiveData":
		f = s.recordingLiveData
	case "RecordingLiveGet":
		f = s.recordingLiveGet
	case "RecordingLiveMute":
		f = s.recordingLiveMute
	case "RecordingLivePause":
		f = s.recordingLivePause
	case "RecordingLiveResume":
		f = s.recordingLiveResume
	case "RecordingLiveScrap":
		f = s.recordingLiveScrap
	case "RecordingLiveSubscribe":
		f = s.recordingLiveSubscribe
	case "RecordingLiveStop":
		f = s.recordingLiveStop
	case "RecordingLiveUnmute":
		f = s.recordingLiveUnmute
	case "SoundData":
		f = s.soundData
	case "SoundList":
		f = s.soundList
	case "ChannelUserEvent":
		f = s.channelUserEvent
	default:
		f = func(ctx context.Context, reply string, req *proxy.Request) {
			s.sendError(reply, eris.New("Not implemented"))
		}
	}

	f(ctx, reply, req)
}

func (s *Server) sendError(reply string, err error) {
	s.publish(reply, proxy.NewErrorResponse(err))
}

/*
// Start runs the server side instance
func (i *Instance) Start(ctx context.Context) {
	i.ctx, i.cancel = context.WithCancel(ctx)

	i.log.Debug("Starting dialog instance")

	go func() {
		i.application()
		i.asterisk()
		i.modules()
		i.channel()
		i.storedRecording()
		i.liveRecording()
		i.bridge()
		i.device()
		i.playback()
		i.mailbox()
		i.sound()
		i.logging()
		i.config()

		// do commands last, since that is the one that will be dispatching
		i.commands()

		close(i.readyCh)

		<-i.ctx.Done()
	}()

	<-i.readyCh
}

// Stop stops the instance
func (i *Instance) Stop() {
	if i == nil {
		return
	}
	i.cancel()
}

func (i *Instance) String() string {
	return fmt.Sprintf("Instance{%s}", i.Dialog.ID)
}
*/
