package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/CyCoreSystems/ari-proxy/v5/messagebus"
	"github.com/CyCoreSystems/ari-proxy/v5/proxy"
	"github.com/CyCoreSystems/ari-proxy/v5/server/dialog"
	"github.com/CyCoreSystems/ari/v5"
	"github.com/CyCoreSystems/ari/v5/client/native"
	"github.com/CyCoreSystems/ari/v5/rid"
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

	// Authorizer admits or denies every bus request before dispatch. When nil
	// at listen time, the ManagerProfile for this node is installed.
	Authorizer Authorizer

	// Prober sends the event-plane liveness probe. Listen installs an ARI
	// user-event prober when it is nil. For a server started with ListenOn
	// and no Prober, the watchdog falls back to the supplied client's
	// Connected() state.
	Prober   LivenessProber
	liveness livenessTracker

	// exit ends the process; nil means os.Exit.
	exit func(code int)

	// now reads the clock; nil means time.Now.
	now func() time.Time

	// after starts a timer; nil means time.After.
	after func(d time.Duration) <-chan time.Time

	// ManagerMediaHost is the manager membership DNS name used to validate
	// ExternalMedia peers. It defaults to the ARI_PROXY_MANAGER_MEDIA_HOST
	// environment variable.
	ManagerMediaHost string
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

		ManagerMediaHost: os.Getenv(ManagerMediaHostEnv),
	}
}

// Listen runs the given server, listening to ARI and MessageBus, as specified
func (s *Server) Listen(ctx context.Context, ariOpts *native.Options, messagebusURL string) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	// native.Connect resolves defaults and environment settings into the
	// options it is given, so the prober below sees the same settings.
	if ariOpts == nil {
		ariOpts = &native.Options{}
	}

	// Connect to ARI
	s.ari, err = native.Connect(ariOpts)
	if err != nil {
		return eris.Wrap(err, "failed to connect to ARI")
	}
	defer s.ari.Close()

	if s.Prober == nil {
		s.Prober = newUserEventProber(ariOpts, s.ari.ApplicationName())
	}

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

// ListenOn runs the given server, listening on the provided ARI and NATS
// connections. Event-plane liveness always uses a round-trip prober: for a
// native client one is built from the client's own ARI settings, and any
// other client needs a caller-supplied Prober.
func (s *Server) ListenOn(ctx context.Context, a ari.Client, n *nats.EncodedConn) error {
	if s.Prober == nil {
		p, err := proberFor(a)
		if err != nil {
			return err
		}
		s.Prober = p
	}

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

	if s.Authorizer == nil {
		s.Authorizer = &ManagerProfile{
			Application:   s.Application,
			Node:          s.AsteriskID,
			MediaPeerHost: s.ManagerMediaHost,
			Resolver:      net.DefaultResolver,
		}
	}

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

	// Close the readyChannel to indicate that we are operational
	if s.readyCh != nil {
		close(s.readyCh)
	}

	// Wait for context closure to exit
	<-ctx.Done()
	return ctx.Err()
}

// runEntityChecker runs the periodic check against the Asterisk entity id and
// acts as the liveness watchdog for the native ARI event plane.
//
// The HTTP control plane and the event websocket are independent, and the
// native client reports Connected() while a half-open websocket read is
// blocked. So the watchdog proves delivery positively: each tick it sends a
// nonce-tagged user event to this application and expects it back over the
// event websocket by the next tick. A probe that does not come back is a
// confirmed event-plane failure; the process exits non-zero so its supervisor
// restarts it with a fresh websocket.
func (s *Server) runEntityChecker(ctx context.Context) {
	ticker := time.NewTicker(proxy.EntityCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.checkHealth(ctx) {
				s.exitProcess(1)
				return
			}
		}
	}
}

// checkHealth runs one watchdog tick. It returns false on a confirmed failure.
func (s *Server) checkHealth(ctx context.Context) bool {
	if s.Prober == nil {
		s.Log.Error("no ARI liveness prober configured; exiting so the sidecar is restarted")
		return false
	}

	// The outstanding probe is judged first, so nothing below can delay a
	// confirmed event-plane failure.
	state := s.liveness.state(s.clock(), livenessDeliveryWindow)
	if state == probeLost {
		s.Log.Error("ARI event websocket did not deliver the liveness probe; exiting so the sidecar is restarted",
			"lastEventAgo", s.timeSinceLastEvent())
		return false
	}

	info, err := s.infoWithinWindow()
	switch {
	case errors.Is(err, errInfoDeadline):
		s.Log.Error("Asterisk info request did not complete within the liveness window; exiting so the sidecar is restarted")
		return false
	case err != nil:
		s.Log.Error("failed to get info from Asterisk", "error", err)
	case s.AsteriskID != info.SystemInfo.EntityID:
		s.Log.Error("system entity id changed; exiting so the sidecar is restarted", "old", s.AsteriskID, "new", info.SystemInfo.EntityID)
		return false
	}

	if state == probeAwaiting {
		return true
	}

	nonce := rid.New("")
	s.liveness.expect(nonce)
	probeCtx, cancel := context.WithTimeout(ctx, proxy.EntityCheckInterval)
	defer cancel()
	switch err := s.Prober.Probe(probeCtx, nonce); {
	case err == nil:
		s.liveness.accepted(nonce, s.clock())
	case errors.Is(err, ErrApplicationNotRegistered):
		s.liveness.clear()
		s.Log.Error("ARI application is no longer registered; exiting so the sidecar is restarted", "error", err)
		return false
	default:
		// A probe that was never sent says nothing about event delivery.
		s.liveness.clear()
		s.Log.Warn("failed to send ARI liveness probe", "error", err)
	}
	return true
}

// errInfoDeadline reports an Asterisk info request still unanswered when the
// liveness window closed.
var errInfoDeadline = errors.New("asterisk info request exceeded the liveness window")

// infoWithinWindow runs the Asterisk info request with the liveness window as its
// deadline. The native client's request takes no context, so a request that
// hangs is abandoned; the watchdog then ends the process.
func (s *Server) infoWithinWindow() (*ari.AsteriskInfo, error) {
	type result struct {
		info *ari.AsteriskInfo
		err  error
	}
	done := make(chan result, 1)
	go func() {
		info, err := s.ari.Asterisk().Info(nil)
		done <- result{info: info, err: err}
	}()
	select {
	case r := <-done:
		return r.info, r.err
	case <-s.timer(livenessDeliveryWindow):
		return nil, errInfoDeadline
	}
}

func (s *Server) timer(d time.Duration) <-chan time.Time {
	if s.after != nil {
		return s.after(d)
	}
	return time.After(d)
}

func (s *Server) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// exitProcess ends the process so its supervisor restarts the sidecar.
func (s *Server) exitProcess(code int) {
	if s.exit != nil {
		s.exit(code)
		return
	}
	os.Exit(code)
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

// runEventHandler processes events which are received from ARI. A closed
// subscription channel while the server is running is a confirmed event-plane
// failure: the process exits so its supervisor restarts it.
func (s *Server) runEventHandler(ctx context.Context) {
	sub := s.ari.Bus().Subscribe(nil, ari.Events.All)
	defer sub.Cancel()

	for {
		s.Log.Debug("listening for events", "application", s.Application)
		select {
		case <-ctx.Done():
			return
		case e, ok := <-sub.Events():
			if !ok {
				if ctx.Err() != nil {
					return
				}
				s.Log.Error("ARI event subscription closed; exiting so the sidecar is restarted")
				s.exitProcess(1)
				return
			}
			s.handleEvent(e)
		}
	}
}

// handleEvent publishes one ARI event. Dialog subscribers receive it before
// the entity that ended, if any, is unbound, so they see its final event.
func (s *Server) handleEvent(e ari.Event) {
	s.lastEventAt.Store(time.Now().UnixNano())
	if nonce, ok := livenessNonce(e); ok {
		s.liveness.observe(nonce)
		return
	}
	s.Log.Debug("event received", "kind", e.GetType())

	// Publish event to canonical destination
	s.publishEvent(fmt.Sprintf("%sevent.%s.%s", s.MBPrefix, s.Application, s.AsteriskID), e)

	// Publish event to any associated dialogs
	for _, d := range s.dialogsForEvent(e) {
		de := e
		de.SetDialog(d)
		s.publishEvent(fmt.Sprintf("%sdialogevent.%s", s.MBPrefix, d), de)
	}

	// Unbind dialogs once their entity has ended, so bindings do not leak.
	s.cleanupDialogsForEvent(e)
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
		go s.handleRequest(ctx, subject, reply, req)
	}
}

// handleRequest authorizes a bus request against the capability profile and
// dispatches only the request the profile rebuilt. A denied request gets the
// generic ErrRequestDenied reply and never reaches native ARI.
func (s *Server) handleRequest(ctx context.Context, subject string, reply string, req *proxy.Request) {
	accepted, err := s.authorize(ctx, subject, req)
	if err != nil {
		kind := ""
		if req != nil {
			kind = req.Kind
		}
		s.Log.Warn("request denied by capability profile", "subject", subject, "kind", kind, "reason", err)
		s.sendError(reply, ErrRequestDenied)
		return
	}
	s.dispatchRequest(ctx, reply, accepted)
}

func (s *Server) authorize(ctx context.Context, subject string, req *proxy.Request) (*proxy.Request, error) {
	if s.Authorizer == nil {
		return nil, errors.New("no authorizer configured")
	}
	route, ok := s.routeFor(subject)
	if !ok {
		return nil, errors.New("unrecognised subject")
	}
	return s.Authorizer.Authorize(ctx, route, req)
}

// routeFor decodes one of the request subjects this server subscribes to.
func (s *Server) routeFor(subject string) (Route, bool) {
	for _, class := range []string{"get", "data", "command", "create"} {
		for _, r := range []Route{
			{Class: class},
			{Class: class, App: s.Application},
			{Class: class, App: s.Application, Node: s.AsteriskID},
		} {
			if subject == proxy.Subject(s.MBPrefix, r.Class, r.App, r.Node) {
				return r, true
			}
		}
	}
	return Route{}, false
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
