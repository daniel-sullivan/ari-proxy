package server

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"reflect"
	"regexp"
	"strings"

	"github.com/CyCoreSystems/ari-proxy/v5/proxy"
	"github.com/CyCoreSystems/ari/v5"
)

// ErrRequestDenied is the generic reply for any bus request outside the
// capability profile. It deliberately carries no detail about which check
// failed.
var ErrRequestDenied = errors.New("request not permitted")

// ManagerMediaHostEnv names the environment variable holding the DNS name
// whose ready addresses are the only permitted ExternalMedia peers (the
// headless manager membership Service).
const ManagerMediaHostEnv = "ARI_PROXY_MANAGER_MEDIA_HOST"

// Route is the bus subject a request arrived on, decoded into its parts.
// An empty App or Node means the subject was not scoped to it.
type Route struct {
	Class string // get, data, command or create
	App   string
	Node  string
}

// Authorizer decides whether a bus request may reach native ARI. On success
// it returns the request to dispatch, rebuilt from validated fields only.
type Authorizer interface {
	Authorize(ctx context.Context, route Route, req *proxy.Request) (*proxy.Request, error)
}

// MediaPeerResolver resolves the manager membership name. *net.Resolver
// satisfies it.
type MediaPeerResolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// Manager diagnostic and media contract (etla_streaming manager call-router).
const (
	diagnosticMarker          = "etla-outgoing-diagnostic-v1"
	diagnosticOriginalPrefix  = diagnosticMarker + "-original-"
	diagnosticExternalPrefix  = diagnosticMarker + "-external-"
	ordinaryExternalPrefix    = "external-"
	diagnosticContext         = "outbound-pstn"
	diagnosticRingTimeoutSecs = 30
	callTokenVariable         = "PJSIP_HEADER(read,X-Twilio-CallToken)"
)

var (
	entityIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,148}$`)
	e164Pattern     = regexp.MustCompile(`^\+[1-9][0-9]{1,14}$`)
	uuidPattern     = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	uuidV7Pattern   = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

	readableChannelVariables = map[string]bool{
		"UNIQUEID":        true,
		callTokenVariable: true,
	}
)

// ManagerProfile is the capability profile for the etla manager and Admin
// Console. It admits only the discovery, read, guarded diagnostic originate,
// ExternalMedia, bridge and call-control shapes those callers emit; every
// other request is denied before any dialog binding or native ARI call.
type ManagerProfile struct {
	// Application and Node are this proxy's ARI application and Asterisk ID.
	Application string
	Node        string

	// MediaPeerHost is the membership DNS name. ExternalMedia is denied when
	// it is empty, when Resolver is nil, or when the lookup fails.
	MediaPeerHost string
	Resolver      MediaPeerResolver
}

// Authorize implements Authorizer.
func (p *ManagerProfile) Authorize(ctx context.Context, route Route, req *proxy.Request) (*proxy.Request, error) {
	if req == nil {
		return nil, errors.New("empty request")
	}
	check, ok := managerOperations[req.Kind]
	if !ok {
		return nil, fmt.Errorf("kind %q is not in the profile", req.Kind)
	}
	if route.Class != check.class {
		return nil, fmt.Errorf("kind %q not permitted on %q subjects", req.Kind, route.Class)
	}
	if check.payload != "" && reflect.ValueOf(req).Elem().FieldByName(check.payload).IsNil() {
		return nil, fmt.Errorf("kind %q is missing its %s payload", req.Kind, check.payload)
	}
	if extra := otherPayload(req, check.payload); extra != "" {
		return nil, fmt.Errorf("kind %q carries unexpected payload %s", req.Kind, extra)
	}
	return check.build(ctx, p, route, req)
}

type operation struct {
	class   string
	payload string // the one proxy.Request payload field allowed, if any
	build   func(context.Context, *ManagerProfile, Route, *proxy.Request) (*proxy.Request, error)
}

var managerOperations = map[string]operation{
	"EndpointListByTech":   {class: "get", payload: "EndpointListByTech", build: buildEndpointListByTech},
	"ChannelList":          {class: "get", build: buildChannelList},
	"ChannelGet":           {class: "get", build: channelOnly},
	"ChannelData":          {class: "data", build: channelOnly},
	"ChannelVariableGet":   {class: "data", payload: "ChannelVariable", build: buildChannelVariableGet},
	"ChannelOriginate":     {class: "create", payload: "ChannelOriginate", build: buildDiagnosticOriginate},
	"ChannelExternalMedia": {class: "create", payload: "ChannelExternalMedia", build: buildExternalMedia},
	"BridgeCreate":         {class: "create", payload: "BridgeCreate", build: buildBridgeCreate},
	"ChannelAnswer":        {class: "command", build: channelOnly},
	"ChannelHold":          {class: "command", build: channelOnly},
	"ChannelStopHold":      {class: "command", build: channelOnly},
	"ChannelStopMOH":       {class: "command", build: channelOnly},
	"ChannelHangup":        {class: "command", payload: "ChannelHangup", build: buildChannelHangup},
	"ChannelMOH":           {class: "command", payload: "ChannelMOH", build: buildChannelMOH},
	"ChannelContinue":      {class: "command", payload: "ChannelContinue", build: buildChannelContinue},
	"BridgeAddChannel":     {class: "command", payload: "BridgeAddChannel", build: buildBridgeAddChannel},
	"BridgeRemoveChannel":  {class: "command", payload: "BridgeRemoveChannel", build: buildBridgeRemoveChannel},
	"BridgeDelete":         {class: "command", build: buildBridgeDelete},
	"ApplicationSubscribe": {class: "command", payload: "ApplicationSubscribe", build: buildApplicationSubscribe},
}

// otherPayload returns the name of any set payload field other than allowed.
func otherPayload(req *proxy.Request, allowed string) string {
	v := reflect.ValueOf(req).Elem()
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		name := t.Field(i).Name
		if name == "Kind" || name == "Key" || name == allowed {
			continue
		}
		if f := v.Field(i); f.Kind() == reflect.Pointer && !f.IsNil() {
			return name
		}
	}
	return ""
}

// nodeKey validates a request bound to this proxy's app and node and returns
// a clean key of the given kind. kind "" with an empty id is the node-only
// routing reference used by Originate and ExternalMedia.
func (p *ManagerProfile) nodeKey(route Route, key *ari.Key, kind string) (*ari.Key, error) {
	if route.App != p.Application || route.Node != p.Node || p.Node == "" || p.Application == "" {
		return nil, errors.New("request must be addressed to this node")
	}
	if key == nil || key.App != route.App || key.Node != route.Node || key.Dialog != "" || key.Kind != kind {
		return nil, errors.New("key does not match the node route")
	}
	if kind == "" {
		if key.ID != "" {
			return nil, errors.New("routing reference must not name an entity")
		}
	} else if !entityIDPattern.MatchString(key.ID) {
		return nil, errors.New("invalid entity id")
	}
	return ari.NewKey(kind, key.ID, ari.WithApp(route.App), ari.WithNode(route.Node)), nil
}

// appKey validates an application-wide request (no node).
func (p *ManagerProfile) appKey(route Route, key *ari.Key, kinds ...string) (*ari.Key, error) {
	if p.Application == "" || route.App != p.Application || route.Node != "" {
		return nil, errors.New("request must be addressed to this application")
	}
	if key == nil || key.App != route.App || key.Node != "" || key.ID != "" || key.Dialog != "" {
		return nil, errors.New("key does not match the application route")
	}
	for _, k := range kinds {
		if key.Kind == k {
			return ari.NewKey(k, "", ari.WithApp(route.App)), nil
		}
	}
	return nil, errors.New("unexpected key kind")
}

func buildEndpointListByTech(_ context.Context, p *ManagerProfile, route Route, req *proxy.Request) (*proxy.Request, error) {
	key, err := p.appKey(route, req.Key, "")
	if err != nil {
		return nil, err
	}
	if req.EndpointListByTech == nil || req.EndpointListByTech.Tech != "PJSIP" {
		return nil, errors.New("only PJSIP endpoints may be listed")
	}
	return &proxy.Request{Kind: req.Kind, Key: key, EndpointListByTech: &proxy.EndpointListByTech{Tech: "PJSIP"}}, nil
}

func buildChannelList(_ context.Context, p *ManagerProfile, route Route, req *proxy.Request) (*proxy.Request, error) {
	key, err := p.appKey(route, req.Key, "", ari.ChannelKey)
	if err != nil {
		return nil, err
	}
	return &proxy.Request{Kind: req.Kind, Key: key}, nil
}

func channelOnly(_ context.Context, p *ManagerProfile, route Route, req *proxy.Request) (*proxy.Request, error) {
	key, err := p.nodeKey(route, req.Key, ari.ChannelKey)
	if err != nil {
		return nil, err
	}
	return &proxy.Request{Kind: req.Kind, Key: key}, nil
}

func buildChannelVariableGet(_ context.Context, p *ManagerProfile, route Route, req *proxy.Request) (*proxy.Request, error) {
	key, err := p.nodeKey(route, req.Key, ari.ChannelKey)
	if err != nil {
		return nil, err
	}
	v := req.ChannelVariable
	if v == nil || v.Value != "" || !readableChannelVariables[v.Name] {
		return nil, errors.New("channel variable is not readable")
	}
	return &proxy.Request{Kind: req.Kind, Key: key, ChannelVariable: &proxy.ChannelVariable{Name: v.Name}}, nil
}

func buildChannelHangup(_ context.Context, p *ManagerProfile, route Route, req *proxy.Request) (*proxy.Request, error) {
	key, err := p.nodeKey(route, req.Key, ari.ChannelKey)
	if err != nil {
		return nil, err
	}
	h := req.ChannelHangup
	if h == nil || (h.Reason != "" && h.Reason != "normal") {
		return nil, errors.New("hangup reason not permitted")
	}
	return &proxy.Request{Kind: req.Kind, Key: key, ChannelHangup: &proxy.ChannelHangup{Reason: h.Reason}}, nil
}

func buildChannelMOH(_ context.Context, p *ManagerProfile, route Route, req *proxy.Request) (*proxy.Request, error) {
	key, err := p.nodeKey(route, req.Key, ari.ChannelKey)
	if err != nil {
		return nil, err
	}
	if req.ChannelMOH == nil || req.ChannelMOH.Music != "" {
		return nil, errors.New("only the default music class is permitted")
	}
	return &proxy.Request{Kind: req.Kind, Key: key, ChannelMOH: &proxy.ChannelMOH{}}, nil
}

func buildChannelContinue(_ context.Context, p *ManagerProfile, route Route, req *proxy.Request) (*proxy.Request, error) {
	key, err := p.nodeKey(route, req.Key, ari.ChannelKey)
	if err != nil {
		return nil, err
	}
	c := req.ChannelContinue
	if c == nil || c.Context != "" || c.Extension != "" || c.Priority != 0 {
		return nil, errors.New("only resume at the current dialplan position is permitted")
	}
	return &proxy.Request{Kind: req.Kind, Key: key, ChannelContinue: &proxy.ChannelContinue{}}, nil
}

func buildBridgeCreate(_ context.Context, p *ManagerProfile, route Route, req *proxy.Request) (*proxy.Request, error) {
	key, err := p.nodeKey(route, req.Key, ari.BridgeKey)
	if err != nil {
		return nil, err
	}
	b := req.BridgeCreate
	if b == nil || b.Type != "mixing" || b.Name != key.ID {
		return nil, errors.New("only a mixing bridge named by its id is permitted")
	}
	return &proxy.Request{Kind: req.Kind, Key: key, BridgeCreate: &proxy.BridgeCreate{Type: "mixing", Name: key.ID}}, nil
}

func buildBridgeAddChannel(_ context.Context, p *ManagerProfile, route Route, req *proxy.Request) (*proxy.Request, error) {
	key, err := p.nodeKey(route, req.Key, ari.BridgeKey)
	if err != nil {
		return nil, err
	}
	a := req.BridgeAddChannel
	if a == nil || !entityIDPattern.MatchString(a.Channel) || a.AbsorbDTMF || a.Mute || a.Role != "" {
		return nil, errors.New("bridge add options not permitted")
	}
	return &proxy.Request{Kind: req.Kind, Key: key, BridgeAddChannel: &proxy.BridgeAddChannel{Channel: a.Channel}}, nil
}

func buildBridgeRemoveChannel(_ context.Context, p *ManagerProfile, route Route, req *proxy.Request) (*proxy.Request, error) {
	key, err := p.nodeKey(route, req.Key, ari.BridgeKey)
	if err != nil {
		return nil, err
	}
	r := req.BridgeRemoveChannel
	if r == nil || !entityIDPattern.MatchString(r.Channel) {
		return nil, errors.New("invalid channel id")
	}
	return &proxy.Request{Kind: req.Kind, Key: key, BridgeRemoveChannel: &proxy.BridgeRemoveChannel{Channel: r.Channel}}, nil
}

func buildBridgeDelete(_ context.Context, p *ManagerProfile, route Route, req *proxy.Request) (*proxy.Request, error) {
	key, err := p.nodeKey(route, req.Key, ari.BridgeKey)
	if err != nil {
		return nil, err
	}
	return &proxy.Request{Kind: req.Kind, Key: key}, nil
}

func buildApplicationSubscribe(_ context.Context, p *ManagerProfile, route Route, req *proxy.Request) (*proxy.Request, error) {
	key, err := p.nodeKey(route, req.Key, ari.ApplicationKey)
	if err != nil {
		return nil, err
	}
	if key.ID != p.Application {
		return nil, errors.New("only this application may be subscribed")
	}
	s := req.ApplicationSubscribe
	if s == nil {
		return nil, errors.New("missing event source")
	}
	bridgeID, ok := strings.CutPrefix(s.EventSource, "bridge:")
	if !ok || !entityIDPattern.MatchString(bridgeID) {
		return nil, errors.New("only bridge event sources are permitted")
	}
	return &proxy.Request{Kind: req.Kind, Key: key, ApplicationSubscribe: &proxy.ApplicationSubscribe{EventSource: "bridge:" + bridgeID}}, nil
}

// buildDiagnosticOriginate admits only the operator diagnostic: a fixed,
// non-optimizing Local channel into the guarded outbound dialplan context.
// The dialplan stays authoritative for which numbers may be dialled.
func buildDiagnosticOriginate(_ context.Context, p *ManagerProfile, route Route, req *proxy.Request) (*proxy.Request, error) {
	key, err := p.nodeKey(route, req.Key, "")
	if err != nil {
		return nil, err
	}
	o := req.ChannelOriginate.OriginateRequest
	caller, destination, ok := parseDiagnosticEndpoint(o.Endpoint)
	if !ok {
		return nil, errors.New("originate endpoint is not the guarded diagnostic route")
	}
	rawID, ok := strings.CutPrefix(o.ChannelID, diagnosticOriginalPrefix)
	if !ok || !uuidV7Pattern.MatchString(rawID) {
		return nil, errors.New("originate channel id is not a diagnostic id")
	}
	switch {
	case o.CallerID != caller:
		return nil, errors.New("caller id must equal the originating DID")
	case o.App != p.Application:
		return nil, errors.New("originate must enter this application")
	case o.AppArgs != diagnosticMarker+","+o.ChannelID:
		return nil, errors.New("originate app args do not match the diagnostic marker")
	case o.Timeout != diagnosticRingTimeoutSecs:
		return nil, errors.New("originate ring timeout not permitted")
	case o.Context != "" || o.Extension != "" || o.Priority != 0 || o.Label != "" ||
		o.Formats != "" || o.OtherChannelID != "" || o.Originator != "" || len(o.Variables) != 0:
		return nil, errors.New("originate carries options outside the diagnostic shape")
	}
	return &proxy.Request{
		Kind: req.Kind,
		Key:  key,
		ChannelOriginate: &proxy.ChannelOriginate{OriginateRequest: ari.OriginateRequest{
			Endpoint:  diagnosticEndpoint(caller, destination),
			App:       p.Application,
			AppArgs:   diagnosticMarker + "," + o.ChannelID,
			ChannelID: o.ChannelID,
			CallerID:  caller,
			Timeout:   diagnosticRingTimeoutSecs,
		}},
	}, nil
}

func diagnosticEndpoint(caller, destination string) string {
	return fmt.Sprintf("Local/%s*%s@%s/n", caller, destination, diagnosticContext)
}

func parseDiagnosticEndpoint(endpoint string) (caller, destination string, ok bool) {
	rest, ok := strings.CutPrefix(endpoint, "Local/")
	if !ok {
		return "", "", false
	}
	rest, ok = strings.CutSuffix(rest, "@"+diagnosticContext+"/n")
	if !ok {
		return "", "", false
	}
	caller, destination, ok = strings.Cut(rest, "*")
	if !ok || !e164Pattern.MatchString(caller) || !e164Pattern.MatchString(destination) {
		return "", "", false
	}
	return caller, destination, true
}

// buildExternalMedia admits the manager's AudioSocket media leg. The peer IP
// must be a current member of the manager membership name; this proves pod
// membership, not ownership of the submitted port.
func buildExternalMedia(ctx context.Context, p *ManagerProfile, route Route, req *proxy.Request) (*proxy.Request, error) {
	key, err := p.nodeKey(route, req.Key, "")
	if err != nil {
		return nil, err
	}
	o := req.ChannelExternalMedia.Options
	switch {
	case o.App != p.Application:
		return nil, errors.New("external media must enter this application")
	case o.Format != "slin16" || o.Encapsulation != "audiosocket" || o.Transport != "tcp":
		return nil, errors.New("external media dialect not permitted")
	case o.ConnectionType != "" || o.Direction != "" || len(o.Variables) != 0:
		return nil, errors.New("external media carries options outside the manager shape")
	case !uuidPattern.MatchString(o.Data):
		return nil, errors.New("external media data must be a session uuid")
	}
	switch o.ChannelID {
	case ordinaryExternalPrefix + o.Data:
	case diagnosticExternalPrefix + o.Data:
		if !uuidV7Pattern.MatchString(o.Data) {
			return nil, errors.New("diagnostic external media requires a v7 session uuid")
		}
	default:
		return nil, errors.New("external media channel id does not match its session")
	}
	peer, err := netip.ParseAddrPort(o.ExternalHost)
	if err != nil || peer.Port() == 0 {
		return nil, errors.New("external host must be a literal ip:port")
	}
	if err := p.checkMediaPeer(ctx, peer.Addr()); err != nil {
		return nil, err
	}
	return &proxy.Request{
		Kind: req.Kind,
		Key:  key,
		ChannelExternalMedia: &proxy.ChannelExternalMedia{Options: ari.ExternalMediaOptions{
			ChannelID:     o.ChannelID,
			App:           p.Application,
			ExternalHost:  netip.AddrPortFrom(peer.Addr().Unmap(), peer.Port()).String(),
			Format:        "slin16",
			Encapsulation: "audiosocket",
			Transport:     "tcp",
			Data:          o.Data,
		}},
	}, nil
}

func (p *ManagerProfile) checkMediaPeer(ctx context.Context, addr netip.Addr) error {
	if p.MediaPeerHost == "" || p.Resolver == nil {
		return errors.New("no manager membership name configured")
	}
	members, err := p.Resolver.LookupNetIP(ctx, "ip", p.MediaPeerHost)
	if err != nil {
		return fmt.Errorf("manager membership lookup: %w", err)
	}
	addr = addr.Unmap()
	for _, m := range members {
		if m.Unmap() == addr {
			return nil
		}
	}
	return errors.New("external host is not a manager member")
}
