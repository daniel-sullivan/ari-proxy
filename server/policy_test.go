package server

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"testing"

	"github.com/CyCoreSystems/ari-proxy/v5/messagebus"
	"github.com/CyCoreSystems/ari-proxy/v5/proxy"
	"github.com/CyCoreSystems/ari/v5"
	"github.com/CyCoreSystems/ari/v5/client/arimocks"
	"github.com/inconshreveable/log15"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testApp       = "etla"
	testNode      = "node-a"
	testV7        = "019a8f2e-1c3d-7abc-8def-0123456789ab"
	testV4        = "3f2c1d0e-9b8a-4c7d-9e6f-5a4b3c2d1e0f"
	testCaller    = "+81312345678"
	testDest      = "+819012345678"
	testMediaHost = "manager-members.streaming.svc.cluster.local"
	diagChannelID = diagnosticOriginalPrefix + testV7
)

var managerPodIP = netip.MustParseAddr("10.42.1.7")

type fakeResolver struct {
	addrs []netip.Addr
	err   error
}

func (f fakeResolver) LookupNetIP(_ context.Context, network, host string) ([]netip.Addr, error) {
	if network != "ip" || host != testMediaHost {
		return nil, errors.New("unexpected lookup")
	}
	return f.addrs, f.err
}

func testProfile(r MediaPeerResolver) *ManagerProfile {
	return &ManagerProfile{Application: testApp, Node: testNode, MediaPeerHost: testMediaHost, Resolver: r}
}

func nodeRoute(class string) Route { return Route{Class: class, App: testApp, Node: testNode} }
func appRoute(class string) Route  { return Route{Class: class, App: testApp} }

func nodeKey(kind, id string) *ari.Key {
	return ari.NewKey(kind, id, ari.WithApp(testApp), ari.WithNode(testNode))
}

func diagnosticOriginate() ari.OriginateRequest {
	return ari.OriginateRequest{
		Endpoint:  "Local/" + testCaller + "*" + testDest + "@outbound-pstn/n",
		App:       testApp,
		AppArgs:   diagnosticMarker + "," + diagChannelID,
		ChannelID: diagChannelID,
		CallerID:  testCaller,
		Timeout:   30,
	}
}

func externalMedia(channelID, data string) ari.ExternalMediaOptions {
	return ari.ExternalMediaOptions{
		ChannelID:     channelID,
		App:           testApp,
		ExternalHost:  "10.42.1.7:41234",
		Format:        "slin16",
		Encapsulation: "audiosocket",
		Transport:     "tcp",
		Data:          data,
	}
}

func originateReq(mut func(*ari.OriginateRequest)) *proxy.Request {
	o := diagnosticOriginate()
	if mut != nil {
		mut(&o)
	}
	return &proxy.Request{Kind: "ChannelOriginate", Key: nodeKey("", ""), ChannelOriginate: &proxy.ChannelOriginate{OriginateRequest: o}}
}

func mediaReq(mut func(*ari.ExternalMediaOptions)) *proxy.Request {
	o := externalMedia(ordinaryExternalPrefix+testV4, testV4)
	if mut != nil {
		mut(&o)
	}
	return &proxy.Request{Kind: "ChannelExternalMedia", Key: nodeKey("", ""), ChannelExternalMedia: &proxy.ChannelExternalMedia{Options: o}}
}

func TestManagerProfileAllows(t *testing.T) {
	t.Parallel()
	members := fakeResolver{addrs: []netip.Addr{netip.MustParseAddr("10.42.0.3"), managerPodIP}}

	tests := []struct {
		name  string
		route Route
		req   *proxy.Request
		want  *proxy.Request
	}{
		{
			name:  "endpoint discovery",
			route: appRoute("get"),
			req:   &proxy.Request{Kind: "EndpointListByTech", Key: ari.NodeKey(testApp, ""), EndpointListByTech: &proxy.EndpointListByTech{Tech: "PJSIP"}},
			want:  &proxy.Request{Kind: "EndpointListByTech", Key: ari.NewKey("", "", ari.WithApp(testApp)), EndpointListByTech: &proxy.EndpointListByTech{Tech: "PJSIP"}},
		},
		{
			name:  "admin console channel list",
			route: appRoute("get"),
			req:   &proxy.Request{Kind: "ChannelList", Key: &ari.Key{App: testApp}},
			want:  &proxy.Request{Kind: "ChannelList", Key: ari.NewKey("", "", ari.WithApp(testApp))},
		},
		{
			name:  "admin console channel data",
			route: nodeRoute("data"),
			req:   &proxy.Request{Kind: "ChannelData", Key: nodeKey(ari.ChannelKey, "1732345.12")},
			want:  &proxy.Request{Kind: "ChannelData", Key: nodeKey(ari.ChannelKey, "1732345.12")},
		},
		{
			name:  "channel get",
			route: nodeRoute("get"),
			req:   &proxy.Request{Kind: "ChannelGet", Key: nodeKey(ari.ChannelKey, "1732345.12")},
			want:  &proxy.Request{Kind: "ChannelGet", Key: nodeKey(ari.ChannelKey, "1732345.12")},
		},
		{
			name:  "uniqueid read",
			route: nodeRoute("data"),
			req:   &proxy.Request{Kind: "ChannelVariableGet", Key: nodeKey(ari.ChannelKey, "c1"), ChannelVariable: &proxy.ChannelVariable{Name: "UNIQUEID"}},
			want:  &proxy.Request{Kind: "ChannelVariableGet", Key: nodeKey(ari.ChannelKey, "c1"), ChannelVariable: &proxy.ChannelVariable{Name: "UNIQUEID"}},
		},
		{
			name:  "call token read",
			route: nodeRoute("data"),
			req:   &proxy.Request{Kind: "ChannelVariableGet", Key: nodeKey(ari.ChannelKey, "c1"), ChannelVariable: &proxy.ChannelVariable{Name: callTokenVariable}},
			want:  &proxy.Request{Kind: "ChannelVariableGet", Key: nodeKey(ari.ChannelKey, "c1"), ChannelVariable: &proxy.ChannelVariable{Name: callTokenVariable}},
		},
		{
			name:  "diagnostic originate",
			route: nodeRoute("create"),
			req:   originateReq(nil),
			want:  &proxy.Request{Kind: "ChannelOriginate", Key: nodeKey("", ""), ChannelOriginate: &proxy.ChannelOriginate{OriginateRequest: diagnosticOriginate()}},
		},
		{
			name:  "ordinary external media",
			route: nodeRoute("create"),
			req:   mediaReq(nil),
			want:  &proxy.Request{Kind: "ChannelExternalMedia", Key: nodeKey("", ""), ChannelExternalMedia: &proxy.ChannelExternalMedia{Options: externalMedia(ordinaryExternalPrefix+testV4, testV4)}},
		},
		{
			name:  "diagnostic external media",
			route: nodeRoute("create"),
			req: mediaReq(func(o *ari.ExternalMediaOptions) {
				*o = externalMedia(diagnosticExternalPrefix+testV7, testV7)
			}),
			want: &proxy.Request{Kind: "ChannelExternalMedia", Key: nodeKey("", ""), ChannelExternalMedia: &proxy.ChannelExternalMedia{Options: externalMedia(diagnosticExternalPrefix+testV7, testV7)}},
		},
		{
			name:  "answer",
			route: nodeRoute("command"),
			req:   &proxy.Request{Kind: "ChannelAnswer", Key: nodeKey(ari.ChannelKey, "c1")},
			want:  &proxy.Request{Kind: "ChannelAnswer", Key: nodeKey(ari.ChannelKey, "c1")},
		},
		{
			name:  "handle hangup",
			route: nodeRoute("command"),
			req:   &proxy.Request{Kind: "ChannelHangup", Key: nodeKey(ari.ChannelKey, "c1"), ChannelHangup: &proxy.ChannelHangup{Reason: "normal"}},
			want:  &proxy.Request{Kind: "ChannelHangup", Key: nodeKey(ari.ChannelKey, "c1"), ChannelHangup: &proxy.ChannelHangup{Reason: "normal"}},
		},
		{
			name:  "keyed hangup",
			route: nodeRoute("command"),
			req:   &proxy.Request{Kind: "ChannelHangup", Key: nodeKey(ari.ChannelKey, "external-"+testV4), ChannelHangup: &proxy.ChannelHangup{}},
			want:  &proxy.Request{Kind: "ChannelHangup", Key: nodeKey(ari.ChannelKey, "external-"+testV4), ChannelHangup: &proxy.ChannelHangup{}},
		},
		{
			name:  "hold",
			route: nodeRoute("command"),
			req:   &proxy.Request{Kind: "ChannelHold", Key: nodeKey(ari.ChannelKey, "c1")},
			want:  &proxy.Request{Kind: "ChannelHold", Key: nodeKey(ari.ChannelKey, "c1")},
		},
		{
			name:  "stop hold",
			route: nodeRoute("command"),
			req:   &proxy.Request{Kind: "ChannelStopHold", Key: nodeKey(ari.ChannelKey, "c1")},
			want:  &proxy.Request{Kind: "ChannelStopHold", Key: nodeKey(ari.ChannelKey, "c1")},
		},
		{
			name:  "default music on hold",
			route: nodeRoute("command"),
			req:   &proxy.Request{Kind: "ChannelMOH", Key: nodeKey(ari.ChannelKey, "c1"), ChannelMOH: &proxy.ChannelMOH{}},
			want:  &proxy.Request{Kind: "ChannelMOH", Key: nodeKey(ari.ChannelKey, "c1"), ChannelMOH: &proxy.ChannelMOH{}},
		},
		{
			name:  "stop music on hold",
			route: nodeRoute("command"),
			req:   &proxy.Request{Kind: "ChannelStopMOH", Key: nodeKey(ari.ChannelKey, "c1")},
			want:  &proxy.Request{Kind: "ChannelStopMOH", Key: nodeKey(ari.ChannelKey, "c1")},
		},
		{
			name:  "resume at current position",
			route: nodeRoute("command"),
			req:   &proxy.Request{Kind: "ChannelContinue", Key: nodeKey(ari.ChannelKey, "c1"), ChannelContinue: &proxy.ChannelContinue{}},
			want:  &proxy.Request{Kind: "ChannelContinue", Key: nodeKey(ari.ChannelKey, "c1"), ChannelContinue: &proxy.ChannelContinue{}},
		},
		{
			name:  "bridge create",
			route: nodeRoute("create"),
			req:   &proxy.Request{Kind: "BridgeCreate", Key: nodeKey(ari.BridgeKey, "call-"+testV4), BridgeCreate: &proxy.BridgeCreate{Type: "mixing", Name: "call-" + testV4}},
			want:  &proxy.Request{Kind: "BridgeCreate", Key: nodeKey(ari.BridgeKey, "call-"+testV4), BridgeCreate: &proxy.BridgeCreate{Type: "mixing", Name: "call-" + testV4}},
		},
		{
			name:  "bridge add",
			route: nodeRoute("command"),
			req:   &proxy.Request{Kind: "BridgeAddChannel", Key: nodeKey(ari.BridgeKey, "b1"), BridgeAddChannel: &proxy.BridgeAddChannel{Channel: "c1"}},
			want:  &proxy.Request{Kind: "BridgeAddChannel", Key: nodeKey(ari.BridgeKey, "b1"), BridgeAddChannel: &proxy.BridgeAddChannel{Channel: "c1"}},
		},
		{
			name:  "bridge remove",
			route: nodeRoute("command"),
			req:   &proxy.Request{Kind: "BridgeRemoveChannel", Key: nodeKey(ari.BridgeKey, "b1"), BridgeRemoveChannel: &proxy.BridgeRemoveChannel{Channel: "c1"}},
			want:  &proxy.Request{Kind: "BridgeRemoveChannel", Key: nodeKey(ari.BridgeKey, "b1"), BridgeRemoveChannel: &proxy.BridgeRemoveChannel{Channel: "c1"}},
		},
		{
			name:  "bridge delete",
			route: nodeRoute("command"),
			req:   &proxy.Request{Kind: "BridgeDelete", Key: nodeKey(ari.BridgeKey, "b1")},
			want:  &proxy.Request{Kind: "BridgeDelete", Key: nodeKey(ari.BridgeKey, "b1")},
		},
		{
			name:  "app subscribes to bridge",
			route: nodeRoute("command"),
			req:   &proxy.Request{Kind: "ApplicationSubscribe", Key: nodeKey(ari.ApplicationKey, testApp), ApplicationSubscribe: &proxy.ApplicationSubscribe{EventSource: "bridge:b1"}},
			want:  &proxy.Request{Kind: "ApplicationSubscribe", Key: nodeKey(ari.ApplicationKey, testApp), ApplicationSubscribe: &proxy.ApplicationSubscribe{EventSource: "bridge:b1"}},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := testProfile(members).Authorize(context.Background(), tt.route, tt.req)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestManagerProfileDenies(t *testing.T) {
	t.Parallel()
	members := fakeResolver{addrs: []netip.Addr{managerPodIP}}

	tests := []struct {
		name     string
		route    Route
		req      *proxy.Request
		resolver MediaPeerResolver
	}{
		{name: "nil request", route: nodeRoute("command")},
		{name: "unsupported kind", route: nodeRoute("command"), req: &proxy.Request{Kind: "AsteriskConfigUpdate", Key: nodeKey("", "")}},
		{name: "channel dial", route: nodeRoute("command"), req: &proxy.Request{Kind: "ChannelDial", Key: nodeKey(ari.ChannelKey, "c1"), ChannelDial: &proxy.ChannelDial{}}},
		{name: "channel create", route: nodeRoute("create"), req: &proxy.Request{Kind: "ChannelCreate", Key: nodeKey("", ""), ChannelCreate: &proxy.ChannelCreate{}}},
		{name: "variable write", route: nodeRoute("command"), req: &proxy.Request{Kind: "ChannelVariableSet", Key: nodeKey(ari.ChannelKey, "c1"), ChannelVariable: &proxy.ChannelVariable{Name: "X", Value: "1"}}},
		{name: "generic playback", route: nodeRoute("create"), req: &proxy.Request{Kind: "ChannelPlay", Key: nodeKey(ari.ChannelKey, "c1"), ChannelPlay: &proxy.ChannelPlay{MediaURI: "sound:x"}}},
		{name: "recording", route: nodeRoute("create"), req: &proxy.Request{Kind: "ChannelRecord", Key: nodeKey(ari.ChannelKey, "c1"), ChannelRecord: &proxy.ChannelRecord{Name: "r"}}},
		{name: "wrong subject class", route: nodeRoute("command"), req: &proxy.Request{Kind: "ChannelGet", Key: nodeKey(ari.ChannelKey, "c1")}},
		{name: "broadcast subject", route: Route{Class: "command"}, req: &proxy.Request{Kind: "ChannelAnswer", Key: &ari.Key{Kind: ari.ChannelKey, ID: "c1"}}},
		{name: "app-wide control", route: appRoute("command"), req: &proxy.Request{Kind: "ChannelAnswer", Key: ari.NewKey(ari.ChannelKey, "c1", ari.WithApp(testApp))}},
		{name: "other app", route: Route{Class: "command", App: "other", Node: testNode}, req: &proxy.Request{Kind: "ChannelAnswer", Key: ari.NewKey(ari.ChannelKey, "c1", ari.WithApp("other"), ari.WithNode(testNode))}},
		{name: "other node", route: Route{Class: "command", App: testApp, Node: "node-b"}, req: &proxy.Request{Kind: "ChannelAnswer", Key: ari.NewKey(ari.ChannelKey, "c1", ari.WithApp(testApp), ari.WithNode("node-b"))}},
		{name: "key node differs from subject", route: nodeRoute("command"), req: &proxy.Request{Kind: "ChannelAnswer", Key: ari.NewKey(ari.ChannelKey, "c1", ari.WithApp(testApp), ari.WithNode("node-b"))}},
		{name: "dialog binding", route: nodeRoute("command"), req: &proxy.Request{Kind: "ChannelAnswer", Key: ari.NewKey(ari.ChannelKey, "c1", ari.WithApp(testApp), ari.WithNode(testNode), ari.WithDialog("d1"))}},
		{name: "wrong key kind", route: nodeRoute("command"), req: &proxy.Request{Kind: "ChannelAnswer", Key: nodeKey(ari.BridgeKey, "c1")}},
		{name: "bad entity id", route: nodeRoute("command"), req: &proxy.Request{Kind: "ChannelAnswer", Key: nodeKey(ari.ChannelKey, "c1/../x")}},
		{name: "extra payload", route: nodeRoute("command"), req: &proxy.Request{Kind: "ChannelAnswer", Key: nodeKey(ari.ChannelKey, "c1"), ChannelDial: &proxy.ChannelDial{}}},
		{name: "non-pjsip endpoints", route: appRoute("get"), req: &proxy.Request{Kind: "EndpointListByTech", Key: ari.NodeKey(testApp, ""), EndpointListByTech: &proxy.EndpointListByTech{Tech: "IAX2"}}},
		{name: "node-scoped channel list", route: nodeRoute("get"), req: &proxy.Request{Kind: "ChannelList", Key: ari.NodeKey(testApp, testNode)}},
		{name: "arbitrary variable read", route: nodeRoute("data"), req: &proxy.Request{Kind: "ChannelVariableGet", Key: nodeKey(ari.ChannelKey, "c1"), ChannelVariable: &proxy.ChannelVariable{Name: "SHELL(id)"}}},
		{name: "hangup reason", route: nodeRoute("command"), req: &proxy.Request{Kind: "ChannelHangup", Key: nodeKey(ari.ChannelKey, "c1"), ChannelHangup: &proxy.ChannelHangup{Reason: "busy"}}},
		{name: "music class", route: nodeRoute("command"), req: &proxy.Request{Kind: "ChannelMOH", Key: nodeKey(ari.ChannelKey, "c1"), ChannelMOH: &proxy.ChannelMOH{Music: "x"}}},
		{name: "redirected continue", route: nodeRoute("command"), req: &proxy.Request{Kind: "ChannelContinue", Key: nodeKey(ari.ChannelKey, "c1"), ChannelContinue: &proxy.ChannelContinue{Context: "outbound-pstn", Extension: "s", Priority: 1}}},
		{name: "continue missing payload", route: nodeRoute("command"), req: &proxy.Request{Kind: "ChannelContinue", Key: nodeKey(ari.ChannelKey, "c1")}},
		{name: "holding bridge", route: nodeRoute("create"), req: &proxy.Request{Kind: "BridgeCreate", Key: nodeKey(ari.BridgeKey, "b1"), BridgeCreate: &proxy.BridgeCreate{Type: "holding", Name: "b1"}}},
		{name: "bridge name mismatch", route: nodeRoute("create"), req: &proxy.Request{Kind: "BridgeCreate", Key: nodeKey(ari.BridgeKey, "b1"), BridgeCreate: &proxy.BridgeCreate{Type: "mixing", Name: "b2"}}},
		{name: "bridge add with role", route: nodeRoute("command"), req: &proxy.Request{Kind: "BridgeAddChannel", Key: nodeKey(ari.BridgeKey, "b1"), BridgeAddChannel: &proxy.BridgeAddChannel{Channel: "c1", Role: "announcer"}}},
		{name: "subscribe other app", route: nodeRoute("command"), req: &proxy.Request{Kind: "ApplicationSubscribe", Key: nodeKey(ari.ApplicationKey, "other"), ApplicationSubscribe: &proxy.ApplicationSubscribe{EventSource: "bridge:b1"}}},
		{name: "subscribe to endpoint", route: nodeRoute("command"), req: &proxy.Request{Kind: "ApplicationSubscribe", Key: nodeKey(ari.ApplicationKey, testApp), ApplicationSubscribe: &proxy.ApplicationSubscribe{EventSource: "endpoint:PJSIP/pstn-out"}}},

		{name: "originate direct trunk", route: nodeRoute("create"), req: originateReq(func(o *ari.OriginateRequest) { o.Endpoint = "PJSIP/" + testDest + "@pstn-out" })},
		{name: "originate optimizing local", route: nodeRoute("create"), req: originateReq(func(o *ari.OriginateRequest) { o.Endpoint = "Local/" + testCaller + "*" + testDest + "@outbound-pstn" })},
		{name: "originate other context", route: nodeRoute("create"), req: originateReq(func(o *ari.OriginateRequest) { o.Endpoint = "Local/" + testCaller + "*" + testDest + "@default/n" })},
		{name: "originate non-e164", route: nodeRoute("create"), req: originateReq(func(o *ari.OriginateRequest) { o.Endpoint = "Local/" + testCaller + "*0901234@outbound-pstn/n" })},
		{name: "originate extra star", route: nodeRoute("create"), req: originateReq(func(o *ari.OriginateRequest) {
			o.Endpoint = "Local/" + testCaller + "*" + testDest + "*1@outbound-pstn/n"
		})},
		{name: "originate spoofed caller id", route: nodeRoute("create"), req: originateReq(func(o *ari.OriginateRequest) { o.CallerID = "+15550100" })},
		{name: "originate other app", route: nodeRoute("create"), req: originateReq(func(o *ari.OriginateRequest) { o.App = "other" })},
		{name: "originate non-diagnostic id", route: nodeRoute("create"), req: originateReq(func(o *ari.OriginateRequest) {
			o.ChannelID = testV7
			o.AppArgs = diagnosticMarker + "," + testV7
		})},
		{name: "originate v4 id", route: nodeRoute("create"), req: originateReq(func(o *ari.OriginateRequest) {
			o.ChannelID = diagnosticOriginalPrefix + testV4
			o.AppArgs = diagnosticMarker + "," + o.ChannelID
		})},
		{name: "originate app args mismatch", route: nodeRoute("create"), req: originateReq(func(o *ari.OriginateRequest) { o.AppArgs = testV7 })},
		{name: "originate long ring", route: nodeRoute("create"), req: originateReq(func(o *ari.OriginateRequest) { o.Timeout = 300 })},
		{name: "originate dialplan target", route: nodeRoute("create"), req: originateReq(func(o *ari.OriginateRequest) { o.Context = "outbound-pstn" })},
		{name: "originate variables", route: nodeRoute("create"), req: originateReq(func(o *ari.OriginateRequest) { o.Variables = map[string]string{"X": "1"} })},
		{name: "originate originator", route: nodeRoute("create"), req: originateReq(func(o *ari.OriginateRequest) { o.Originator = "c1" })},
		{name: "originate formats", route: nodeRoute("create"), req: originateReq(func(o *ari.OriginateRequest) { o.Formats = "ulaw" })},
		{name: "originate from channel key", route: nodeRoute("create"), req: func() *proxy.Request {
			r := originateReq(nil)
			r.Key = nodeKey(ari.ChannelKey, "c1")
			return r
		}()},
		{name: "originate app-wide", route: appRoute("create"), req: func() *proxy.Request {
			r := originateReq(nil)
			r.Key = ari.NewKey("", "", ari.WithApp(testApp))
			return r
		}()},

		{name: "media nonmember ip", route: nodeRoute("create"), req: mediaReq(func(o *ari.ExternalMediaOptions) { o.ExternalHost = "10.42.9.9:41234" })},
		{name: "media hostname", route: nodeRoute("create"), req: mediaReq(func(o *ari.ExternalMediaOptions) { o.ExternalHost = testMediaHost + ":41234" })},
		{name: "media zero port", route: nodeRoute("create"), req: mediaReq(func(o *ari.ExternalMediaOptions) { o.ExternalHost = "10.42.1.7:0" })},
		{name: "media lookup failure", route: nodeRoute("create"), req: mediaReq(nil), resolver: fakeResolver{err: errors.New("no such host")}},
		{name: "media no members", route: nodeRoute("create"), req: mediaReq(nil), resolver: fakeResolver{}},
		{name: "media rtp dialect", route: nodeRoute("create"), req: mediaReq(func(o *ari.ExternalMediaOptions) { o.Encapsulation = "rtp"; o.Transport = "udp" })},
		{name: "media other format", route: nodeRoute("create"), req: mediaReq(func(o *ari.ExternalMediaOptions) { o.Format = "ulaw" })},
		{name: "media direction", route: nodeRoute("create"), req: mediaReq(func(o *ari.ExternalMediaOptions) { o.Direction = "both" })},
		{name: "media variables", route: nodeRoute("create"), req: mediaReq(func(o *ari.ExternalMediaOptions) { o.Variables = map[string]string{"X": "1"} })},
		{name: "media other app", route: nodeRoute("create"), req: mediaReq(func(o *ari.ExternalMediaOptions) { o.App = "other" })},
		{name: "media id mismatch", route: nodeRoute("create"), req: mediaReq(func(o *ari.ExternalMediaOptions) { o.ChannelID = ordinaryExternalPrefix + testV7 })},
		{name: "media non-uuid data", route: nodeRoute("create"), req: mediaReq(func(o *ari.ExternalMediaOptions) { o.Data = "x"; o.ChannelID = "external-x" })},
		{name: "media diagnostic v4", route: nodeRoute("create"), req: mediaReq(func(o *ari.ExternalMediaOptions) { o.ChannelID = diagnosticExternalPrefix + testV4 })},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := tt.resolver
			if r == nil {
				r = members
			}
			got, err := testProfile(r).Authorize(context.Background(), tt.route, tt.req)
			assert.Error(t, err)
			assert.Nil(t, got)
		})
	}
}

func TestManagerProfileWithoutMembershipNameDeniesMedia(t *testing.T) {
	t.Parallel()
	for _, p := range []*ManagerProfile{
		{Application: testApp, Node: testNode, Resolver: fakeResolver{addrs: []netip.Addr{managerPodIP}}},
		{Application: testApp, Node: testNode, MediaPeerHost: testMediaHost},
	} {
		_, err := p.Authorize(context.Background(), nodeRoute("create"), mediaReq(nil))
		assert.Error(t, err)
	}
}

// recordingBus records replies and fails on anything else the handler might
// do with the bus.
type recordingBus struct {
	messagebus.Server
	mu      sync.Mutex
	replies []*proxy.Response
	events  []string
}

func (b *recordingBus) PublishResponse(_ string, msg *proxy.Response) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.replies = append(b.replies, msg)
	return nil
}

func testServer(a ari.Client, bus messagebus.Server) *Server {
	log := log15.New()
	log.SetHandler(log15.DiscardHandler())
	return &Server{
		Application: testApp,
		AsteriskID:  testNode,
		MBPrefix:    "ari.",
		ari:         a,
		mbus:        bus,
		Log:         log,
		Authorizer:  testProfile(fakeResolver{addrs: []netip.Addr{managerPodIP}}),
	}
}

func TestHandleRequestDenialMakesNoDispatch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		subject string
		req     *proxy.Request
	}{
		{name: "unknown subject", subject: "ari.command.etla.node-b", req: &proxy.Request{Kind: "ChannelAnswer", Key: nodeKey(ari.ChannelKey, "c1")}},
		{name: "other prefix", subject: "x.command.etla.node-a", req: &proxy.Request{Kind: "ChannelAnswer", Key: nodeKey(ari.ChannelKey, "c1")}},
		{name: "config write", subject: "ari.command.etla.node-a", req: &proxy.Request{Kind: "AsteriskConfigUpdate", Key: nodeKey("", "")}},
		{name: "direct trunk originate", subject: "ari.create.etla.node-a", req: originateReq(func(o *ari.OriginateRequest) { o.Endpoint = "PJSIP/" + testDest + "@pstn-out" })},
		{name: "originate with dialog", subject: "ari.create.etla.node-a", req: func() *proxy.Request {
			r := originateReq(nil)
			r.Key.Dialog = "d1"
			return r
		}()},
		{name: "nonmember media", subject: "ari.create.etla.node-a", req: mediaReq(func(o *ari.ExternalMediaOptions) { o.ExternalHost = "10.0.0.1:4000" })},
		{name: "originate without payload", subject: "ari.create.etla.node-a", req: &proxy.Request{Kind: "ChannelOriginate", Key: nodeKey("", "")}},
		{name: "external media without payload", subject: "ari.create.etla.node-a", req: &proxy.Request{Kind: "ChannelExternalMedia", Key: nodeKey("", "")}},
		{name: "variable write", subject: "ari.command.etla.node-a", req: &proxy.Request{Kind: "ChannelVariableSet", Key: nodeKey(ari.ChannelKey, "c1"), ChannelVariable: &proxy.ChannelVariable{Name: "X", Value: "1"}}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			native := &arimocks.Client{}
			bus := &recordingBus{}
			s := testServer(native, bus)
			s.Dialog = nil // any dialog binding would panic

			s.handleRequest(context.Background(), tt.subject, "reply", tt.req)

			assert.Empty(t, native.Calls, "denied request reached native ARI")
			require.Len(t, bus.replies, 1)
			assert.Equal(t, ErrRequestDenied.Error(), bus.replies[0].Error)
		})
	}
}

func TestHandleRequestDispatchesRebuiltRequest(t *testing.T) {
	t.Parallel()
	ch := &arimocks.Channel{}
	ch.On("Hangup", nodeKey(ari.ChannelKey, "c1"), "normal").Return(nil).Once()
	native := &arimocks.Client{}
	native.On("Channel").Return(ch)
	bus := &recordingBus{}
	s := testServer(native, bus)

	s.handleRequest(context.Background(), "ari.command.etla.node-a", "reply", &proxy.Request{
		Kind:          "ChannelHangup",
		Key:           nodeKey(ari.ChannelKey, "c1"),
		ChannelHangup: &proxy.ChannelHangup{Reason: "normal"},
	})

	ch.AssertExpectations(t)
	require.Len(t, bus.replies, 1)
	assert.Empty(t, bus.replies[0].Error)
}

func TestRouteFor(t *testing.T) {
	t.Parallel()
	s := testServer(nil, nil)
	tests := []struct {
		subject string
		want    Route
		ok      bool
	}{
		{subject: "ari.get", want: Route{Class: "get"}, ok: true},
		{subject: "ari.get.etla", want: Route{Class: "get", App: testApp}, ok: true},
		{subject: "ari.create.etla.node-a", want: Route{Class: "create", App: testApp, Node: testNode}, ok: true},
		{subject: "ari.event.etla.node-a"},
		{subject: "ari.command.other.node-a"},
		{subject: "ari.command.etla.node-a.extra"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.subject, func(t *testing.T) {
			t.Parallel()
			got, ok := s.routeFor(tt.subject)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}
