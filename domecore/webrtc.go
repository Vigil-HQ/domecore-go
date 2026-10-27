// Package domecore provides WebRTC primitives for establishing peer-to-peer connections and
// multi-party rooms from backend services. It wraps the Pion WebRTC library behind small,
// signaling-agnostic interfaces so that any transport (WebSocket, NATS, Connect RPC, HTTP)
// can be used to exchange SDP offers/answers and ICE candidates.
//
// Two session models are supported:
//   - Standalone peers (WebRTC.NewPeer) for one-to-one sessions between a client and the service,
//     using either trickle ICE over a Signaler or a single request/response exchange.
//   - Rooms (WebRTC.CreateRoom) that act as a Selective Forwarding Unit (SFU), forwarding every
//     published audio/video track to all other participants and relaying data channel messages.
//
// Example usage:
//
//	rtc, err := NewWebRTC(WebRTCConfig{
//	    ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
//	}, logger)
//	room, err := rtc.GetOrCreateRoom("standup", RoomOptions{AutoClose: true})
//	peer, err := room.Join(userID, PeerOptions{Signaler: wsSignaler})
package domecore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/beego/beego/v2/core/logs"
	"github.com/google/uuid"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

const (
	// RoomDataChannelLabel is the label of the data channel the server opens on every room peer.
	// Clients receive it through the RTCPeerConnection "datachannel" event.
	RoomDataChannelLabel = "domecore-room"
	// DefaultSTUNServer is used when no ICE servers are configured
	DefaultSTUNServer = "stun:stun.l.google.com:19302"
	// iceGatheringTimeout bounds how long non-trickle negotiation waits for ICE gathering to complete
	iceGatheringTimeout = 10 * time.Second
)

//Exceptions

var ErrRoomExists = errors.New("webrtc: room already exists")
var ErrRoomNotFound = errors.New("webrtc: room not found")
var ErrRoomClosed = errors.New("webrtc: room is closed")
var ErrRoomFull = errors.New("webrtc: room has reached its maximum number of peers")
var ErrPeerExists = errors.New("webrtc: peer already exists")
var ErrPeerNotFound = errors.New("webrtc: peer not found")
var ErrPeerClosed = errors.New("webrtc: peer is closed")
var ErrNoSignaler = errors.New("webrtc: a signaler is required for this operation")
var ErrNegotiationInProgress = errors.New("webrtc: a negotiation is already in progress")
var ErrNegotiationCollision = errors.New("webrtc: offer collided with a pending local offer; the remote peer must roll back")
var ErrInvalidSDPType = errors.New("webrtc: unexpected session description type")
var ErrDataChannelNotFound = errors.New("webrtc: data channel not found")
var ErrDataChannelNotOpen = errors.New("webrtc: data channel is not open")

//Configuration

// WebRTCConfig holds the settings shared by every peer connection created by a WebRTC instance.
type WebRTCConfig struct {
	// ICEServers lists the STUN/TURN servers used for ICE. Defaults to DefaultSTUNServer when empty.
	ICEServers []webrtc.ICEServer
	// ICETransportPolicy restricts which candidates are used (e.g. webrtc.ICETransportPolicyRelay for TURN only).
	// The zero value allows all candidates.
	ICETransportPolicy webrtc.ICETransportPolicy
	// PublicIPs are external addresses advertised in host candidates when the service runs behind
	// a 1:1 NAT, such as a cloud VM with an Elastic/static public IP.
	PublicIPs []string
	// UDPPortMin is the lower bound of the UDP port range used for ICE. Zero leaves the choice to the OS.
	UDPPortMin uint16
	// UDPPortMax is the upper bound of the UDP port range used for ICE. Zero leaves the choice to the OS.
	UDPPortMax uint16
	// MediaEngine customises the negotiated codecs. When nil, Pion's default codecs are registered.
	MediaEngine *webrtc.MediaEngine
}

// PeerOptions configures a single peer connection and its event callbacks.
// Callbacks are invoked from Pion's internal goroutines and must not block for long.
type PeerOptions struct {
	// Signaler delivers server-generated offers and ICE candidates to the remote peer.
	// It is required for rooms and for Peer.Negotiate; it is optional for answer-only flows.
	Signaler Signaler
	// DisableTrickle makes offers and answers wait for ICE gathering to complete so they contain
	// every candidate. Use it for single request/response exchanges such as WHIP/WHEP-style HTTP endpoints.
	DisableTrickle bool
	// Metadata holds application-defined attributes (e.g. user ID, display name, tenant)
	Metadata map[string]string
	// OnTrack is called when the remote peer starts sending a media track
	OnTrack func(peer Peer, track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver)
	// OnDataChannel is called when the remote peer opens a data channel
	OnDataChannel func(peer Peer, channel *webrtc.DataChannel)
	// OnMessage is called for every message received on any of the peer's data channels.
	// Prefer it over DataChannel.OnMessage, which would replace the handler registered by this package.
	OnMessage func(peer Peer, label string, message webrtc.DataChannelMessage)
	// OnConnectionStateChange is called whenever the aggregate connection state changes
	OnConnectionStateChange func(peer Peer, state webrtc.PeerConnectionState)
}

// RoomOptions configures the behaviour of a multi-party room.
type RoomOptions struct {
	// MaxPeers limits the number of participants. Zero means unlimited.
	MaxPeers int
	// DataOnly disables media: peers are not offered audio/video receive transceivers
	DataOnly bool
	// RelayMessages forwards every message received on the room data channel to all other peers
	RelayMessages bool
	// AutoClose closes the room and removes it from the registry when its last peer leaves
	AutoClose bool
	// OnPeerJoined is called after a peer has joined and its initial offer has been sent
	OnPeerJoined func(room Room, peer Peer)
	// OnPeerLeft is called after a peer has left the room and its connection has been closed
	OnPeerLeft func(room Room, peer Peer)
	// OnMessage is called for every message received on the room data channel
	OnMessage func(room Room, from Peer, message webrtc.DataChannelMessage)
}

// SignalerFuncs is an adapter that allows plain functions to be used as a Signaler.
// Nil functions are treated as no-ops.
//
// Example Usage:
//
//	signaler := SignalerFuncs{
//	    OfferFunc: func(peerID string, offer webrtc.SessionDescription) error {
//	        return conn.WriteJSON(Message{Type: "offer", SDP: &offer})
//	    },
//	    ICECandidateFunc: func(peerID string, candidate webrtc.ICECandidateInit) error {
//	        return conn.WriteJSON(Message{Type: "candidate", Candidate: &candidate})
//	    },
//	}
type SignalerFuncs struct {
	// OfferFunc delivers an SDP offer to the remote peer
	OfferFunc func(peerID string, offer webrtc.SessionDescription) error
	// ICECandidateFunc delivers a local ICE candidate to the remote peer
	ICECandidateFunc func(peerID string, candidate webrtc.ICECandidateInit) error
}

// SignalOffer calls OfferFunc when it is set.
func (s SignalerFuncs) SignalOffer(peerID string, offer webrtc.SessionDescription) error {
	if s.OfferFunc == nil {
		return nil
	}
	return s.OfferFunc(peerID, offer)
}

// SignalICECandidate calls ICECandidateFunc when it is set.
func (s SignalerFuncs) SignalICECandidate(peerID string, candidate webrtc.ICECandidateInit) error {
	if s.ICECandidateFunc == nil {
		return nil
	}
	return s.ICECandidateFunc(peerID, candidate)
}

//WebRTC

// webRTC is the concrete implementation of the WebRTC interface.
// It holds the shared Pion API and the registries of rooms and standalone peers.
//
// Fields:
//   - api: The Pion API carrying codec, interceptor and network settings
//   - configuration: The peer connection configuration (ICE servers and transport policy)
//   - loggR: The logger used for WebRTC events
//   - rooms: Active rooms keyed by room ID
//   - peers: Active standalone peers keyed by peer ID
type webRTC struct {
	api           *webrtc.API
	configuration webrtc.Configuration
	loggR         *logs.BeeLogger

	mu    sync.RWMutex
	rooms map[string]*room
	peers map[string]*peer
}

// NewWebRTC creates a new WebRTC instance with the provided configuration and logger.
// It builds a Pion API with the configured network settings and codecs. Create one instance
// per service and share it, since the API and its settings are reused by every connection.
//
// Parameters:
//   - config: The shared WebRTC configuration (ICE servers, NAT and port settings, codecs)
//   - logger: The logger instance used for WebRTC events; falls back to the global Beego logger when nil
//
// Returns:
//   - A WebRTC implementation ready to create peers and rooms
//   - An error if the network settings are invalid
//
// Example Usage:
//
//	rtc, err := NewWebRTC(WebRTCConfig{
//	    ICEServers: []webrtc.ICEServer{
//	        {URLs: []string{"stun:stun.l.google.com:19302"}},
//	        {URLs: []string{"turn:turn.example.com:3478"}, Username: "user", Credential: "secret"},
//	    },
//	    PublicIPs:  []string{"203.0.113.10"},
//	    UDPPortMin: 50000,
//	    UDPPortMax: 50100,
//	}, logger)
func NewWebRTC(config WebRTCConfig, logger *logs.BeeLogger) (WebRTC, error) {
	if logger == nil {
		logger = logs.GetBeeLogger()
	}

	settingEngine := webrtc.SettingEngine{}
	if config.UDPPortMin != 0 || config.UDPPortMax != 0 {
		if err := settingEngine.SetEphemeralUDPPortRange(config.UDPPortMin, config.UDPPortMax); err != nil {
			return nil, fmt.Errorf("invalid UDP port range: %w", err)
		}
	}
	if len(config.PublicIPs) > 0 {
		rule := webrtc.ICEAddressRewriteRule{
			External:        config.PublicIPs,
			AsCandidateType: webrtc.ICECandidateTypeHost,
		}
		if err := settingEngine.SetICEAddressRewriteRules(rule); err != nil {
			return nil, fmt.Errorf("invalid public IPs: %w", err)
		}
	}

	apiOptions := []func(*webrtc.API){webrtc.WithSettingEngine(settingEngine)}
	if config.MediaEngine != nil {
		apiOptions = append(apiOptions, webrtc.WithMediaEngine(config.MediaEngine))
	}

	iceServers := config.ICEServers
	if len(iceServers) == 0 {
		iceServers = []webrtc.ICEServer{{URLs: []string{DefaultSTUNServer}}}
	}

	logger.Info("📡[WebRTC]: WebRTC engine initialised with %d ICE server(s)", len(iceServers))
	return &webRTC{
		api: webrtc.NewAPI(apiOptions...),
		configuration: webrtc.Configuration{
			ICEServers:         iceServers,
			ICETransportPolicy: config.ICETransportPolicy,
		},
		loggR: logger,
		rooms: make(map[string]*room),
		peers: make(map[string]*peer),
	}, nil
}

// NewPeer creates a standalone peer connection for one-to-one sessions.
// The peer is tracked until it is closed, after which it is removed from the registry automatically.
//
// Parameters:
//   - peerID: The unique identifier of the peer; a random UUID is generated when empty
//   - options: The peer's signaler, metadata and event callbacks
//
// Returns:
//   - The newly created Peer
//   - ErrPeerExists if a standalone peer with the same ID already exists, or an error from Pion
//
// Example Usage:
//
//	peer, err := rtc.NewPeer("", PeerOptions{DisableTrickle: true})
//	answer, err := peer.HandleOffer(ctx, offer)
func (w *webRTC) NewPeer(peerID string, options PeerOptions) (Peer, error) {
	if peerID == "" {
		peerID = uuid.NewString()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, exists := w.peers[peerID]; exists {
		return nil, ErrPeerExists
	}

	p, err := w.newPeer(peerID, options)
	if err != nil {
		return nil, err
	}
	w.peers[peerID] = p

	go func() {
		<-p.Done()
		w.mu.Lock()
		if w.peers[peerID] == p {
			delete(w.peers, peerID)
		}
		w.mu.Unlock()
	}()

	return p, nil
}

// CreateRoom creates a new multi-party room.
//
// Parameters:
//   - roomID: The unique identifier of the room
//   - options: The room's limits, behaviour flags and event callbacks
//
// Returns:
//   - The newly created Room
//   - ErrRoomExists if a room with the same ID already exists
//
// Example Usage:
//
//	room, err := rtc.CreateRoom("support-call-42", RoomOptions{MaxPeers: 2, AutoClose: true})
func (w *webRTC) CreateRoom(roomID string, options RoomOptions) (Room, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, exists := w.rooms[roomID]; exists {
		return nil, ErrRoomExists
	}

	return w.createRoomLocked(roomID, options), nil
}

// GetRoom returns an existing room.
//
// Parameters:
//   - roomID: The unique identifier of the room
//
// Returns:
//   - The Room if it exists
//   - ErrRoomNotFound otherwise
func (w *webRTC) GetRoom(roomID string) (Room, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	r, ok := w.rooms[roomID]
	if !ok {
		return nil, ErrRoomNotFound
	}
	return r, nil
}

// GetOrCreateRoom returns an existing room or creates it when absent.
// The options are only applied when the room is created.
//
// Parameters:
//   - roomID: The unique identifier of the room
//   - options: The options used if the room has to be created
//
// Returns:
//   - The existing or newly created Room
//   - An error is reserved for future validation and is currently always nil
//
// Example Usage:
//
//	room, _ := rtc.GetOrCreateRoom(meetingID, RoomOptions{AutoClose: true, RelayMessages: true})
func (w *webRTC) GetOrCreateRoom(roomID string, options RoomOptions) (Room, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if r, ok := w.rooms[roomID]; ok {
		return r, nil
	}
	return w.createRoomLocked(roomID, options), nil
}

// DeleteRoom closes a room and removes it from the registry.
//
// Parameters:
//   - roomID: The unique identifier of the room
//
// Returns:
//   - ErrRoomNotFound if the room does not exist, or an error from closing the room
func (w *webRTC) DeleteRoom(roomID string) error {
	w.mu.Lock()
	r, ok := w.rooms[roomID]
	if ok {
		delete(w.rooms, roomID)
	}
	w.mu.Unlock()

	if !ok {
		return ErrRoomNotFound
	}
	return r.Close()
}

// Rooms returns a snapshot of all active rooms.
func (w *webRTC) Rooms() []Room {
	w.mu.RLock()
	defer w.mu.RUnlock()

	rooms := make([]Room, 0, len(w.rooms))
	for _, r := range w.rooms {
		rooms = append(rooms, r)
	}
	return rooms
}

// ICEServers returns the configured STUN/TURN servers so they can be handed to clients,
// which must use the same servers to discover compatible candidates.
func (w *webRTC) ICEServers() []webrtc.ICEServer {
	return append([]webrtc.ICEServer(nil), w.configuration.ICEServers...)
}

// API returns the underlying Pion API.
func (w *webRTC) API() *webrtc.API {
	return w.api
}

// Close closes every room and standalone peer managed by this instance.
//
// Returns:
//   - A joined error of every failure encountered while closing, or nil
func (w *webRTC) Close() error {
	w.mu.Lock()
	rooms := w.rooms
	peers := w.peers
	w.rooms = make(map[string]*room)
	w.peers = make(map[string]*peer)
	w.mu.Unlock()

	var errs []error
	for _, r := range rooms {
		errs = append(errs, r.Close())
	}
	for _, p := range peers {
		errs = append(errs, p.Close())
	}

	w.loggR.Info("📡[WebRTC]: Closed %d room(s) and %d standalone peer(s)", len(rooms), len(peers))
	return errors.Join(errs...)
}

// createRoomLocked creates and registers a room. The caller must hold w.mu.
func (w *webRTC) createRoomLocked(roomID string, options RoomOptions) *room {
	r := &room{
		id:      roomID,
		manager: w,
		options: options,
		loggR:   w.loggR,
		peers:   make(map[string]*peer),
		tracks:  make(map[string]*roomTrack),
	}
	w.rooms[roomID] = r

	w.loggR.Info("📡[WebRTC]: Room %s created", roomID)
	return r
}

// removeRoom unregisters a room if it is still the registered instance for its ID.
func (w *webRTC) removeRoom(r *room) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.rooms[r.id] == r {
		delete(w.rooms, r.id)
	}
}

// newPeer creates a Pion PeerConnection and wraps it in a peer with its handlers registered.
func (w *webRTC) newPeer(peerID string, options PeerOptions) (*peer, error) {
	pc, err := w.api.NewPeerConnection(w.configuration)
	if err != nil {
		return nil, fmt.Errorf("failed to create peer connection: %w", err)
	}

	p := &peer{
		id:           peerID,
		pc:           pc,
		options:      options,
		loggR:        w.loggR,
		dataChannels: make(map[string]*webrtc.DataChannel),
		done:         make(chan struct{}),
	}
	p.registerHandlers()

	return p, nil
}

//Peer

// peer is the concrete implementation of the Peer interface.
//
// Fields:
//   - id: The unique identifier of the peer
//   - pc: The underlying Pion PeerConnection
//   - options: The peer's signaler, metadata and callbacks
//   - loggR: The logger used for peer events
//   - mu: Serialises signaling state transitions and guards the fields below it
//   - pendingNegotiation: Set when a renegotiation was requested while another was in progress
//   - pendingCandidates: Remote ICE candidates received before the remote description was set
//   - dataChannels: Open data channels keyed by label
//   - done: Closed when the peer is closed
type peer struct {
	id      string
	pc      *webrtc.PeerConnection
	options PeerOptions
	loggR   *logs.BeeLogger

	mu                 sync.Mutex
	pendingNegotiation bool
	pendingCandidates  []webrtc.ICECandidateInit
	dataChannels       map[string]*webrtc.DataChannel

	done      chan struct{}
	closeOnce sync.Once
}

// ID returns the unique identifier of the peer.
func (p *peer) ID() string {
	return p.id
}

// Metadata returns the application-defined metadata attached to the peer.
func (p *peer) Metadata() map[string]string {
	return p.options.Metadata
}

// CreateOffer creates an SDP offer and applies it as the local description without signaling it.
// When trickle ICE is disabled, it waits for ICE gathering so the offer contains every candidate.
//
// Parameters:
//   - ctx: A context bounding how long to wait for ICE gathering
//
// Returns:
//   - The local offer to deliver to the remote peer
//   - ErrNegotiationInProgress if an offer/answer exchange is still pending, or an error from Pion
//
// Example Usage:
//
//	offer, err := peer.CreateOffer(ctx)
//	// return offer in the HTTP response, then later:
//	err = peer.HandleAnswer(answer)
func (p *peer) CreateOffer(ctx context.Context) (webrtc.SessionDescription, error) {
	offer, deferred, err := p.prepareOffer(ctx, false)
	if err != nil {
		return webrtc.SessionDescription{}, err
	}
	if deferred {
		return webrtc.SessionDescription{}, ErrNegotiationInProgress
	}
	return offer, nil
}

// Negotiate creates an offer and delivers it through the Signaler.
// If an offer/answer exchange is already in progress, the negotiation is deferred and performed
// automatically once the pending answer is handled, so callers can invoke it after every change.
//
// Parameters:
//   - ctx: A context bounding how long to wait for ICE gathering
//
// Returns:
//   - ErrNoSignaler if the peer has no Signaler, or an error from Pion or the Signaler
//
// Example Usage:
//
//	sender, err := peer.AddTrack(track)
//	err = peer.Negotiate(ctx)
func (p *peer) Negotiate(ctx context.Context) error {
	if p.options.Signaler == nil {
		return ErrNoSignaler
	}

	offer, deferred, err := p.prepareOffer(ctx, true)
	if err != nil || deferred {
		return err
	}

	if err := p.options.Signaler.SignalOffer(p.id, offer); err != nil {
		return fmt.Errorf("failed to signal offer: %w", err)
	}
	return nil
}

// HandleOffer applies a remote offer and returns the local answer.
// The server acts as the "impolite" side of perfect negotiation: if the offer collides with a
// pending local offer it is rejected with ErrNegotiationCollision, and the client is expected to
// roll back its offer and answer the server's instead.
//
// Parameters:
//   - ctx: A context bounding how long to wait for ICE gathering
//   - offer: The remote SDP offer
//
// Returns:
//   - The local answer to deliver to the remote peer
//   - ErrInvalidSDPType, ErrNegotiationCollision, or an error from Pion
//
// Example Usage:
//
//	answer, err := peer.HandleOffer(ctx, offer)
//	if err != nil {
//	    return err
//	}
//	return conn.WriteJSON(Message{Type: "answer", SDP: &answer})
func (p *peer) HandleOffer(ctx context.Context, offer webrtc.SessionDescription) (webrtc.SessionDescription, error) {
	if offer.Type != webrtc.SDPTypeOffer {
		return webrtc.SessionDescription{}, fmt.Errorf("%w: expected offer, got %s", ErrInvalidSDPType, offer.Type)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isClosed() {
		return webrtc.SessionDescription{}, ErrPeerClosed
	}
	if p.pc.SignalingState() == webrtc.SignalingStateHaveLocalOffer {
		return webrtc.SessionDescription{}, ErrNegotiationCollision
	}

	if err := p.pc.SetRemoteDescription(offer); err != nil {
		return webrtc.SessionDescription{}, fmt.Errorf("failed to set remote offer: %w", err)
	}
	p.flushPendingCandidatesLocked()

	answer, err := p.pc.CreateAnswer(nil)
	if err != nil {
		return webrtc.SessionDescription{}, fmt.Errorf("failed to create answer: %w", err)
	}
	if err := p.setLocalDescription(ctx, answer); err != nil {
		return webrtc.SessionDescription{}, err
	}

	return *p.pc.LocalDescription(), nil
}

// HandleAnswer applies a remote answer to a previously sent offer.
// Any negotiation deferred while the offer was outstanding is started afterwards.
//
// Parameters:
//   - answer: The remote SDP answer
//
// Returns:
//   - ErrInvalidSDPType, or an error from Pion or from the deferred negotiation
func (p *peer) HandleAnswer(answer webrtc.SessionDescription) error {
	if answer.Type != webrtc.SDPTypeAnswer {
		return fmt.Errorf("%w: expected answer, got %s", ErrInvalidSDPType, answer.Type)
	}

	p.mu.Lock()
	if err := p.pc.SetRemoteDescription(answer); err != nil {
		p.mu.Unlock()
		return fmt.Errorf("failed to set remote answer: %w", err)
	}
	p.flushPendingCandidatesLocked()
	pending := p.pendingNegotiation
	p.mu.Unlock()

	if pending && p.options.Signaler != nil {
		return p.Negotiate(context.Background())
	}
	return nil
}

// AddICECandidate adds a remote ICE candidate. Candidates that arrive before the remote
// description is set are buffered and applied as soon as it is.
//
// Parameters:
//   - candidate: The remote ICE candidate
//
// Returns:
//   - An error from Pion if the candidate is invalid
func (p *peer) AddICECandidate(candidate webrtc.ICECandidateInit) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.pc.RemoteDescription() == nil {
		p.pendingCandidates = append(p.pendingCandidates, candidate)
		return nil
	}
	return p.pc.AddICECandidate(candidate)
}

// CreateDataChannel opens a new data channel. Messages received on it are delivered to PeerOptions.OnMessage.
// A data channel added after the initial negotiation requires a renegotiation (Negotiate).
//
// Parameters:
//   - label: The label identifying the channel on both sides
//   - init: Optional reliability and ordering settings; nil uses reliable, ordered delivery
//
// Returns:
//   - The created data channel, or an error from Pion
func (p *peer) CreateDataChannel(label string, init *webrtc.DataChannelInit) (*webrtc.DataChannel, error) {
	channel, err := p.pc.CreateDataChannel(label, init)
	if err != nil {
		return nil, fmt.Errorf("failed to create data channel %q: %w", label, err)
	}
	p.registerDataChannel(channel)
	return channel, nil
}

// Send sends binary data on the data channel with the given label.
//
// Returns:
//   - ErrDataChannelNotFound or ErrDataChannelNotOpen, or an error from Pion
func (p *peer) Send(label string, data []byte) error {
	channel, err := p.openDataChannel(label)
	if err != nil {
		return err
	}
	return channel.Send(data)
}

// SendText sends a text message on the data channel with the given label.
//
// Returns:
//   - ErrDataChannelNotFound or ErrDataChannelNotOpen, or an error from Pion
func (p *peer) SendText(label string, text string) error {
	channel, err := p.openDataChannel(label)
	if err != nil {
		return err
	}
	return channel.SendText(text)
}

// AddTrack adds a local media track and starts draining its RTCP feedback, which Pion's
// interceptors (NACK, reports) require to function.
//
// Parameters:
//   - track: The local track, e.g. a *webrtc.TrackLocalStaticSample or *webrtc.TrackLocalStaticRTP
//
// Returns:
//   - The RTP sender for the track, used to remove it later
//   - An error from Pion
func (p *peer) AddTrack(track webrtc.TrackLocal) (*webrtc.RTPSender, error) {
	return p.addTrack(track, nil)
}

// RemoveTrack stops sending a local track.
func (p *peer) RemoveTrack(sender *webrtc.RTPSender) error {
	return p.pc.RemoveTrack(sender)
}

// ConnectionState returns the current aggregate connection state.
func (p *peer) ConnectionState() webrtc.PeerConnectionState {
	return p.pc.ConnectionState()
}

// PeerConnection returns the underlying Pion PeerConnection.
func (p *peer) PeerConnection() *webrtc.PeerConnection {
	return p.pc
}

// Done returns a channel that is closed once the peer has been closed.
func (p *peer) Done() <-chan struct{} {
	return p.done
}

// Close closes the peer connection. It is safe to call multiple times.
func (p *peer) Close() error {
	var err error
	p.closeOnce.Do(func() {
		close(p.done)
		err = p.pc.Close()
		p.loggR.Debug("📡[WebRTC]: Peer %s closed", p.id)
	})
	return err
}

// registerHandlers wires Pion's PeerConnection events to the signaler and the peer's callbacks.
func (p *peer) registerHandlers() {
	p.pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		// A nil candidate signals the end of gathering; non-trickle sessions ship candidates in the SDP.
		if candidate == nil || p.options.DisableTrickle {
			return
		}
		if p.options.Signaler == nil {
			p.loggR.Warn("📡[WebRTC]: Peer %s gathered an ICE candidate but has no signaler", p.id)
			return
		}
		if err := p.options.Signaler.SignalICECandidate(p.id, candidate.ToJSON()); err != nil {
			p.loggR.Error("📡[WebRTC]: Failed to signal ICE candidate to peer %s: %v", p.id, err)
		}
	})

	p.pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		p.loggR.Debug("📡[WebRTC]: Peer %s connection state: %s", p.id, state)
		if callback := p.options.OnConnectionStateChange; callback != nil {
			callback(p, state)
		}
		// A failed connection cannot recover without an ICE restart; release its resources.
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			_ = p.Close()
		}
	})

	p.pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		p.loggR.Debug("📡[WebRTC]: Peer %s published %s track %s (%s)", p.id, track.Kind(), track.ID(), track.Codec().MimeType)
		if callback := p.options.OnTrack; callback != nil {
			callback(p, track, receiver)
		}
	})

	p.pc.OnDataChannel(func(channel *webrtc.DataChannel) {
		p.registerDataChannel(channel)
		if callback := p.options.OnDataChannel; callback != nil {
			callback(p, channel)
		}
	})
}

// registerDataChannel tracks a data channel by label and routes its messages to PeerOptions.OnMessage.
func (p *peer) registerDataChannel(channel *webrtc.DataChannel) {
	label := channel.Label()

	p.mu.Lock()
	p.dataChannels[label] = channel
	p.mu.Unlock()

	channel.OnMessage(func(message webrtc.DataChannelMessage) {
		if callback := p.options.OnMessage; callback != nil {
			callback(p, label, message)
		}
	})
	channel.OnClose(func() {
		p.mu.Lock()
		if p.dataChannels[label] == channel {
			delete(p.dataChannels, label)
		}
		p.mu.Unlock()
	})
}

// openDataChannel returns the data channel with the given label if it exists and is open.
func (p *peer) openDataChannel(label string) (*webrtc.DataChannel, error) {
	p.mu.Lock()
	channel, ok := p.dataChannels[label]
	p.mu.Unlock()

	if !ok {
		return nil, ErrDataChannelNotFound
	}
	if channel.ReadyState() != webrtc.DataChannelStateOpen {
		return nil, ErrDataChannelNotOpen
	}
	return channel, nil
}

// addTrack adds a local track and drains its RTCP feedback, passing packets to onRTCP when provided.
func (p *peer) addTrack(track webrtc.TrackLocal, onRTCP func([]rtcp.Packet)) (*webrtc.RTPSender, error) {
	sender, err := p.pc.AddTrack(track)
	if err != nil {
		return nil, fmt.Errorf("failed to add track %s: %w", track.ID(), err)
	}

	go func() {
		for {
			packets, _, err := sender.ReadRTCP()
			if err != nil {
				return
			}
			if onRTCP != nil {
				onRTCP(packets)
			}
		}
	}()

	return sender, nil
}

// prepareOffer creates and applies a local offer. When deferIfBusy is set and an exchange is
// already in progress, it marks a pending negotiation and reports deferred instead of failing.
func (p *peer) prepareOffer(ctx context.Context, deferIfBusy bool) (offer webrtc.SessionDescription, deferred bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isClosed() {
		return webrtc.SessionDescription{}, false, ErrPeerClosed
	}
	if p.pc.SignalingState() != webrtc.SignalingStateStable {
		if deferIfBusy {
			p.pendingNegotiation = true
		}
		return webrtc.SessionDescription{}, true, nil
	}
	p.pendingNegotiation = false

	offer, err = p.pc.CreateOffer(nil)
	if err != nil {
		return webrtc.SessionDescription{}, false, fmt.Errorf("failed to create offer: %w", err)
	}
	if err := p.setLocalDescription(ctx, offer); err != nil {
		return webrtc.SessionDescription{}, false, err
	}

	return *p.pc.LocalDescription(), false, nil
}

// setLocalDescription applies a local description and, when trickle ICE is disabled,
// waits for ICE gathering to complete so the description carries every candidate.
func (p *peer) setLocalDescription(ctx context.Context, description webrtc.SessionDescription) error {
	var gatheringComplete <-chan struct{}
	if p.options.DisableTrickle {
		gatheringComplete = webrtc.GatheringCompletePromise(p.pc)
	}

	if err := p.pc.SetLocalDescription(description); err != nil {
		return fmt.Errorf("failed to set local %s: %w", description.Type, err)
	}
	if gatheringComplete == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, iceGatheringTimeout)
	defer cancel()

	select {
	case <-gatheringComplete:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("failed waiting for ICE gathering: %w", ctx.Err())
	}
}

// flushPendingCandidatesLocked applies buffered remote ICE candidates. The caller must hold p.mu.
func (p *peer) flushPendingCandidatesLocked() {
	for _, candidate := range p.pendingCandidates {
		if err := p.pc.AddICECandidate(candidate); err != nil {
			p.loggR.Warn("📡[WebRTC]: Peer %s rejected buffered ICE candidate: %v", p.id, err)
		}
	}
	p.pendingCandidates = nil
}

// isClosed reports whether the peer has been closed.
func (p *peer) isClosed() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

//Room

// room is the concrete implementation of the Room interface. It forwards every published
// track to all other peers (SFU) and relays messages on the room data channel.
//
// Fields:
//   - id: The unique identifier of the room
//   - manager: The WebRTC instance that owns the room
//   - options: The room's limits, behaviour flags and callbacks
//   - loggR: The logger used for room events
//   - mu: Guards peers, tracks and closed
//   - peers: Members of the room keyed by peer ID
//   - tracks: Published tracks keyed by "<publisherID>/<trackID>"
//   - closed: Set once the room has been closed
type room struct {
	id      string
	manager *webRTC
	options RoomOptions
	loggR   *logs.BeeLogger

	mu     sync.RWMutex
	peers  map[string]*peer
	tracks map[string]*roomTrack
	closed bool
}

// roomTrack is a remote track published by one peer and fanned out to the others.
//
// Fields:
//   - key: The track's key in room.tracks
//   - publisher: The peer that publishes the track
//   - remote: The incoming track from the publisher
//   - local: The outgoing track shared by every subscriber
//   - senders: The RTP sender of each subscriber keyed by peer ID
type roomTrack struct {
	key       string
	publisher *peer
	remote    *webrtc.TrackRemote
	local     *webrtc.TrackLocalStaticRTP
	senders   map[string]*webrtc.RTPSender
}

// ID returns the unique identifier of the room.
func (r *room) ID() string {
	return r.id
}

// Join adds a peer to the room. The peer is offered receive transceivers for audio and video
// (unless DataOnly is set) and the room data channel, subscribed to every published track,
// and sent an initial offer through its Signaler.
//
// Forwarded tracks use the publisher's peer ID as their stream ID, so clients can map
// MediaStream.id back to the participant that owns the media.
//
// Parameters:
//   - peerID: The unique identifier of the peer within the room
//   - options: The peer's signaler (required), metadata and callbacks
//
// Returns:
//   - The joined Peer
//   - ErrNoSignaler, ErrRoomClosed, ErrPeerExists, ErrRoomFull, or an error from Pion
//
// Example Usage:
//
//	peer, err := room.Join(claims.Id, PeerOptions{
//	    Signaler: signaler,
//	    Metadata: map[string]string{"name": claims.Name},
//	})
func (r *room) Join(peerID string, options PeerOptions) (Peer, error) {
	if options.Signaler == nil {
		return nil, ErrNoSignaler
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, ErrRoomClosed
	}
	if _, exists := r.peers[peerID]; exists {
		r.mu.Unlock()
		return nil, ErrPeerExists
	}
	if r.options.MaxPeers > 0 && len(r.peers) >= r.options.MaxPeers {
		r.mu.Unlock()
		return nil, ErrRoomFull
	}

	p, err := r.manager.newPeer(peerID, r.wrapPeerOptions(options))
	if err != nil {
		r.mu.Unlock()
		return nil, err
	}
	if err := r.preparePeerLocked(p); err != nil {
		r.mu.Unlock()
		_ = p.Close()
		return nil, err
	}
	r.peers[peerID] = p
	r.mu.Unlock()

	if err := p.Negotiate(context.Background()); err != nil {
		_ = r.Leave(peerID)
		return nil, fmt.Errorf("failed to negotiate with peer %s: %w", peerID, err)
	}

	r.loggR.Info("📡[WebRTC]: Peer %s joined room %s", peerID, r.id)
	if callback := r.options.OnPeerJoined; callback != nil {
		callback(r, p)
	}
	return p, nil
}

// Leave removes a peer from the room, closes its connection, unpublishes its tracks
// from the other peers and renegotiates with them.
//
// Parameters:
//   - peerID: The unique identifier of the peer
//
// Returns:
//   - ErrPeerNotFound if the peer is not a member of the room
func (r *room) Leave(peerID string) error {
	r.mu.Lock()
	p, ok := r.peers[peerID]
	if !ok {
		r.mu.Unlock()
		return ErrPeerNotFound
	}
	delete(r.peers, peerID)

	affected := make(map[string]*peer)
	for key, track := range r.tracks {
		if track.publisher == p {
			delete(r.tracks, key)
			for _, subscriber := range r.detachTrackLocked(track) {
				affected[subscriber.id] = subscriber
			}
			continue
		}
		delete(track.senders, peerID)
	}
	empty := len(r.peers) == 0
	r.mu.Unlock()

	_ = p.Close()
	r.renegotiate(affected)

	r.loggR.Info("📡[WebRTC]: Peer %s left room %s", peerID, r.id)
	if callback := r.options.OnPeerLeft; callback != nil {
		callback(r, p)
	}

	if empty && r.options.AutoClose {
		r.manager.removeRoom(r)
		return r.Close()
	}
	return nil
}

// GetPeer returns a member of the room.
//
// Returns:
//   - ErrPeerNotFound if the peer is not a member of the room
func (r *room) GetPeer(peerID string) (Peer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	p, ok := r.peers[peerID]
	if !ok {
		return nil, ErrPeerNotFound
	}
	return p, nil
}

// Peers returns a snapshot of all members of the room.
func (r *room) Peers() []Peer {
	r.mu.RLock()
	defer r.mu.RUnlock()

	peers := make([]Peer, 0, len(r.peers))
	for _, p := range r.peers {
		peers = append(peers, p)
	}
	return peers
}

// Broadcast sends binary data on the room data channel to every peer except the excluded ones.
// Peers whose data channel is not open yet are skipped.
//
// Returns:
//   - A joined error of every peer that failed to receive the message, or nil
//
// Example Usage:
//
//	payload, _ := json.Marshal(Event{Type: "recording-started"})
//	err := room.Broadcast(payload)
func (r *room) Broadcast(data []byte, excludePeerIDs ...string) error {
	return r.broadcast(func(p *peer) error {
		return p.Send(RoomDataChannelLabel, data)
	}, excludePeerIDs)
}

// BroadcastText sends a text message on the room data channel to every peer except the excluded ones.
// Peers whose data channel is not open yet are skipped.
//
// Returns:
//   - A joined error of every peer that failed to receive the message, or nil
func (r *room) BroadcastText(text string, excludePeerIDs ...string) error {
	return r.broadcast(func(p *peer) error {
		return p.SendText(RoomDataChannelLabel, text)
	}, excludePeerIDs)
}

// Close disconnects every peer and marks the room as closed. It is safe to call multiple times.
// Use WebRTC.DeleteRoom to also remove the room from the registry.
func (r *room) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	peers := r.peers
	r.peers = make(map[string]*peer)
	r.tracks = make(map[string]*roomTrack)
	r.mu.Unlock()

	var errs []error
	for _, p := range peers {
		errs = append(errs, p.Close())
		if callback := r.options.OnPeerLeft; callback != nil {
			callback(r, p)
		}
	}

	r.loggR.Info("📡[WebRTC]: Room %s closed", r.id)
	return errors.Join(errs...)
}

// wrapPeerOptions chains the room's internal handlers in front of the caller's callbacks.
func (r *room) wrapPeerOptions(options PeerOptions) PeerOptions {
	wrapped := options

	wrapped.OnTrack = func(p Peer, track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		r.publish(p.ID(), track)
		if options.OnTrack != nil {
			options.OnTrack(p, track, receiver)
		}
	}

	wrapped.OnConnectionStateChange = func(p Peer, state webrtc.PeerConnectionState) {
		switch state {
		case webrtc.PeerConnectionStateConnected:
			// New subscribers need a keyframe before they can decode forwarded video.
			r.requestKeyFrames(p.ID())
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			if err := r.Leave(p.ID()); err != nil && !errors.Is(err, ErrPeerNotFound) {
				r.loggR.Warn("📡[WebRTC]: Failed to remove peer %s from room %s: %v", p.ID(), r.id, err)
			}
		}
		if options.OnConnectionStateChange != nil {
			options.OnConnectionStateChange(p, state)
		}
	}

	wrapped.OnMessage = func(p Peer, label string, message webrtc.DataChannelMessage) {
		if label == RoomDataChannelLabel {
			r.handleMessage(p, message)
		}
		if options.OnMessage != nil {
			options.OnMessage(p, label, message)
		}
	}

	return wrapped
}

// preparePeerLocked adds the room's transceivers, data channel and existing tracks to a new peer.
// The caller must hold r.mu.
func (r *room) preparePeerLocked(p *peer) error {
	if !r.options.DataOnly {
		receiveOnly := webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}
		for _, kind := range []webrtc.RTPCodecType{webrtc.RTPCodecTypeAudio, webrtc.RTPCodecTypeVideo} {
			if _, err := p.pc.AddTransceiverFromKind(kind, receiveOnly); err != nil {
				return fmt.Errorf("failed to add %s transceiver: %w", kind, err)
			}
		}
	}

	if _, err := p.CreateDataChannel(RoomDataChannelLabel, nil); err != nil {
		return err
	}

	for _, track := range r.tracks {
		if err := r.subscribeLocked(p, track); err != nil {
			return err
		}
	}
	return nil
}

// publish registers a track received from a peer, subscribes every other peer to it,
// and starts forwarding its RTP packets.
func (r *room) publish(publisherID string, remote *webrtc.TrackRemote) {
	r.mu.Lock()
	publisher, ok := r.peers[publisherID]
	if !ok || r.closed {
		r.mu.Unlock()
		return
	}

	local, err := webrtc.NewTrackLocalStaticRTP(remote.Codec().RTPCodecCapability, remote.ID(), publisherID)
	if err != nil {
		r.mu.Unlock()
		r.loggR.Error("📡[WebRTC]: Failed to create forwarding track for peer %s: %v", publisherID, err)
		return
	}

	track := &roomTrack{
		key:       publisherID + "/" + remote.ID(),
		publisher: publisher,
		remote:    remote,
		local:     local,
		senders:   make(map[string]*webrtc.RTPSender),
	}
	r.tracks[track.key] = track

	subscribers := make(map[string]*peer)
	for id, subscriber := range r.peers {
		if id == publisherID {
			continue
		}
		if err := r.subscribeLocked(subscriber, track); err != nil {
			r.loggR.Warn("📡[WebRTC]: Failed to subscribe peer %s to %s: %v", id, track.key, err)
			continue
		}
		subscribers[id] = subscriber
	}
	r.mu.Unlock()

	r.renegotiate(subscribers)
	go r.forward(track)
}

// forward copies RTP packets from the publisher to every subscriber until the publisher's track ends.
func (r *room) forward(track *roomTrack) {
	for {
		packet, _, err := track.remote.ReadRTP()
		if err != nil {
			break
		}
		// Write errors only concern individual subscribers and must not stop the fan-out.
		_ = track.local.WriteRTP(packet)
	}
	r.unpublish(track)
}

// unpublish removes a track from the room and renegotiates with its subscribers.
// It is a no-op if the track has already been removed or replaced.
func (r *room) unpublish(track *roomTrack) {
	r.mu.Lock()
	if r.tracks[track.key] != track {
		r.mu.Unlock()
		return
	}
	delete(r.tracks, track.key)

	affected := make(map[string]*peer)
	for _, subscriber := range r.detachTrackLocked(track) {
		affected[subscriber.id] = subscriber
	}
	r.mu.Unlock()

	r.renegotiate(affected)
}

// subscribeLocked adds a published track to a subscriber and forwards its keyframe requests
// to the publisher. The caller must hold r.mu.
func (r *room) subscribeLocked(subscriber *peer, track *roomTrack) error {
	sender, err := subscriber.addTrack(track.local, func(packets []rtcp.Packet) {
		for _, packet := range packets {
			switch packet.(type) {
			case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
				r.requestKeyFrame(track)
			}
		}
	})
	if err != nil {
		return err
	}
	track.senders[subscriber.id] = sender
	return nil
}

// detachTrackLocked removes a track from every subscriber that is still in the room and
// returns those subscribers. The caller must hold r.mu.
func (r *room) detachTrackLocked(track *roomTrack) []*peer {
	subscribers := make([]*peer, 0, len(track.senders))
	for id, sender := range track.senders {
		subscriber, ok := r.peers[id]
		if !ok {
			continue
		}
		if err := subscriber.RemoveTrack(sender); err != nil {
			r.loggR.Warn("📡[WebRTC]: Failed to remove %s from peer %s: %v", track.key, id, err)
			continue
		}
		subscribers = append(subscribers, subscriber)
	}
	return subscribers
}

// renegotiate sends a fresh offer to each peer concurrently so a slow signaler cannot stall the others.
func (r *room) renegotiate(peers map[string]*peer) {
	for _, p := range peers {
		go func(p *peer) {
			// Peers closing concurrently are expected to fail renegotiation and are not worth reporting.
			if err := p.Negotiate(context.Background()); err != nil && !p.isClosed() {
				r.loggR.Warn("📡[WebRTC]: Failed to renegotiate with peer %s in room %s: %v", p.id, r.id, err)
			}
		}(p)
	}
}

// requestKeyFrames asks the publisher of every track the given peer subscribes to for a keyframe.
func (r *room) requestKeyFrames(subscriberID string) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, track := range r.tracks {
		if _, subscribed := track.senders[subscriberID]; subscribed {
			r.requestKeyFrame(track)
		}
	}
}

// requestKeyFrame sends a Picture Loss Indication for a video track to its publisher.
func (r *room) requestKeyFrame(track *roomTrack) {
	if track.remote.Kind() != webrtc.RTPCodecTypeVideo {
		return
	}
	pli := &rtcp.PictureLossIndication{MediaSSRC: uint32(track.remote.SSRC())}
	if err := track.publisher.pc.WriteRTCP([]rtcp.Packet{pli}); err != nil {
		r.loggR.Debug("📡[WebRTC]: Failed to request keyframe for %s: %v", track.key, err)
	}
}

// handleMessage relays a room data channel message and notifies the room's OnMessage callback.
func (r *room) handleMessage(from Peer, message webrtc.DataChannelMessage) {
	if r.options.RelayMessages {
		var err error
		if message.IsString {
			err = r.BroadcastText(string(message.Data), from.ID())
		} else {
			err = r.Broadcast(message.Data, from.ID())
		}
		if err != nil {
			r.loggR.Warn("📡[WebRTC]: Failed to relay message from peer %s in room %s: %v", from.ID(), r.id, err)
		}
	}
	if callback := r.options.OnMessage; callback != nil {
		callback(r, from, message)
	}
}

// broadcast applies send to every peer except the excluded ones, skipping peers whose
// room data channel is not open yet.
func (r *room) broadcast(send func(p *peer) error, excludePeerIDs []string) error {
	excluded := make(map[string]struct{}, len(excludePeerIDs))
	for _, id := range excludePeerIDs {
		excluded[id] = struct{}{}
	}

	r.mu.RLock()
	recipients := make([]*peer, 0, len(r.peers))
	for id, p := range r.peers {
		if _, skip := excluded[id]; !skip {
			recipients = append(recipients, p)
		}
	}
	r.mu.RUnlock()

	var errs []error
	for _, p := range recipients {
		err := send(p)
		if err == nil || errors.Is(err, ErrDataChannelNotOpen) || errors.Is(err, ErrDataChannelNotFound) {
			continue
		}
		errs = append(errs, fmt.Errorf("peer %s: %w", p.id, err))
	}
	return errors.Join(errs...)
}
