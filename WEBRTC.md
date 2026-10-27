# domecore WebRTC

`domecore` ships a small WebRTC layer that lets backend services set up **peer-to-peer connections** and **multi-party rooms** without dealing with the details of [Pion WebRTC](https://github.com/pion/webrtc) (`github.com/pion/webrtc/v4`).

It handles:

- Offer/answer negotiation and renegotiation, including glare (offer collision) handling
- Trickle ICE, with remote candidates buffered until the remote description is set
- Non-trickle (single request/response) exchanges for HTTP endpoints
- Rooms that act as an **SFU** (Selective Forwarding Unit). Each participant's audio/video is forwarded to everyone else, and keyframe requests are passed back to the publisher.
- A room-wide data channel with optional message relay and server-side broadcast
- STUN/TURN, 1:1 NAT (public IP) and UDP port range configuration

It does **not** pick a signaling transport. You decide how SDP and ICE candidates travel between the client and your service (WebSocket, NATS, Connect streams, HTTP), and plug that in through the `Signaler` interface.

---

## Contents

1. [Installation](#installation)
2. [Core concepts](#core-concepts)
3. [Configuration](#configuration)
4. [Usage 1: Video room over WebSocket (SFU)](#usage-1-video-room-over-websocket-sfu)
5. [Usage 2: Signaling over NATS or Connect RPC](#usage-2-signaling-over-nats-or-connect-rpc)
6. [Usage 3: One-to-one session over plain HTTP (non-trickle)](#usage-3-one-to-one-session-over-plain-http-non-trickle)
7. [Usage 4: Server-initiated session that streams media to a client](#usage-4-server-initiated-session-that-streams-media-to-a-client)
8. [Usage 5: Data-only rooms (chat, presence, collaboration)](#usage-5-data-only-rooms-chat-presence-collaboration)
9. [Usage 6: Combining with domecore auth and tenancy](#usage-6-combining-with-domecore-auth-and-tenancy)
10. [Negotiation rules for clients](#negotiation-rules-for-clients)
11. [Lifecycle and cleanup](#lifecycle-and-cleanup)
12. [Errors](#errors)
13. [Production checklist](#production-checklist)
14. [Limitations](#limitations)

---

## Installation

```bash
go get github.com/Vigil-HQ/domecore-go
```

```go
import (
    "github.com/Vigil-HQ/domecore-go/domecore"
    "github.com/pion/webrtc/v4"
)
```

The API works with Pion types (`webrtc.SessionDescription`, `webrtc.ICECandidateInit`, `webrtc.TrackRemote`, …). These serialise to the same JSON shape the browser's `RTCSessionDescription` and `RTCIceCandidate` use, so you can send them to clients as-is.

---

## Core concepts

| Type | Purpose |
|---|---|
| `WebRTC` | Entry point. Create **one per service** with `NewWebRTC`. It owns the shared Pion API (codecs, interceptors, network settings) and keeps track of rooms and standalone peers. |
| `Room` | A multi-party session. Each member's published tracks go to every other member. Every member also gets a data channel labelled `domecore-room` (`RoomDataChannelLabel`). |
| `Peer` | One server-side `RTCPeerConnection`. Created through `Room.Join` (room member) or `WebRTC.NewPeer` (standalone, one-to-one). |
| `Signaler` | **Your** outbound signaling transport. The library calls it to send offers and ICE candidates to the remote client. |
| `SignalerFuncs` | Adapter that turns two plain functions into a `Signaler`. |

What travels in which direction:

```
             ┌──────────── your transport (WS / NATS / Connect / HTTP) ────────────┐
 Client  ──► │ offer / answer / ICE candidate ──► Peer.HandleOffer / HandleAnswer  │
             │                                    / AddICECandidate                │
 Client  ◄── │ offer / ICE candidate          ◄── Signaler.SignalOffer             │
             │                                    / SignalICECandidate             │
             └─────────────────────────────────────────────────────────────────────┘
```

Every `Signaler` method gets the `peerID`, so a single signaler can serve many peers (for example, by looking up the matching socket).

---

## Configuration

```go
rtc, err := domecore.NewWebRTC(domecore.WebRTCConfig{
    // STUN/TURN servers. Defaults to stun:stun.l.google.com:19302 when empty.
    ICEServers: []webrtc.ICEServer{
        {URLs: []string{"stun:stun.l.google.com:19302"}},
        {
            URLs:       []string{"turn:turn.example.com:3478?transport=udp"},
            Username:   "svc-user",
            Credential: "svc-secret",
        },
    },

    // Optional: only use relay (TURN) candidates.
    // ICETransportPolicy: webrtc.ICETransportPolicyRelay,

    // Public address(es) to advertise when the service runs behind a 1:1 NAT
    // (e.g. a cloud VM with a static/elastic IP).
    PublicIPs: []string{"203.0.113.10"},

    // Restrict the UDP ports used for media so they can be opened in a firewall/security group.
    UDPPortMin: 50000,
    UDPPortMax: 50100,

    // Optional: restrict or extend codecs. nil = Pion defaults (Opus, VP8, VP9, H264, AV1, …).
    // MediaEngine: customMediaEngine,
}, logger) // *logs.BeeLogger. Pass nil to use the global Beego logger.
if err != nil {
    log.Fatal(err)
}
defer rtc.Close()
```

Give clients the same ICE servers so both sides can find compatible candidates:

```go
json.NewEncoder(w).Encode(map[string]any{"iceServers": rtc.ICEServers()})
```

### Options at a glance

`PeerOptions` (per connection):

| Field | Description |
|---|---|
| `Signaler` | Sends offers and ICE candidates to the client. **Required** for rooms and for `Peer.Negotiate`. |
| `DisableTrickle` | Waits for ICE gathering so the SDP contains all candidates. Use this for single request/response HTTP flows. |
| `Metadata` | Your own key/values (user ID, display name, tenant, …), read back with `Peer.Metadata()`. |
| `OnTrack` | The remote peer started sending a media track. |
| `OnDataChannel` | The remote peer opened a data channel. |
| `OnMessage` | A message arrived on **any** of the peer's data channels (`label` tells you which one). |
| `OnConnectionStateChange` | The connection state changed (`connecting`, `connected`, `disconnected`, `failed`, `closed`). |

`RoomOptions` (per room):

| Field | Description |
|---|---|
| `MaxPeers` | Maximum number of members. `0` means unlimited. |
| `DataOnly` | Don't offer audio/video. Members only get the room data channel. |
| `RelayMessages` | Forward every message on the room data channel to all other members. |
| `AutoClose` | Close the room and remove it from the registry when the last member leaves. |
| `OnPeerJoined` / `OnPeerLeft` | Membership hooks (e.g. for presence events or auditing). |
| `OnMessage` | A message arrived on the room data channel. |

> Callbacks run on Pion's internal goroutines. Keep them short, and move slow work (DB writes, network calls) to your own goroutine.

---

## Usage 1: Video room over WebSocket (SFU)

This is the most common case: several browser clients join a call, and the service forwards everyone's camera and microphone to everyone else.

### Server

The example uses `github.com/gorilla/websocket`, but any WebSocket library works. A WebSocket connection usually allows **only one concurrent writer**, and the signaler is called from several goroutines, so writes are serialised with a mutex.

```go
// SignalMessage is the JSON envelope exchanged with the browser.
type SignalMessage struct {
    Type      string                     `json:"type"` // "offer" | "answer" | "candidate"
    SDP       *webrtc.SessionDescription `json:"sdp,omitempty"`
    Candidate *webrtc.ICECandidateInit   `json:"candidate,omitempty"`
}

// wsSignaler implements domecore.Signaler on top of a single WebSocket connection.
type wsSignaler struct {
    mu   sync.Mutex
    conn *websocket.Conn
}

func (s *wsSignaler) write(message SignalMessage) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    return s.conn.WriteJSON(message)
}

func (s *wsSignaler) SignalOffer(_ string, offer webrtc.SessionDescription) error {
    return s.write(SignalMessage{Type: "offer", SDP: &offer})
}

func (s *wsSignaler) SignalICECandidate(_ string, candidate webrtc.ICECandidateInit) error {
    return s.write(SignalMessage{Type: "candidate", Candidate: &candidate})
}

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

func roomHandler(rtc domecore.WebRTC) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        conn, err := upgrader.Upgrade(w, r, nil)
        if err != nil {
            return
        }
        defer conn.Close()

        room, _ := rtc.GetOrCreateRoom(r.URL.Query().Get("room"), domecore.RoomOptions{
            MaxPeers:      8,
            AutoClose:     true,
            RelayMessages: true,
        })

        // Join sends the first offer through the signaler straight away.
        peer, err := room.Join(r.URL.Query().Get("user"), domecore.PeerOptions{
            Signaler: &wsSignaler{conn: conn},
        })
        if err != nil {
            http.Error(w, err.Error(), http.StatusConflict)
            return
        }
        defer room.Leave(peer.ID())

        // Read loop: send answers and candidates from the browser to the peer.
        for {
            var message SignalMessage
            if err := conn.ReadJSON(&message); err != nil {
                return // socket closed -> deferred Leave cleans up
            }
            switch message.Type {
            case "answer":
                err = peer.HandleAnswer(*message.SDP)
            case "candidate":
                err = peer.AddICECandidate(*message.Candidate)
            }
            if err != nil {
                log.Printf("signal error for %s: %v", peer.ID(), err)
            }
        }
    }
}
```

What happens:

1. `Join` sets up the peer with receive-only audio and video transceivers and the `domecore-room` data channel. It subscribes the peer to every track already published in the room and sends the **first offer**.
2. The browser answers. If it attached its camera/mic, those tracks show up through `OnTrack`, and the room starts forwarding them.
3. When any member publishes or leaves, the room **renegotiates** with the affected members by sending them fresh offers. The browser just answers each one.

### Browser

```js
const ws = new WebSocket(`wss://api.example.com/rtc?room=standup&user=${userId}`);
const pc = new RTCPeerConnection({ iceServers });           // from rtc.ICEServers()
const local = await navigator.mediaDevices.getUserMedia({ audio: true, video: true });
let published = false;

pc.onicecandidate = ({ candidate }) =>
  candidate && ws.send(JSON.stringify({ type: "candidate", candidate }));

// Forwarded tracks use the publisher's peer ID as the stream ID.
pc.ontrack = ({ track, streams: [stream] }) => renderParticipant(stream.id, stream);

// Room data channel created by the server.
pc.ondatachannel = ({ channel }) => {
  if (channel.label === "domecore-room") {
    channel.onmessage = (e) => console.log("room message", e.data);
    window.roomChannel = channel;
  }
};

ws.onmessage = async ({ data }) => {
  const message = JSON.parse(data);
  if (message.type === "offer") {
    await pc.setRemoteDescription(message.sdp);
    if (!published) {
      // Added after the server's offer so the tracks reuse its recvonly transceivers.
      local.getTracks().forEach((t) => pc.addTrack(t, local));
      published = true;
    }
    await pc.setLocalDescription(await pc.createAnswer());
    ws.send(JSON.stringify({ type: "answer", sdp: pc.localDescription }));
  } else if (message.type === "candidate") {
    await pc.addIceCandidate(message.candidate);
  }
};
```

> In rooms the **server is always the offerer**. The browser only answers, so a room client never needs perfect-negotiation logic.

---

## Usage 2: Signaling over NATS or Connect RPC

The signaler is just an interface, so a service that already talks to clients through a gateway can route signaling over NATS (or any message bus) instead of holding sockets itself.

```go
// natsSignaler publishes signaling messages to per-peer subjects that a gateway forwards to clients.
type natsSignaler struct {
    nc     *nats.Conn
    roomID string
}

func (s natsSignaler) SignalOffer(peerID string, offer webrtc.SessionDescription) error {
    payload, _ := json.Marshal(SignalMessage{Type: "offer", SDP: &offer})
    return s.nc.Publish(fmt.Sprintf("rtc.%s.%s.out", s.roomID, peerID), payload)
}

func (s natsSignaler) SignalICECandidate(peerID string, candidate webrtc.ICECandidateInit) error {
    payload, _ := json.Marshal(SignalMessage{Type: "candidate", Candidate: &candidate})
    return s.nc.Publish(fmt.Sprintf("rtc.%s.%s.out", s.roomID, peerID), payload)
}

// Inbound: answers and candidates from the client arrive on rtc.<room>.<peer>.in
nc.Subscribe("rtc.*.*.in", func(msg *nats.Msg) {
    parts := strings.Split(msg.Subject, ".")
    room, err := rtc.GetRoom(parts[1])
    if err != nil {
        return
    }
    peer, err := room.GetPeer(parts[2])
    if err != nil {
        return
    }

    var message SignalMessage
    if err := json.Unmarshal(msg.Data, &message); err != nil {
        return
    }
    switch message.Type {
    case "answer":
        _ = peer.HandleAnswer(*message.SDP)
    case "candidate":
        _ = peer.AddICECandidate(*message.Candidate)
    }
})
```

For quick wiring (tests, prototypes, Connect server-streams), `SignalerFuncs` avoids declaring a type:

```go
signaler := domecore.SignalerFuncs{
    OfferFunc: func(peerID string, offer webrtc.SessionDescription) error {
        return stream.Send(&rtcv1.SignalResponse{Type: "offer", Sdp: offer.SDP})
    },
    ICECandidateFunc: func(peerID string, candidate webrtc.ICECandidateInit) error {
        return stream.Send(&rtcv1.SignalResponse{Type: "candidate", Candidate: candidate.Candidate})
    },
}
```

> Connect/gRPC streams also allow only one concurrent `Send`, so protect them with a mutex just like the WebSocket example.

---

## Usage 3: One-to-one session over plain HTTP (non-trickle)

For simple one-to-one sessions (e.g. a client sending a camera feed to an analysis service, or a WHIP-style ingest endpoint), a single HTTP request/response is enough. `DisableTrickle` waits for ICE gathering, so the answer already holds every candidate and no signaler is needed.

```go
func ingestHandler(rtc domecore.WebRTC) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        var offer webrtc.SessionDescription
        if err := json.NewDecoder(r.Body).Decode(&offer); err != nil {
            http.Error(w, "invalid offer", http.StatusBadRequest)
            return
        }

        peer, err := rtc.NewPeer("", domecore.PeerOptions{
            DisableTrickle: true,
            OnTrack: func(p domecore.Peer, track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
                go consume(p.ID(), track) // e.g. decode, record, run inference
            },
            OnMessage: func(p domecore.Peer, label string, message webrtc.DataChannelMessage) {
                log.Printf("[%s] %s: %s", p.ID(), label, message.Data)
            },
        })
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }

        answer, err := peer.HandleOffer(r.Context(), offer)
        if err != nil {
            _ = peer.Close()
            http.Error(w, err.Error(), http.StatusBadRequest)
            return
        }

        w.Header().Set("Location", "/rtc/sessions/"+peer.ID()) // lets the client DELETE it later
        _ = json.NewEncoder(w).Encode(answer)
    }
}

func consume(peerID string, track *webrtc.TrackRemote) {
    for {
        packet, _, err := track.ReadRTP()
        if err != nil {
            return // track ended or peer closed
        }
        _ = packet // process RTP payload
    }
}
```

Client side, the browser should also wait for gathering before posting its offer:

```js
const pc = new RTCPeerConnection({ iceServers });
stream.getTracks().forEach((t) => pc.addTrack(t, stream));
await pc.setLocalDescription(await pc.createOffer());
await new Promise((r) => pc.iceGatheringState === "complete" ? r()
  : pc.addEventListener("icegatheringstatechange", () => pc.iceGatheringState === "complete" && r()));
const res = await fetch("/rtc/ingest", { method: "POST", body: JSON.stringify(pc.localDescription) });
await pc.setRemoteDescription(await res.json());
```

To end the session, look the peer up by ID in your own map, or simply call `peer.Close()` when your DELETE handler runs. Standalone peers drop out of the registry automatically once they are closed.

---

## Usage 4: Server-initiated session that streams media to a client

A service can also be the **media source**: text-to-speech audio, a rendered video feed, a camera bridge, etc. Create a local track, add it, and let the server offer.

```go
peer, err := rtc.NewPeer(sessionID, domecore.PeerOptions{Signaler: signaler})
if err != nil {
    return err
}

audio, err := webrtc.NewTrackLocalStaticSample(
    webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "tts", sessionID,
)
if err != nil {
    return err
}
if _, err := peer.AddTrack(audio); err != nil { // RTCP is drained for you
    return err
}

// Offer goes out through the signaler; the client's answer comes back via peer.HandleAnswer.
if err := peer.Negotiate(ctx); err != nil {
    return err
}

// Once connected, write encoded Opus frames.
go func() {
    for frame := range opusFrames {
        _ = audio.WriteSample(media.Sample{Data: frame, Duration: 20 * time.Millisecond})
    }
}()
```

If you can't push the offer (e.g. plain HTTP), return it in a response and wait for the answer in a second request:

```go
offer, err := peer.CreateOffer(ctx)      // POST /sessions -> returns offer
// ...
err = peer.HandleAnswer(answerFromClient) // PATCH /sessions/{id} -> applies answer
```

Changes made **after** the session is up (`AddTrack`, `RemoveTrack`, `CreateDataChannel`) need another `peer.Negotiate(ctx)`. You can safely call `Negotiate` while an earlier offer is still unanswered. The new negotiation is queued and runs automatically once `HandleAnswer` gets the pending answer.

---

## Usage 5: Data-only rooms (chat, presence, collaboration)

WebRTC data channels give low-latency, peer-to-server messaging. That suits live cursors, game state, chat and presence, with no media involved.

```go
room, err := rtc.CreateRoom(docID, domecore.RoomOptions{
    DataOnly:      true, // no audio/video transceivers
    RelayMessages: true, // every message is forwarded to the other members
    AutoClose:     true,
    OnMessage: func(room domecore.Room, from domecore.Peer, message webrtc.DataChannelMessage) {
        persistEdit(room.ID(), from.ID(), message.Data) // server still sees everything
    },
    OnPeerJoined: func(room domecore.Room, peer domecore.Peer) {
        _ = room.BroadcastText(fmt.Sprintf(`{"type":"joined","user":%q}`, peer.ID()), peer.ID())
    },
    OnPeerLeft: func(room domecore.Room, peer domecore.Peer) {
        _ = room.BroadcastText(fmt.Sprintf(`{"type":"left","user":%q}`, peer.ID()))
    },
})
```

Server-side messages:

```go
room.Broadcast(payload)                        // binary to everyone
room.BroadcastText(`{"type":"lock"}`, ownerID) // text to everyone except ownerID

peer, _ := room.GetPeer(userID)
peer.SendText(domecore.RoomDataChannelLabel, `{"type":"kick"}`) // just one member
```

Members whose data channel isn't open yet are skipped silently by `Broadcast`, so you don't need to track readiness.

For unreliable or unordered delivery (e.g. high-frequency positions), create an extra channel on a standalone peer:

```go
maxRetransmits := uint16(0)
ordered := false
_, err := peer.CreateDataChannel("positions", &webrtc.DataChannelInit{
    Ordered: &ordered, MaxRetransmits: &maxRetransmits,
})
_ = peer.Negotiate(ctx)
```

---

## Usage 6: Combining with domecore auth and tenancy

The WebRTC layer fits next to the existing `Authenticator`, `ContextHelper` and `AuthZ`. Authenticate the signaling request, then use the token claims for the peer ID and metadata. Scope room IDs by tenant so rooms can never cross tenants.

```go
func (s *RTCService) Join(ctx context.Context, roomName string, signaler domecore.Signaler) (domecore.Peer, error) {
    claims := s.contextHelper.GetUserClaims(ctx)

    tenantID, err := s.contextHelper.GetTenant(ctx)
    if err != nil {
        return nil, domecore.ErrMissingTenantHeader
    }

    if _, err := s.authZ.HasPermission(claims, tenantID, "rtc", "room", "join"); err != nil {
        return nil, connect.NewError(connect.CodePermissionDenied, err)
    }

    room, _ := s.rtc.GetOrCreateRoom(tenantID+":"+roomName, domecore.RoomOptions{AutoClose: true})
    return room.Join(claims.Id, domecore.PeerOptions{
        Signaler: signaler,
        Metadata: map[string]string{
            "name":   claims.Name,
            "email":  claims.Email,
            "tenant": tenantID,
        },
    })
}
```

Because forwarded tracks use the publisher's peer ID (here the user ID) as their stream ID, clients can map each incoming `MediaStream.id` straight to a participant.

---

## Negotiation rules for clients

| Situation | What the server does | What the client must do |
|---|---|---|
| Room member | Always sends the offer (on join and whenever tracks change). | Answer every offer. Never send offers. |
| Standalone, client-initiated | `HandleOffer` returns the answer. | Send offers, apply the returned answer. |
| Standalone, server-initiated | `Negotiate` / `CreateOffer` produce the offer. | Answer it. |
| Both sides offer at once (glare) | Acts as the **impolite** peer: `HandleOffer` returns `ErrNegotiationCollision`. | Be the **polite** peer: roll back your offer (`setLocalDescription({type: "rollback"})`) and answer the server's offer. |

ICE candidates can arrive in any order. Candidates that arrive before the remote description are buffered by the server and applied as soon as it is set.

---

## Lifecycle and cleanup

- **`Peer.Close()`** closes the connection. It's safe to call more than once. `Peer.Done()` is closed when that happens, which is handy in `select` loops:

  ```go
  select {
  case <-peer.Done():
      return
  case <-ctx.Done():
      _ = peer.Close()
  }
  ```

- A peer whose connection ends up `failed` or `closed` is closed automatically. Inside a room, it is also removed (as if it had called `Leave`). Its tracks are unpublished and the other members renegotiate.
- **`Room.Leave(id)`** removes one member. **`Room.Close()`** disconnects everyone. **`WebRTC.DeleteRoom(id)`** closes the room *and* removes it from the registry. With `AutoClose`, this happens on its own when the last member leaves.
- **`WebRTC.Close()`** shuts down every room and standalone peer. Call it during graceful shutdown.
- `disconnected` is a temporary state (e.g. network switch) and doesn't remove the peer. It either recovers to `connected` or moves to `failed`.

---

## Errors

| Error | Returned when |
|---|---|
| `ErrRoomExists` | `CreateRoom` with an ID that's already in use. |
| `ErrRoomNotFound` | `GetRoom` / `DeleteRoom` on an unknown ID. |
| `ErrRoomClosed` | `Join` on a room that has been closed. |
| `ErrRoomFull` | `Join` when `MaxPeers` has been reached. |
| `ErrPeerExists` | `Join` / `NewPeer` with an ID that's already in use. |
| `ErrPeerNotFound` | `GetPeer` / `Leave` on an unknown member. |
| `ErrPeerClosed` | Negotiating on a closed peer. |
| `ErrNoSignaler` | `Join` or `Negotiate` without a `Signaler`. |
| `ErrNegotiationInProgress` | `CreateOffer` while an earlier offer is still unanswered. |
| `ErrNegotiationCollision` | `HandleOffer` while the server's own offer is pending (glare). |
| `ErrInvalidSDPType` | An answer passed to `HandleOffer`, or an offer passed to `HandleAnswer`. |
| `ErrDataChannelNotFound` / `ErrDataChannelNotOpen` | `Send`/`SendText` on an unknown or not-yet-open channel. |

All of these are sentinel errors, so check them with `errors.Is`.

---

## Production checklist

- **TURN server.** Without one, clients behind symmetric NATs or strict corporate firewalls can't connect. Run [coturn](https://github.com/coturn/coturn) (or a managed TURN service) and list it in `ICEServers`, preferably with short-lived credentials.
- **Public IP.** On cloud VMs/containers with a private address behind a 1:1 NAT, set `PublicIPs` so the server advertises a reachable address.
- **Port range and firewall.** Set `UDPPortMin`/`UDPPortMax` and open exactly that UDP range (plus your signaling port).
- **Kubernetes.** Media is UDP from the pod itself, so each pod needs a reachable address and port range (host networking, a UDP `LoadBalancer`/`NodePort` per pod, or relay everything through TURN with `ICETransportPolicy: webrtc.ICETransportPolicyRelay`).
- **Sticky routing.** Room state lives in memory in one process. Every member of a room must reach the **same instance**, e.g. by routing on the room ID at the gateway.
- **Thread-safe signalers.** Protect sockets/streams with a mutex, because the library calls the `Signaler` from several goroutines.
- **Shutdown.** Call `rtc.Close()` on SIGTERM.

---

## Limitations

- **Single-node rooms.** Rooms aren't shared across service instances. Use sticky routing, or cascade SFUs yourself.
- **No simulcast/SVC layer selection.** Each published track is forwarded as-is to every subscriber.
- **No ICE restart.** A `failed` connection is closed, and the client should reconnect (join again).
- **No recording or transcoding.** Read the `*webrtc.TrackRemote` in `OnTrack` if you need to process media. Pion's [`pkg/media`](https://pkg.go.dev/github.com/pion/webrtc/v4/pkg/media) has IVF/OGG/H264 writers.
- **Advanced Pion features** (custom interceptors, stats, transceiver tuning) are reachable through `WebRTC.API()` and `Peer.PeerConnection()`.
