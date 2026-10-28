# domecore WebRTC examples

Runnable examples for the `domecore.WebRTC`, `Room`, `Peer` and `Signaler` interfaces.
See [`WEBRTC.md`](../../WEBRTC.md) for the full API guide.

The examples live in their own Go module (`examples/go.mod`), so their dependencies
(e.g. `gorilla/websocket`) don't get added to the library. The module points at the local
checkout through a `replace` directive. Run every command from the `examples/` directory:

```bash
cd examples
```

| Example | Session model | Signaling | Needs a browser | Shows |
|---|---|---|---|---|
| [`loopback-room`](loopback-room/main.go) | Room (data-only) | In-memory channels | No | `CreateRoom`, `Join`, `SignalerFuncs`, `HandleAnswer`, `AddICECandidate`, `RelayMessages`, `Broadcast`, `GetPeer`/`SendText`, `Leave`, `AutoClose` |
| [`server-stream`](server-stream/main.go) | Standalone peer, server offers | HTTP (non-trickle) | No | `NewPeer`, `AddTrack`, `CreateDataChannel`, `CreateOffer`, `HandleAnswer`, `Done`, `Close` |
| [`video-room`](video-room/main.go) | Room (SFU, audio/video) | WebSocket (trickle) | Yes | A custom `Signaler`, `GetOrCreateRoom`, `MaxPeers`, `OnPeerJoined`/`OnPeerLeft`, `ICEServers`, port range and public IP config, graceful shutdown |
| [`http-ingest`](http-ingest/main.go) | Standalone peer, client offers | HTTP (WHIP-style) | Yes | `NewPeer` with `DisableTrickle`, `HandleOffer`, `OnTrack`, `OnMessage`, `PeerConnection()` for RTCP |

## loopback-room

Two Pion clients join a data-only room in the same process and talk over the room data
channel. Go channels stand in for the network, which makes it easy to see exactly what a
real transport has to carry in each direction. The run checks itself and exits.

```bash
go run ./webrtc/loopback-room
```

## server-stream

The service is the media source. `POST /sessions` creates a peer with an Opus track and a
`telemetry` data channel, then returns the offer from `CreateOffer`. The client sends its
answer with `PATCH /sessions/{id}`, and `DELETE /sessions/{id}` ends the session. A Go client
in the same process receives the audio for a few seconds, prints the telemetry and exits.

```bash
go run ./webrtc/server-stream
```

## video-room

A multi-party video call with text chat. Open the page in two or more tabs (or on two
devices) using the same `room` value:

```bash
go run ./webrtc/video-room            # -addr :8080 -public-ip 203.0.113.10
open "http://localhost:8080/?room=standup&name=alice"
open "http://localhost:8080/?room=standup&name=bob"
```

The server always sends the offers. The browser only answers, adding its camera and microphone
after the first offer so they reuse the server's receive-only transceivers. Each remote
`MediaStream.id` is the publisher's peer ID.

## http-ingest

The browser sends its camera to the service in one HTTP request/response, WHIP-style. The
server logs RTP throughput per track and echoes anything sent on the `control` data channel.

```bash
go run ./webrtc/http-ingest           # -addr :8081
open http://localhost:8081
```

## Notes

- Browsers only allow `getUserMedia` on `localhost` or HTTPS. To test from another device, put
  the server behind TLS (for example, a reverse proxy or a tunnel).
- The browser examples use a public STUN server. On networks where direct connectivity fails,
  add a TURN server to `WebRTCConfig.ICEServers` (see the production checklist in `WEBRTC.md`).
- `video-room` uses UDP ports `50000-50100` for media. Open that range if a firewall is in the way.
