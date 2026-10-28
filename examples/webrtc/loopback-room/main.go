// Command loopback-room shows the full room lifecycle without a browser.
//
// Two Pion clients ("alice" and "bob") join a data-only room hosted by domecore.WebRTC in the
// same process. Signaling travels over Go channels, which stand in for a WebSocket or NATS
// connection, so the example shows exactly what a real transport has to carry:
//
//   - server -> client: offers and ICE candidates, delivered through a domecore.Signaler
//   - client -> server: answers and ICE candidates, applied with Peer.HandleAnswer / Peer.AddICECandidate
//
// The room relays every message on the room data channel to the other members, and the server
// also broadcasts its own presence messages.
//
// Run it with:
//
//	go run ./webrtc/loopback-room
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Vigil-HQ/domecore-go/domecore"
	"github.com/pion/webrtc/v4"
)

// signal is the envelope exchanged between a client and the server.
// In a real service this would be the JSON body of a WebSocket or NATS message.
type signal struct {
	SDP       *webrtc.SessionDescription
	Candidate *webrtc.ICECandidateInit
}

// connection is the in-memory stand-in for one client's signaling socket.
type connection struct {
	toClient chan signal
	toServer chan signal
}

func newConnection() *connection {
	return &connection{toClient: make(chan signal, 64), toServer: make(chan signal, 64)}
}

func main() {
	rtc, err := domecore.NewWebRTC(domecore.WebRTCConfig{}, nil)
	if err != nil {
		log.Fatalf("create webrtc: %v", err)
	}
	defer rtc.Close()

	room, err := rtc.CreateRoom("loopback", domecore.RoomOptions{
		DataOnly:      true, // no audio/video transceivers, only the room data channel
		RelayMessages: true, // forward every member message to the other members
		AutoClose:     true, // remove the room once the last member leaves
		OnPeerJoined: func(room domecore.Room, peer domecore.Peer) {
			log.Printf("[server] %s (%s) joined %s", peer.ID(), peer.Metadata()["name"], room.ID())
		},
		OnPeerLeft: func(room domecore.Room, peer domecore.Peer) {
			log.Printf("[server] %s left %s", peer.ID(), room.ID())
			_ = room.BroadcastText(fmt.Sprintf(`{"type":"left","user":%q}`, peer.ID()))
		},
		OnMessage: func(room domecore.Room, from domecore.Peer, message webrtc.DataChannelMessage) {
			log.Printf("[server] saw message from %s: %s", from.ID(), message.Data)
		},
	})
	if err != nil {
		log.Fatalf("create room: %v", err)
	}

	alice := newClient("alice")
	bob := newClient("bob")
	defer alice.close()
	defer bob.close()

	for _, c := range []*client{alice, bob} {
		if err := join(room, c); err != nil {
			log.Fatalf("join %s: %v", c.id, err)
		}
	}

	// Wait until both clients have the room data channel open, then talk.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, c := range []*client{alice, bob} {
		if err := c.waitOpen(ctx); err != nil {
			log.Fatalf("%s data channel: %v", c.id, err)
		}
	}

	// Server-side broadcast reaches every member.
	if err := room.BroadcastText(`{"type":"announcement","text":"welcome to the loopback room"}`); err != nil {
		log.Printf("[server] broadcast: %v", err)
	}

	// Member messages are relayed by the room because RelayMessages is enabled.
	alice.send("hello from alice")
	bob.send("hi alice, bob here")

	// A message for a single member goes straight to its peer.
	if peer, err := room.GetPeer("bob"); err == nil {
		_ = peer.SendText(domecore.RoomDataChannelLabel, `{"type":"direct","text":"just for bob"}`)
	}

	if err := alice.waitFor(ctx, "hi alice, bob here"); err != nil {
		log.Fatalf("alice did not receive bob's message: %v", err)
	}
	if err := bob.waitFor(ctx, "hello from alice"); err != nil {
		log.Fatalf("bob did not receive alice's message: %v", err)
	}

	log.Printf("[server] members: %d, rooms: %d", len(room.Peers()), len(rtc.Rooms()))

	// Leaving closes the server-side peer. With AutoClose the room disappears with the last member.
	_ = room.Leave("alice")
	_ = room.Leave("bob")

	if _, err := rtc.GetRoom("loopback"); errors.Is(err, domecore.ErrRoomNotFound) {
		log.Printf("[server] room was closed automatically after the last member left")
	}
}

// join is what a signaling handler does for each new connection: join the room with a Signaler
// bound to the connection, then pump the client's answers and candidates into the peer.
func join(room domecore.Room, c *client) error {
	signaler := domecore.SignalerFuncs{
		OfferFunc: func(peerID string, offer webrtc.SessionDescription) error {
			c.conn.toClient <- signal{SDP: &offer}
			return nil
		},
		ICECandidateFunc: func(peerID string, candidate webrtc.ICECandidateInit) error {
			c.conn.toClient <- signal{Candidate: &candidate}
			return nil
		},
	}

	// Join sends the first offer through the signaler before it returns.
	peer, err := room.Join(c.id, domecore.PeerOptions{
		Signaler: signaler,
		Metadata: map[string]string{"name": c.id},
		OnConnectionStateChange: func(peer domecore.Peer, state webrtc.PeerConnectionState) {
			log.Printf("[server] %s connection: %s", peer.ID(), state)
		},
	})
	if err != nil {
		return err
	}

	// The read loop: everything the client sends goes to the server-side peer.
	go func() {
		for {
			select {
			case <-peer.Done():
				return
			case message := <-c.conn.toServer:
				var err error
				switch {
				case message.SDP != nil:
					err = peer.HandleAnswer(*message.SDP)
				case message.Candidate != nil:
					err = peer.AddICECandidate(*message.Candidate)
				}
				if err != nil {
					log.Printf("[server] signal error for %s: %v", peer.ID(), err)
				}
			}
		}
	}()
	return nil
}

// client is a minimal room member, written the way a browser client would be:
// it answers every offer, trickles its candidates and listens on the room data channel.
type client struct {
	id   string
	conn *connection
	pc   *webrtc.PeerConnection

	mu       sync.Mutex
	channel  *webrtc.DataChannel
	opened   chan struct{}
	received chan string
}

func newClient(id string) *client {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		log.Fatalf("client %s: %v", id, err)
	}

	c := &client{
		id:       id,
		conn:     newConnection(),
		pc:       pc,
		opened:   make(chan struct{}),
		received: make(chan string, 64),
	}

	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate != nil {
			init := candidate.ToJSON()
			c.conn.toServer <- signal{Candidate: &init}
		}
	})

	// The server creates the room data channel; clients receive it here.
	pc.OnDataChannel(func(channel *webrtc.DataChannel) {
		if channel.Label() != domecore.RoomDataChannelLabel {
			return
		}
		channel.OnOpen(func() {
			c.mu.Lock()
			c.channel = channel
			c.mu.Unlock()
			close(c.opened)
		})
		channel.OnMessage(func(message webrtc.DataChannelMessage) {
			log.Printf("[%s] received: %s", c.id, message.Data)
			c.received <- string(message.Data)
		})
	})

	go c.readLoop()
	return c
}

// readLoop answers server offers. Candidates can arrive before the offer they belong to,
// so they are buffered until a remote description is set.
func (c *client) readLoop() {
	var pending []webrtc.ICECandidateInit
	for message := range c.conn.toClient {
		switch {
		case message.SDP != nil:
			if err := c.pc.SetRemoteDescription(*message.SDP); err != nil {
				log.Printf("[%s] set offer: %v", c.id, err)
				continue
			}
			for _, candidate := range pending {
				_ = c.pc.AddICECandidate(candidate)
			}
			pending = nil

			answer, err := c.pc.CreateAnswer(nil)
			if err != nil {
				log.Printf("[%s] create answer: %v", c.id, err)
				continue
			}
			if err := c.pc.SetLocalDescription(answer); err != nil {
				log.Printf("[%s] set answer: %v", c.id, err)
				continue
			}
			c.conn.toServer <- signal{SDP: c.pc.LocalDescription()}

		case message.Candidate != nil:
			if c.pc.RemoteDescription() == nil {
				pending = append(pending, *message.Candidate)
				continue
			}
			_ = c.pc.AddICECandidate(*message.Candidate)
		}
	}
}

func (c *client) waitOpen(ctx context.Context) error {
	select {
	case <-c.opened:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *client) send(text string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.channel.SendText(text); err != nil {
		log.Printf("[%s] send: %v", c.id, err)
	}
}

func (c *client) waitFor(ctx context.Context, text string) error {
	for {
		select {
		case message := <-c.received:
			if message == text {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *client) close() {
	_ = c.pc.Close()
}
