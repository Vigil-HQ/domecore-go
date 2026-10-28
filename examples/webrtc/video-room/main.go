// Command video-room is a multi-party video call: a domecore Room acting as an SFU, with
// signaling over WebSocket and a small browser client.
//
// Every participant's camera and microphone are forwarded to every other participant, and the
// room data channel is used as a text chat (RelayMessages forwards each message to the others).
//
// Run it with:
//
//	go run ./webrtc/video-room
//
// then open http://localhost:8080/?room=standup in two or more browser tabs.
// Flags let you change the listen address and the advertised public IP (see -h).
package main

import (
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Vigil-HQ/domecore-go/domecore"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
)

//go:embed index.html
var static embed.FS

// SignalMessage is the JSON envelope exchanged with the browser.
type SignalMessage struct {
	Type      string                     `json:"type"` // "offer" | "answer" | "candidate"
	SDP       *webrtc.SessionDescription `json:"sdp,omitempty"`
	Candidate *webrtc.ICECandidateInit   `json:"candidate,omitempty"`
}

// wsSignaler implements domecore.Signaler on top of one WebSocket connection.
// Gorilla allows a single concurrent writer and the signaler is called from Pion goroutines,
// so writes are serialised with a mutex.
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

var upgrader = websocket.Upgrader{
	// Accept any origin for the demo. Check the origin in production.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// roomHandler upgrades the request, joins the requested room and pumps the browser's
// answers and ICE candidates into the server-side peer until the socket closes.
func roomHandler(rtc domecore.WebRTC) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		roomID := r.URL.Query().Get("room")
		name := r.URL.Query().Get("name")
		if roomID == "" {
			http.Error(w, "room is required", http.StatusBadRequest)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		room, _ := rtc.GetOrCreateRoom(roomID, domecore.RoomOptions{
			MaxPeers:      8,
			AutoClose:     true,
			RelayMessages: true,
			OnPeerJoined: func(room domecore.Room, peer domecore.Peer) {
				log.Printf("%s (%s) joined %s, %d member(s)", peer.Metadata()["name"], peer.ID(), room.ID(), len(room.Peers()))
				_ = room.BroadcastText(fmt.Sprintf("* %s joined", peer.Metadata()["name"]), peer.ID())
			},
			OnPeerLeft: func(room domecore.Room, peer domecore.Peer) {
				log.Printf("%s (%s) left %s", peer.Metadata()["name"], peer.ID(), room.ID())
				_ = room.BroadcastText(fmt.Sprintf("* %s left", peer.Metadata()["name"]))
			},
		})

		// The peer ID becomes the stream ID of every track this participant publishes,
		// so browsers can map incoming streams to participants.
		peer, err := room.Join(uuid.NewString(), domecore.PeerOptions{
			Signaler: &wsSignaler{conn: conn},
			Metadata: map[string]string{"name": name},
			OnTrack: func(peer domecore.Peer, track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
				log.Printf("%s is publishing %s (%s)", peer.Metadata()["name"], track.Kind(), track.Codec().MimeType)
			},
		})
		if err != nil {
			// The upgrade already happened, so report the error over the socket.
			_ = conn.WriteJSON(map[string]string{"type": "error", "error": err.Error()})
			return
		}
		defer room.Leave(peer.ID())

		for {
			var message SignalMessage
			if err := conn.ReadJSON(&message); err != nil {
				return // socket closed -> deferred Leave renegotiates with the remaining members
			}
			switch {
			case message.Type == "answer" && message.SDP != nil:
				err = peer.HandleAnswer(*message.SDP)
			case message.Type == "candidate" && message.Candidate != nil:
				err = peer.AddICECandidate(*message.Candidate)
			}
			if err != nil {
				log.Printf("signal error for %s: %v", peer.ID(), err)
			}
		}
	}
}

// configHandler hands the browser the same ICE servers the service uses.
func configHandler(rtc domecore.WebRTC) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"iceServers": rtc.ICEServers()})
	}
}

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	publicIP := flag.String("public-ip", "", "public IP to advertise when running behind a 1:1 NAT")
	flag.Parse()

	config := domecore.WebRTCConfig{
		ICEServers: []webrtc.ICEServer{{URLs: []string{domecore.DefaultSTUNServer}}},
		// A fixed UDP range is what you would open in a firewall or security group.
		UDPPortMin: 50000,
		UDPPortMax: 50100,
	}
	if *publicIP != "" {
		config.PublicIPs = []string{*publicIP}
	}

	rtc, err := domecore.NewWebRTC(config, nil)
	if err != nil {
		log.Fatalf("create webrtc: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServerFS(static))
	mux.HandleFunc("GET /config", configHandler(rtc))
	mux.HandleFunc("GET /ws", roomHandler(rtc))

	server := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		log.Printf("open http://localhost%s/?room=standup in several tabs", *addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	// Graceful shutdown: close every room and peer on SIGINT/SIGTERM.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	_ = server.Close()
	_ = rtc.Close()
}
