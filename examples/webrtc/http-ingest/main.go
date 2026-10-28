// Command http-ingest is a WHIP-style ingest endpoint: a browser sends its camera to the
// service in a single HTTP request/response, with no signaling socket.
//
// The browser POSTs a complete offer (it waits for ICE gathering), the service answers with a
// standalone peer created with DisableTrickle, and the media is consumed in OnTrack. A data
// channel opened by the browser is echoed back to show OnMessage and Peer.SendText.
//
//	POST   /ingest       -> body: offer, response: answer + Location header
//	DELETE /ingest/{id}  -> closes the session
//
// Run it with:
//
//	go run ./webrtc/http-ingest
//
// then open http://localhost:8081.
package main

import (
	"embed"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/Vigil-HQ/domecore-go/domecore"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

//go:embed index.html
var static embed.FS

type ingestService struct {
	rtc      domecore.WebRTC
	mu       sync.Mutex
	sessions map[string]domecore.Peer
}

// ingest applies the browser's offer to a new standalone peer and returns the answer.
func (s *ingestService) ingest(w http.ResponseWriter, r *http.Request) {
	var offer webrtc.SessionDescription
	if err := json.NewDecoder(r.Body).Decode(&offer); err != nil {
		http.Error(w, "invalid offer", http.StatusBadRequest)
		return
	}

	peer, err := s.rtc.NewPeer("", domecore.PeerOptions{
		// The answer waits for ICE gathering, so it already holds every candidate.
		DisableTrickle: true,
		OnTrack: func(peer domecore.Peer, track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			go consume(peer, track)
		},
		OnMessage: func(peer domecore.Peer, label string, message webrtc.DataChannelMessage) {
			log.Printf("[%s] %s: %s", peer.ID(), label, message.Data)
			_ = peer.SendText(label, "echo: "+string(message.Data))
		},
		OnConnectionStateChange: func(peer domecore.Peer, state webrtc.PeerConnectionState) {
			log.Printf("[%s] connection: %s", peer.ID(), state)
			if state == webrtc.PeerConnectionStateClosed || state == webrtc.PeerConnectionStateFailed {
				s.forget(peer.ID())
			}
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

	s.mu.Lock()
	s.sessions[peer.ID()] = peer
	s.mu.Unlock()

	w.Header().Set("Location", "/ingest/"+peer.ID())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(answer)
}

// stop ends a session on request.
func (s *ingestService) stop(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	peer, ok := s.sessions[r.PathValue("id")]
	s.mu.Unlock()
	if !ok {
		http.Error(w, domecore.ErrPeerNotFound.Error(), http.StatusNotFound)
		return
	}
	_ = peer.Close()
	w.WriteHeader(http.StatusNoContent)
}

func (s *ingestService) forget(id string) {
	s.mu.Lock()
	delete(s.sessions, id)
	s.mu.Unlock()
}

// consume reads RTP from an incoming track and logs throughput every few seconds.
// This is where a real service would decode, record or run inference on the media.
// For video, it asks for a keyframe on start so a decoder could begin immediately.
func consume(peer domecore.Peer, track *webrtc.TrackRemote) {
	log.Printf("[%s] receiving %s (%s)", peer.ID(), track.Kind(), track.Codec().MimeType)

	if track.Kind() == webrtc.RTPCodecTypeVideo {
		_ = peer.PeerConnection().WriteRTCP([]rtcp.Packet{
			&rtcp.PictureLossIndication{MediaSSRC: uint32(track.SSRC())},
		})
	}

	var packets, bytes int
	report := time.NewTicker(5 * time.Second)
	defer report.Stop()
	for {
		packet, _, err := track.ReadRTP()
		if err != nil {
			log.Printf("[%s] %s track ended", peer.ID(), track.Kind())
			return
		}
		packets++
		bytes += len(packet.Payload)

		select {
		case <-report.C:
			log.Printf("[%s] %s: %d packets, %d KiB", peer.ID(), track.Kind(), packets, bytes/1024)
		default:
		}
	}
}

func main() {
	addr := flag.String("addr", ":8081", "HTTP listen address")
	flag.Parse()

	rtc, err := domecore.NewWebRTC(domecore.WebRTCConfig{}, nil)
	if err != nil {
		log.Fatalf("create webrtc: %v", err)
	}
	defer rtc.Close()

	service := &ingestService{rtc: rtc, sessions: make(map[string]domecore.Peer)}

	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServerFS(static))
	mux.HandleFunc("GET /config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"iceServers": rtc.ICEServers()})
	})
	mux.HandleFunc("POST /ingest", service.ingest)
	mux.HandleFunc("DELETE /ingest/{id}", service.stop)

	log.Printf("open http://localhost%s", *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
