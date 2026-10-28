// Command server-stream shows a server-initiated, one-to-one session over plain HTTP.
//
// The service is the media source (think text-to-speech, a rendered feed or a camera bridge).
// It creates a standalone peer, adds an audio track and a data channel, and returns the offer
// in an HTTP response. The client answers in a second request:
//
//	POST   /sessions       -> creates a peer and returns {id, offer}
//	PATCH  /sessions/{id}  -> applies the client's answer
//	DELETE /sessions/{id}  -> closes the session
//
// DisableTrickle makes the offer carry every ICE candidate, so no signaling socket is needed.
// The example starts the HTTP server and a Pion client in the same process and exits after a
// few seconds of streaming.
//
// Run it with:
//
//	go run ./webrtc/server-stream
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Vigil-HQ/domecore-go/domecore"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

// opusSilence is a valid 20ms Opus frame containing silence. A real service would write
// encoded frames from its TTS engine or media pipeline instead.
var opusSilence = []byte{0xf8, 0xff, 0xfe}

const telemetryLabel = "telemetry"

// sessionResponse is returned by POST /sessions.
type sessionResponse struct {
	ID    string                    `json:"id"`
	Offer webrtc.SessionDescription `json:"offer"`
}

// streamService owns the WebRTC instance and keeps track of active sessions by ID.
type streamService struct {
	rtc      domecore.WebRTC
	mu       sync.Mutex
	sessions map[string]domecore.Peer
}

func (s *streamService) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /sessions", s.create)
	mux.HandleFunc("PATCH /sessions/{id}", s.answer)
	mux.HandleFunc("DELETE /sessions/{id}", s.delete)
	return mux
}

// create builds a peer with an outgoing audio track and data channel, then returns its offer.
func (s *streamService) create(w http.ResponseWriter, r *http.Request) {
	peer, err := s.rtc.NewPeer("", domecore.PeerOptions{
		DisableTrickle: true,
		OnConnectionStateChange: func(peer domecore.Peer, state webrtc.PeerConnectionState) {
			log.Printf("[server] session %s: %s", peer.ID(), state)
		},
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	audio, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", peer.ID(),
	)
	if err == nil {
		_, err = peer.AddTrack(audio) // RTCP from the client is drained by the peer
	}
	if err == nil {
		_, err = peer.CreateDataChannel(telemetryLabel, nil)
	}
	if err != nil {
		_ = peer.Close()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// CreateOffer sets the local description but does not signal it: the HTTP response does.
	offer, err := peer.CreateOffer(r.Context())
	if err != nil {
		_ = peer.Close()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.mu.Lock()
	s.sessions[peer.ID()] = peer
	s.mu.Unlock()

	go s.stream(peer, audio)

	w.Header().Set("Location", "/sessions/"+peer.ID())
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(sessionResponse{ID: peer.ID(), Offer: offer})
}

// answer applies the client's answer to the pending offer.
func (s *streamService) answer(w http.ResponseWriter, r *http.Request) {
	peer, ok := s.lookup(r.PathValue("id"))
	if !ok {
		http.Error(w, domecore.ErrPeerNotFound.Error(), http.StatusNotFound)
		return
	}

	var answer webrtc.SessionDescription
	if err := json.NewDecoder(r.Body).Decode(&answer); err != nil {
		http.Error(w, "invalid answer", http.StatusBadRequest)
		return
	}
	if err := peer.HandleAnswer(answer); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, domecore.ErrInvalidSDPType) {
			status = http.StatusBadRequest
		}
		http.Error(w, err.Error(), status)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// delete closes the session. Standalone peers leave the WebRTC registry on their own once closed.
func (s *streamService) delete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	peer, ok := s.lookup(id)
	if !ok {
		http.Error(w, domecore.ErrPeerNotFound.Error(), http.StatusNotFound)
		return
	}

	s.mu.Lock()
	delete(s.sessions, id)
	s.mu.Unlock()

	_ = peer.Close()
	w.WriteHeader(http.StatusNoContent)
}

func (s *streamService) lookup(id string) (domecore.Peer, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	peer, ok := s.sessions[id]
	return peer, ok
}

// stream writes audio frames and a telemetry message once a second until the peer is closed.
// Writes before the connection is up are simply dropped, so no readiness tracking is needed.
func (s *streamService) stream(peer domecore.Peer, audio *webrtc.TrackLocalStaticSample) {
	frames := time.NewTicker(20 * time.Millisecond)
	defer frames.Stop()
	telemetry := time.NewTicker(time.Second)
	defer telemetry.Stop()

	sent := 0
	for {
		select {
		case <-peer.Done():
			log.Printf("[server] session %s ended after %d frames", peer.ID(), sent)
			return
		case <-frames.C:
			if err := audio.WriteSample(media.Sample{Data: opusSilence, Duration: 20 * time.Millisecond}); err == nil {
				sent++
			}
		case now := <-telemetry.C:
			// ErrDataChannelNotOpen is expected until the client has connected.
			err := peer.SendText(telemetryLabel, fmt.Sprintf(`{"frames":%d,"at":%q}`, sent, now.Format(time.RFC3339)))
			if err != nil && !errors.Is(err, domecore.ErrDataChannelNotOpen) {
				log.Printf("[server] telemetry: %v", err)
			}
		}
	}
}

func main() {
	rtc, err := domecore.NewWebRTC(domecore.WebRTCConfig{}, nil)
	if err != nil {
		log.Fatalf("create webrtc: %v", err)
	}
	defer rtc.Close()

	service := &streamService{rtc: rtc, sessions: make(map[string]domecore.Peer)}
	server := httptest.NewServer(service.routes())
	defer server.Close()
	log.Printf("[server] listening on %s", server.URL)

	if err := runClient(server.URL, 4*time.Second); err != nil {
		log.Fatalf("[client] %v", err)
	}
}

// runClient plays the role of the browser or device receiving the stream.
func runClient(baseURL string, duration time.Duration) error {
	// 1. Ask the service for a session and its offer.
	res, err := http.Post(baseURL+"/sessions", "application/json", nil)
	if err != nil {
		return err
	}
	var session sessionResponse
	err = json.NewDecoder(res.Body).Decode(&session)
	_ = res.Body.Close()
	if err != nil {
		return fmt.Errorf("decode session: %w", err)
	}
	log.Printf("[client] got session %s", session.ID)

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return err
	}
	defer pc.Close()

	var packets atomic.Int64
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		log.Printf("[client] receiving %s track (%s)", track.Kind(), track.Codec().MimeType)
		for {
			if _, _, err := track.ReadRTP(); err != nil {
				return
			}
			packets.Add(1)
		}
	})
	pc.OnDataChannel(func(channel *webrtc.DataChannel) {
		channel.OnMessage(func(message webrtc.DataChannelMessage) {
			log.Printf("[client] %s: %s", channel.Label(), message.Data)
		})
	})

	// 2. Answer the offer. The client waits for ICE gathering too, so the answer is complete.
	if err := pc.SetRemoteDescription(session.Offer); err != nil {
		return fmt.Errorf("set offer: %w", err)
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return err
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		return err
	}
	<-gathered

	// 3. Send the answer back to the service.
	body, _ := json.Marshal(pc.LocalDescription())
	if err := request(http.MethodPatch, baseURL+"/sessions/"+session.ID, body); err != nil {
		return fmt.Errorf("send answer: %w", err)
	}

	time.Sleep(duration)
	log.Printf("[client] received %d RTP packets", packets.Load())

	// 4. End the session.
	return request(http.MethodDelete, baseURL+"/sessions/"+session.ID, nil)
}

func request(method, url string, body []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return fmt.Errorf("%s %s: %s", method, url, res.Status)
	}
	return nil
}
