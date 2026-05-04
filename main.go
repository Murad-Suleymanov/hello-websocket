package main

import (
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Message struct {
	Source      string    `json:"source"`
	Timestamp   time.Time `json:"timestamp"`
	Temperature float64   `json:"temperature"`
	Counter     int64     `json:"counter"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var (
	activeConns = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "websocket_active_connections",
		Help: "Active WebSocket connections",
	})
	msgSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "websocket_messages_sent_total",
		Help: "Total WebSocket messages sent",
	})
)

var counter int64

type session struct {
	outbox chan string
	cancel chan struct{}
}

type sessionRegistry struct {
	mu       sync.RWMutex
	sessions map[string]*session
}

func (r *sessionRegistry) add(id string, s *session) {
	r.mu.Lock()
	r.sessions[id] = s
	r.mu.Unlock()
}

func (r *sessionRegistry) remove(id string) {
	r.mu.Lock()
	delete(r.sessions, id)
	r.mu.Unlock()
}

func (r *sessionRegistry) send(id, msg string) bool {
	r.mu.RLock()
	s, ok := r.sessions[id]
	r.mu.RUnlock()
	if !ok {
		return false
	}
	select {
	case s.outbox <- msg:
		return true
	case <-time.After(time.Second):
		return false
	}
}

func (r *sessionRegistry) cancel(id string) bool {
	r.mu.Lock()
	s, ok := r.sessions[id]
	if ok {
		delete(r.sessions, id)
	}
	r.mu.Unlock()
	if !ok {
		return false
	}
	close(s.cancel)
	return true
}

func (r *sessionRegistry) ids() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.sessions))
	for id := range r.sessions {
		ids = append(ids, id)
	}
	return ids
}

var registry = &sessionRegistry{sessions: make(map[string]*session)}

func newSessionID() string {
	var b [16]byte
	_, _ = crand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade error: %v", err)
		return
	}
	defer conn.Close()

	activeConns.Inc()
	defer activeConns.Dec()

	id := newSessionID()
	s := &session{
		outbox: make(chan string, 64),
		cancel: make(chan struct{}),
	}
	registry.add(id, s)
	defer registry.remove(id)

	log.Printf("client connected: %s session=%s", r.RemoteAddr, id)

	if err := conn.WriteMessage(websocket.TextMessage, []byte(id)); err != nil {
		log.Printf("write session id error: %v", err)
		return
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			log.Printf("client disconnected: %s session=%s", r.RemoteAddr, id)
			return
		case <-s.cancel:
			log.Printf("session cancelled: %s", id)
			_ = conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, "cancelled"),
				time.Now().Add(time.Second))
			return
		case msg := <-s.outbox:
			if err := conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
				log.Printf("write outbox error: %v", err)
				return
			}
			msgSent.Inc()
		case t := <-ticker.C:
			payload := Message{
				Source:      "hello-websocket",
				Timestamp:   t.UTC(),
				Temperature: 20 + rand.Float64()*15,
				Counter:     atomic.AddInt64(&counter, 1),
			}
			data, err := json.Marshal(payload)
			if err != nil {
				log.Printf("marshal error: %v", err)
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				log.Printf("write tick error: %v", err)
				return
			}
			msgSent.Inc()
		}
	}
}

func sessionsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(registry.ids())
}

func sendHandler(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	msg := string(body)
	var unquoted string
	if json.Unmarshal(body, &unquoted) == nil {
		msg = unquoted
	}

	if !registry.send(id, msg) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"Session tapılmadı"}`))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func cancelHandler(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !registry.cancel(id) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"Session tapılmadı"}`))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wsHandler)
	mux.HandleFunc("GET /sessions", sessionsHandler)
	mux.HandleFunc("POST /sessions/{id}/send", sendHandler)
	mux.HandleFunc("DELETE /sessions/{id}", cancelHandler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"healthy"}`))
	})
	mux.Handle("/metrics", promhttp.Handler())

	log.Println("hello-websocket running on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
