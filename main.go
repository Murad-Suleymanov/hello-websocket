package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
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

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade error: %v", err)
		return
	}
	defer conn.Close()

	activeConns.Inc()
	defer activeConns.Dec()

	log.Printf("client connected: %s", r.RemoteAddr)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-done:
			log.Printf("client disconnected: %s", r.RemoteAddr)
			return
		case t := <-ticker.C:
			msg := Message{
				Source:      "hello-websocket",
				Timestamp:   t.UTC(),
				Temperature: 20 + rand.Float64()*15,
				Counter:     atomic.AddInt64(&counter, 1),
			}
			data, err := json.Marshal(msg)
			if err != nil {
				log.Printf("marshal error: %v", err)
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				log.Printf("write error: %v", err)
				return
			}
			msgSent.Inc()
		}
	}
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wsHandler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"healthy"}`))
	})
	mux.Handle("/metrics", promhttp.Handler())

	log.Println("hello-websocket running on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
