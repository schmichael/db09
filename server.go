package db09

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path"
	"strconv"
	"time"
)

type server struct {
	db *MemDB
}

func (s server) KeyHandler(w http.ResponseWriter, r *http.Request) {
	dir, key := path.Split(r.URL.Path)
	if dir != "/keys/" || key == "" {
		w.WriteHeader(404)
		fmt.Fprintf(w, "dir(%s) key(%s) not found", dir, key)
		log.Printf("Returning 404: %s %s", r.Method, r.URL)
		return
	}

	// Default to quorum
	rl := s.db.rf>>1 + 1
	if r, err := strconv.Atoi(r.URL.Query().Get("rl")); err == nil {
		rl = r
	}

	var err error
	var v *Value

	switch r.Method {
	case "GET":
		v, err = s.db.Get(key, rl)
	case "PUT", "POST": //FIXME POST only supported out of laziness
		incoming := Value{}
		if err = json.NewDecoder(r.Body).Decode(&incoming); err != nil {
			break
		}
		if incoming.Timestamp == 0 {
			incoming.Timestamp = uint64(time.Now().UnixNano())
		}
		err = s.db.Set(key, &incoming, rl)
		log.Printf("%s %s %q (err? %v)", r.Method, r.URL, incoming.V, err)
	case "DELETE":
		v := Value{Deleted: true}
		if ts, err := strconv.ParseUint(r.URL.Query().Get("timestamp"), 10, 64); err == nil {
			v.Timestamp = ts
		} else {
			v.Timestamp = uint64(time.Now().UnixNano())
		}
		err = s.db.Set(key, &v, rl)
		log.Printf("%s %s %d (err? %v)", r.Method, r.URL, v.Timestamp, err)
	default:
		w.WriteHeader(405)
		fmt.Fprintf(w, "%s unsupported", r.Method)
		return
	}

	if err != nil {
		log.Printf("%s %s Error: %v", r.Method, r.URL, err)
		if err == NotFound {
			w.WriteHeader(404)
			w.Write([]byte("not found"))
		} else {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
		}
		return
	}

	w.WriteHeader(200)
	if v == nil {
		w.Write([]byte("ok"))
		return
	}
	w.Header().Add("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(&v); err != nil {
		log.Printf("Error writing response: %v", err)
	}
}

func (s server) GossipHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		w.Header().Add("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(s.db.Gossip()); err != nil {
			w.WriteHeader(500)
			log.Printf("Error writing Gossip... hopefully the client just disappeared? %v", err)
		}
	case "POST":
		state := &State{}
		if err := json.NewDecoder(r.Body).Decode(state); err != nil {
			w.WriteHeader(500)
			log.Printf("Error reading Gossip: %v", err)
			return
		}
		s.db.GossipUpdate(state)
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	default:
		w.WriteHeader(405)
		fmt.Fprintf(w, "%s unsupported", r.Method)
	}
}

func (s server) VersionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(405)
		fmt.Fprintf(w, "%s unsupported", r.Method)
		return
	}
	fmt.Fprintf(w, "%d", s.db.Version())
}

func (s server) NodeStatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(405)
		fmt.Fprintf(w, "%s unsupported", r.Method)
		return
	}
	w.Header().Add("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(s.db.NodeStatus()); err != nil {
		log.Printf("Error encoding node status: %v", err)
	}
}

// Serve db on bind. Blocks until error.
func Serve(bind string, db *MemDB) {
	s := server{db}
	http.HandleFunc("/keys/", s.KeyHandler)
	http.HandleFunc("/gossip", s.GossipHandler)
	http.HandleFunc("/gossip/version", s.VersionHandler)
	http.HandleFunc("/status/node", s.NodeStatusHandler)
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pong"))
	})
	if err := http.ListenAndServe(bind, nil); err != nil {
		log.Printf("http server exited with: %v", err)
	}
}
