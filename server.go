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

type keyHandler struct {
	db DB
}

func (h *keyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	dir, key := path.Split(r.URL.Path)
	if dir != "keys/" || key == "" {
		w.WriteHeader(404)
		fmt.Fprintf(w, "dir(%s) key(%s) not found", dir, key)
		return
	}

	rl := 2
	if r, err := strconv.Atoi(r.URL.Query().Get("rl")); err == nil {
		rl = r
	}

	var err error
	var v *Value

	switch r.Method {
	case "GET":
		v, err = h.db.Get([]byte(key), rl)
	case "PUT":
		incoming := &Value{}
		if err = json.NewDecoder(r.Body).Decode(incoming); err != nil {
			break
		}
		if incoming.Timestamp == 0 {
			incoming.Timestamp = uint64(time.Now().UnixNano())
		}
		err = h.db.Set([]byte(key), incoming, rl)
	default:
		w.WriteHeader(405)
		fmt.Fprintf(w, "%s unsupported", r.Method)
		return
	}

	if err != nil {
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
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("Error writing response: %v", err)
	}
}

type gossipHandler struct {
	db DB
}

func (h *gossipHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		if err := json.NewEncoder(w).Encode(h.db.Gossip()); err != nil {
			w.WriteHeader(500)
			log.Printf("Error writing Gossip... hopefully the client just disappeared? %v", err)
		}
	case "POST":
		s := &State{}
		if err := json.NewDecoder(r.Body).Decode(s); err != nil {
			w.WriteHeader(500)
			log.Printf("Error reading Gossip: %v", err)
			return
		}
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	default:
		w.WriteHeader(405)
		fmt.Fprintf(w, "%s unsupported", r.Method)
	}
}

// Serve db on bind. Blocks until error.
func Serve(bind string, db DB) {
	http.Handle("/keys/", &keyHandler{db})
	http.Handle("/gossip/", &keyHandler{db})
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pong"))
	})
	if err := http.ListenAndServe(bind, nil); err != nil {
		log.Printf("http server exited with: %v", err)
	}
}
