package db09

import (
	"fmt"
	"log"
	"net/http"
	"path"
)

type dbHandler struct {
	db DB
}

func (h *dbHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	dir, key := path.Split(r.URL.Path)
	if dir != "keys/" || key == "" {
		w.WriteHeader(404)
		fmt.Fprintf(w, "dir(%s) key(%s) not found", dir, key)
		return
	}

	var err error
	var value string

	switch r.Method {
	case "GET":
		err, value = h.get(key)
	case "PUT":
		v := r.Form.Get("value")
		if v == "" {
			w.WriteHeader(400)
			w.Write([]byte("missing 'value' form value"))
			return
		}
		err = h.set(key, v)
	case "DELETE":
		w.WriteHeader(500)
		w.Write([]byte("todo"))
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
	w.Write([]byte(value))
}

func (h *dbHandler) get(key string) (error, string) {
	panic("TODO")
	return nil, ""
}

func (h *dbHandler) set(key string, value string) error {
	panic("TODO")
	return nil
}

// Serve db on bind. Blocks until error.
func Serve(bind string, db DB) {
	http.Handle("/keys/", &dbHandler{db})
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pong"))
	})
	if err := http.ListenAndServe(bind, nil); err != nil {
		log.Printf("http server exited with: %v", err)
	}
}
