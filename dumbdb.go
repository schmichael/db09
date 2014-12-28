package dumbdb

import (
	"errors"
	"sync"
)

var (
	TooManyReplicas = errors.New("desired replicas exceeds cluster replication level")
	EmptyKey        = errors.New("key wasn't specified")
)

type Client struct {
	addr string
}

type Value struct {
	Version uint64
	V       []byte
}

type State struct {
	Version uint64
	Ring    [256][256]string
}

type DB interface {
	RecvGossip() *State
	SendGossip(*State)
	Get(key []byte, replicas int) (*Value, error)
}

type MemDB struct {
	rl int // replication level

	ringL sync.Mutex
	ring  [256][256]DB // 256 * 256 * 16 = 1MB

	dbL sync.Mutex
	db  map[string]*Value
}

func NewMemDB(rl int, seeds []DB) *MemDB {
	d := &MemDB{
		rl: rl,
		db: make(map[string]*Value),
	}

	// Gossip to get initial state
	ver := 0
	for _, s := range seeds {
		//TODO do concurrently
		state := s.RecvGossip()
		if state.Version > ver {
			for x := range state.Ring {
				for y, addr := range state.Ring[x] {
					//TODO Reuse clients
					d.ring[x][y] = &Client{addr: addr}
				}
			}
		}
	}
	if ver == 0 {
		// No ring, claim it all
		for x := range d.Ring {
			for y := range d.Ring[x] {
				d.ring[x][y] = d
			}
		}
		ver++
		return d
	}

	// Claim part of the ring
	for x := range state.Ring {
		for y := range state.Ring[x] {
			peers := make(map[string]struct{}, rl)
			//TODO This falls apart for keys > 256-rl
			for i := 0; i < rl; i++ {
				peers[state.Ring[x][y+i]] = struct{}{}
			}
		}
	}
	return d
}

func (d *MemDB) Get(key []byte, replicas int) (*Value, error) {
	if replicas > d.rl {
		return nil, TooManyReplicas
	}
	if len(key) == 0 {
		return nil, EmptyKey
	}

	x := key[0]
	var y byte
	if len(key) > 1 {
		y = key[1]
	}

	// Only forward Get if needed
	if replicas > 0 {
		d.ringL.Lock()
		for i := d.rl; i > 0; i-- {
			d.ringk
		}
		ringL.Unlock()
	}
}

func (d *MemDB) Gossip(incoming *State) *State {
}
