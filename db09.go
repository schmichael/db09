package db09

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"sync"
)

const Tokens = 1 << 16

var (
	TooManyReplicas = errors.New("desired replicas exceeds cluster replication level")
	EmptyKey        = errors.New("key wasn't specified")
	NotFound        = errors.New("key not found")
	StaleWrite      = errors.New("version being written older than existing version")
	WrongNode       = errors.New("request sent to node which isn't a replica for token")

	// Minimum tokens to claim
	minClaim = 100
)

type State struct {
	Version uint64         `json:"version"`
	Ring    [Tokens]string `json:"ring"`
}

type DB interface {
	Addr() string
	Get(key string, replicas int) (*Value, error)
	Set(key string, v *Value, replicas int) error

	GossipUpdate(*State) error
	Gossip() *State
}

type MemDB struct {
	addr string
	rl   int // replication level

	ringL   sync.Mutex
	ring    [Tokens]DB // 256 * 256 * 16 = 1MB
	version uint64
	peers   map[string]DB

	dbL sync.Mutex
	db  map[string]*Value
}

func NewMemDB(selfAddr string, rl int, seeds []*Client) *MemDB {
	d := &MemDB{
		addr:  selfAddr,
		rl:    rl,
		db:    make(map[string]*Value),
		peers: make(map[string]DB),
	}

	// Gossip to get initial state
	for _, s := range seeds {
		//TODO do concurrently (have fun managing d.peers)
		state := s.Gossip()
		if state == nil || state.Version <= d.version {
			continue
		}
		d.updateRing(state)
	}

	if d.version == 0 {
		log.Println("I'm the first! Claiming the whole ring.")
		d.version++
		// No ring, claim it all
		for x := range d.ring {
			d.ring[x] = d
		}
		return d
	}

	//FIXME This needs a distributed lock or vector clocks, but I'm way too lazy
	//      for that. Just only spin up one node at a time, ok?
	d.version++

	// Claim part of the ring
	candidates := []uint16{}
candidateSearch:
	for token := 0; token < Tokens; token++ {
		replicants := make(map[string]struct{}, rl)
		for i := uint16(token); i < uint16(token)+uint16(rl); i++ {
			a := d.ring[i].Addr()
			if _, ok := replicants[a]; ok {
				// This replicant owns two tokens too close together, take one
				candidates = append(candidates, uint16(token))
				token = int(i)
				continue candidateSearch
			}
			replicants[a] = struct{}{}
		}
	}

	log.Printf("Found %d candidates to create ring version %d", len(candidates), d.version)

	// If there's <=minClaim*2 candidates, grab them all. Otherwise take half
	if len(candidates) > (minClaim * 2) {
		candidates = candidates[:len(candidates)>>1]
	}
	for _, c := range candidates {
		d.ring[c] = d
	}

	// Grab up to minClaim more
	for left := minClaim - len(candidates); left > 0; left-- {
		for {
			i := rand.Intn(Tokens)
			if !d.has(i) {
				d.ring[i] = d
				break
			}
		}
	}

	// Build and gossip state
	state := &State{Version: d.version}
	for t, c := range d.ring {
		state.Ring[t] = c.Addr()
	}

	// debugging
	counts := make(map[string]int, len(d.peers))
	for _, c := range d.ring {
		counts[c.Addr()]++
	}
	for server, count := range counts {
		log.Printf("Ring verison %d: %s -> %d", d.version, server, count)
	}

	wg := sync.WaitGroup{}
	for _, c := range d.peers {
		// goroutines! 😎
		wg.Add(1)
		go func(c DB) {
			defer wg.Done()
			if err := c.GossipUpdate(state); err != nil {
				log.Printf("Error gossiping to %s: %v", c, err)
			} else {
				log.Printf("Gossip version %d to %s", d.version, c.Addr())
			}
		}(c)
	}
	go func() {
		wg.Wait()
		log.Printf("Done with initial gossip.")
	}()

	return d
}

// has returns true if this token is local
func (d *MemDB) has(token int) bool {
	d.ringL.Lock()
	defer d.ringL.Unlock()
	for i := 0; i < d.rl; i++ {
		if d.ring[uint16(token+i)].Addr() == d.Addr() {
			return true
		}
	}
	return false
}

// replicas returns the nodes that are repicas for a given token
func (d *MemDB) replicas(token int) []DB {
	d.ringL.Lock()
	defer d.ringL.Unlock()
	r := make([]DB, d.rl)
	for i := 0; i < d.rl; i++ {
		r[i] = d.ring[uint16(token+i)]
	}
	return r
}

func (d *MemDB) Get(key string, replicas int) (*Value, error) {
	if replicas > d.rl {
		return nil, TooManyReplicas
	}
	if len(key) == 0 {
		return nil, EmptyKey
	}

	// Only forward Get if needed
	if replicas == 0 {
		return d.localGet(key)
	}

	// Get replicas
	token := tokenize(key)
	nodes := d.replicas(token)
	log.Printf("Sending GET %q (%d) to %v", key, token, nodes)

	// Get results
	//TODO concurrently get from peers (probably want to add a timeout argument)
	results := []Result{}
	for _, node := range nodes {
		v, err := node.Get(key, 0)
		if err != nil {
			if err != NotFound {
				log.Printf("Error GETing %q from %s: %v", key, node, err)
			}
			continue
		}
		results = append(results, Result{Addr: node.Addr(), Value: v})

		// Gather number of results equal to replicas param
		if len(results) == replicas {
			break
		}
	}

	// Get timestamp counts
	tsCounts := map[uint64]int{}
	candidates := make(map[uint64]*Value, len(results))
	for _, result := range results {
		tsCounts[result.Timestamp]++

		if v, ok := candidates[result.Timestamp]; ok {
			if !v.Equals(result.Value) {
				// Well this should never happen
				log.Printf("Values with the same Key/Version mismatch: %#v != %#v", v, result)
				tsCounts[result.Timestamp] -= 2 // negate both mismatched results
				continue
			}
		}
		candidates[result.Timestamp] = result.Value
	}

	// Get highest timestamp with count >= replicas
	var winner uint64
	for ts, count := range tsCounts {
		if count >= replicas && ts > winner {
			winner = ts
			break
		}
	}

	// No winners, ouch
	if winner == 0 {
		log.Printf("Out of %d results for %q, no single version occurred at least %d times.",
			len(results), key, replicas)
		return nil, NotFound
	}

	value := candidates[winner]

	// Read repair!
	// FIXME Do concurrently
	for _, node := range nodes {
		if err := node.Set(key, value, 0); err != nil {
			//TODO Just think of how much fun getting StaleWrites here will be!
			//     Surely there's something intelligent to do?
			log.Printf("Failed read repairing %q to %s: %v", key, node, err)
			continue
		}
		log.Printf("Read repair for %s=%q (ts=%v) to %v", key, value.V, winner, node)
	}

	// Hey! We have a winner! Something worked or at least a happy confluence
	// of bugs made it look like things worked!
	return value, nil
}

func (d *MemDB) localGet(key string) (*Value, error) {
	{
		// Sanity check
		token := tokenize(key)
		has := d.has(token)

		if !has {
			//TODO This would be a great time to Gossip as obviously the cluster state
			//     is inconsistent.
			log.Printf("Local GET for key %q token %d even though this node isn't a replica!", key, token)
		}
	}

	// Always try to retrieve a key locally even if this node isn't a replica. It
	// may have been in the past and a stale version is better than no version?
	d.dbL.Lock()
	v := d.db[string(key)]
	d.dbL.Unlock()

	if v == nil {
		return nil, NotFound
	}
	return v, nil
}

// Set key to value.
func (d *MemDB) Set(key string, v *Value, replicas int) error {
	if replicas > d.rl {
		return TooManyReplicas
	}
	if len(key) == 0 {
		return EmptyKey
	}

	// Only forward Set if needed
	if replicas == 0 {
		return d.localSet(key, v)
	}

	// Get replicas
	token := tokenize(key)
	nodes := d.replicas(token)

	log.Printf("SET %s=%q ts %d on peers %v", key, v.V, v.Timestamp, nodes)

	// Get results
	//TODO concurrently set peers (probably want to add a out argument)
	sets := 0
	var lastErr error
	for _, node := range nodes {
		if err := node.Set(key, v, 0); err != nil {
			log.Printf("SET of %s to %v failed: %v", key, node, err)
			lastErr = err
			continue
		}
		sets++
		if sets == replicas {
			break
		}
	}
	return lastErr
}

func (d *MemDB) localSet(key string, v *Value) error {
	if !d.has(tokenize(key)) {
		log.Printf("Aborting SET for %q because local node is not a replica.", key)
		//XXX I don't think it's worth reporting this as an error because cluster
		//    state changes asynchronously... but maybe clients should
		return WrongNode
	}

	//FIXME Can't hash by []bytes so we have to index by strings
	k := string(key)

	d.dbL.Lock()
	defer d.dbL.Unlock()
	if existing, ok := d.db[k]; ok && existing.Timestamp > v.Timestamp {
		return StaleWrite
	}
	d.db[k] = v
	return nil
}

func (d *MemDB) GossipUpdate(s *State) error {
	d.ringL.Lock()
	defer d.ringL.Unlock()
	if s.Version < d.version {
		return StaleWrite
	}
	if s.Version == d.version {
		// We could error check here, but that sounds awful TODO I guess?
		return nil
	}

	d.updateRing(s)
	return nil
}

func (d *MemDB) Gossip() *State {
	d.ringL.Lock()
	defer d.ringL.Unlock()
	s := State{Version: d.version}
	for i, c := range d.ring {
		s.Ring[i] = c.Addr()
	}
	return &s
}

// MUST HAVE ringL SRY
func (d *MemDB) updateRing(state *State) {
	log.Printf("Updating ring state from %d to %d", d.version, state.Version)
	d.version = state.Version
	peers := map[string]DB{}
	for token, addr := range state.Ring {
		//HACK for debugging
		if token > 1078 && token < 1090 {
			fmt.Println(addr, token)
		}
		if addr == d.addr {
			// Hey, that's me!
			d.ring[token] = d
		} else {
			// First try to find client in new peer map
			if c, ok := peers[addr]; ok {
				d.ring[token] = c
				continue
			}

			// Well that failed, try to find it in the old peer map
			if c, ok := d.peers[addr]; ok {
				d.ring[token] = c
				peers[addr] = c
				continue
			}

			// Ok, it's a new client
			c := &Client{addr: addr}
			d.ring[token] = c
			d.peers[addr] = c
		}
	}

	// Overwrite old peers with new
	d.peers = peers
}

func (d *MemDB) Addr() string {
	return d.addr
}

func (d *MemDB) String() string {
	return "self"
}

type nodeMap map[string]DB

func (n nodeMap) String() string {
	s := make([]string, 0, len(n))
	for key := range n {
		s = append(s, key)
	}
	sort.Strings(s)
	return strings.Join(s, ", ")
}
