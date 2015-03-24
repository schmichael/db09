package db09

import (
	"errors"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	TooManyReplicas = errors.New("desired replicas exceeds cluster replication level")
	EmptyKey        = errors.New("key wasn't specified")
	NotFound        = errors.New("key not found")
	StaleWrite      = errors.New("version being written older than existing version")
	WrongNode       = errors.New("request sent to node which isn't a replica for token")
)

type State struct {
	Version uint64   `json:"version"`
	Ring    []string `json:"ring"`
}

type DB interface {
	Addr() string
	Get(key string, replicas int) (Value, error)
	Set(key string, v Value, replicas int) error

	GossipUpdate(*State) error
	Gossip() *State
	Version() uint64
}

type MemDB struct {
	addr  string
	parts int // partitions (tokens)
	rf    int // replication factor

	ringL   sync.Mutex
	ring    []DB
	version uint64
	peers   map[string]*Client

	dbL sync.Mutex
	db  []map[string]Value // token -> key -> value
}

func NewMemDB(selfAddr string, partitions, rf int, seeds []*Client) *MemDB {
	d := &MemDB{
		addr:  selfAddr,
		parts: partitions,
		rf:    rf,
		peers: make(map[string]*Client, len(seeds)),
		db:    make([]map[string]Value, partitions),
		ring:  make([]DB, partitions),
	}

	// Initialize in memory db
	for i := range d.db {
		d.db[i] = make(map[string]Value)
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
		log.Println("Frist! Claiming the whole ring.")
		d.version++
		// No ring, claim every nth token where n=RF
		for i := 0; i < ((partitions - rf) + 1); i += rf {
			d.ring[i] = d
		}
		return d
	}

	//FIXME This needs a distributed lock or vector clocks, but I'm way too lazy
	//      for that. Just only spin up one node at a time, ok?
	d.version++

	// Claim part of the ring
	candidates := []int{}
candidateSearch:
	for token := 0; token < partitions; {
		if d.ring[token] == nil {
			log.Printf("Unclaimed partition: %d -- it's a candidate!", token)
			candidates = append(candidates, token)
			token += rf
			continue candidateSearch
		}

		replicants := make(map[string]struct{}, rf)
		for i := token; i < token+rf; i = (i + 1) % partitions {
			if d.ring[i] == nil {
				continue
			}
			a := d.ring[i].Addr()
			if _, ok := replicants[a]; ok {
				// This replicant owns two tokens too close together, take one
				candidates = append(candidates, token)
				token += rf
				continue candidateSearch
			}
			replicants[a] = struct{}{}
		}
		// Not a candidate, check next partition
		token++
	}

	log.Printf("Found %d candidates to create ring version %d", len(candidates), d.version)

	for _, c := range candidates {
		d.ring[c] = d
	}

	// Grab up to minClaim more
	minClaim := partitions / rf / len(d.peers)
	i := 0
	for left := minClaim - len(candidates); left > 0; left-- {
		for ; i < partitions; i++ {
			if !d.has(i) {
				d.ring[i] = d
				break
			}
		}
	}

	// Build and gossip state
	state := &State{Version: d.version, Ring: make([]string, partitions)}
	for t, c := range d.ring {
		if c != nil {
			state.Ring[t] = c.Addr()
		}
	}

	// debugging
	counts := make(map[string]int, len(d.peers)+1)
	for _, c := range d.ring {
		switch c {
		case nil:
			counts["<unassigned>"]++
		default:
			counts[c.Addr()]++
		}
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

	ver := d.version
	go func() {
		wg.Wait()
		log.Printf("Done with initial gossip for version %d", ver)
	}()

	return d
}

// has returns true if this token is local
func (d *MemDB) has(token int) bool {
	d.ringL.Lock()
	defer d.ringL.Unlock()
	self := d.Addr()
	for i := 0; i < d.rf; i++ {
		if Addr(d.ring[(i+token)%d.parts]) == self {
			return true
		}
	}
	return false
}

// replicas returns the nodes that are repicas for a given token
func (d *MemDB) replicas(token int) []DB {
	d.ringL.Lock()
	defer d.ringL.Unlock()
	r := make([]DB, 0, d.rf)
	for i := 0; i < d.rf; i++ {
		if c := d.ring[(token+i)%d.parts]; c != nil {
			r = append(r, c)
		}
	}
	return r
}

func (d *MemDB) Get(key string, replicas int) (Value, error) {
	if replicas > d.rf {
		return EmptyValue, TooManyReplicas
	}
	if len(key) == 0 {
		return EmptyValue, EmptyKey
	}

	// Only forward Get if needed
	if replicas == 0 {
		return d.localGet(key)
	}

	// Get replicas
	token := d.tokenize(key)
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
	candidates := make(map[uint64]Value, len(results))
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
		return EmptyValue, NotFound
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

func (d *MemDB) localGet(key string) (Value, error) {
	token := d.tokenize(key)

	// Sanity check
	has := d.has(token)
	if !has {
		//TODO This would be a great time to Gossip as obviously the cluster state
		//     is inconsistent.
		log.Printf("Local GET for key %q token %d even though this node isn't a replica!", key, token)
	}

	// Always try to retrieve a key locally even if this node isn't a replica. It
	// may have been in the past and a stale version is better than no version?
	d.dbL.Lock()
	v, ok := d.db[token][string(key)]
	d.dbL.Unlock()

	if !ok {
		return EmptyValue, NotFound
	}
	return v, nil
}

// Set key to value.
func (d *MemDB) Set(key string, v Value, replicas int) error {
	if replicas > d.rf {
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
	token := d.tokenize(key)
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

func (d *MemDB) localSet(key string, v Value) error {
	token := d.tokenize(key)
	if !d.has(token) {
		log.Printf("Aborting SET for %q because local node is not a replica.", key)
		//XXX I don't think it's worth reporting this as an error because cluster
		//    state changes asynchronously... but maybe clients should be made aware
		return WrongNode
	}

	//FIXME Can't hash by []bytes so we have to index by strings
	k := string(key)

	d.dbL.Lock()
	defer d.dbL.Unlock()
	if existing, ok := d.db[token][k]; ok && existing.Timestamp > v.Timestamp {
		return StaleWrite
	}
	d.db[token][k] = v
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
	s := State{Version: d.version, Ring: make([]string, d.parts)}
	for i, c := range d.ring {
		s.Ring[i] = Addr(c)
	}
	return &s
}

// MUST HAVE ringL SRY
func (d *MemDB) updateRing(state *State) {
	log.Printf("Updating ring state from %d to %d", d.version, state.Version)
	peers := map[string]*Client{}
	streamTokens := map[string][]int{}
	for token, addr := range state.Ring {
		log.Println("recvd: ", token, " ", addr)
		if addr == d.addr {
			// Hey, that's me!
			d.ring[token] = d
			continue
		}
		if addr == "" {
			// Unassigned
			d.ring[token] = nil
			continue
		}

		wasMine := false
		if d.ring[token] != nil {
			wasMine = d.ring[token].Addr() == d.addr
		}

		var c *Client

		// First try to find client in new peer map
		if c = peers[addr]; c != nil {
			d.ring[token] = c
		}

		// Well that failed, try to find it in the old peer map
		if c = d.peers[addr]; c != nil {
			d.ring[token] = c
			peers[addr] = c
		}

		if c == nil {
			// Ok, it's a new client
			c = &Client{addr: addr}
			d.ring[token] = c
			d.peers[addr] = c
			log.Printf("Discovered new peer via incoming gossip: %s (token %d)", c, token)
		}

		// Stream data for tokens that used to belong to this node to the new owner
		if wasMine {
			streamTokens[c.Addr()] = append(streamTokens[c.Addr()], token)
		}
	}

	// Asynchronously stream the list of tokens we used to own to their new
	// owners
	for addr, tokens := range streamTokens {
		go d.stream(d.peers[addr], tokens, state.Version)
	}

	// Overwrite old peers with new
	d.peers = peers
	d.version = state.Version
}

func (d *MemDB) Version() uint64 {
	d.ringL.Lock()
	defer d.ringL.Unlock()
	return d.version
}

func (d *MemDB) Addr() string {
	return d.addr
}

func (d *MemDB) String() string {
	return "self"
}

func (d *MemDB) stream(p *Client, tokens []int, ver uint64) {
	for i := 0; ; i++ {
		peerver := p.Version()
		if peerver == ver {
			break
		}
		if peerver < ver {
			if i > 20 {
				log.Printf("Giving up streaming %d tokens to client %s", len(tokens), p)
			}
			log.Printf("Waiting for client %s to upgrade ring version from %d to %d before streaming %d token", p, peerver, ver, len(tokens))
			time.Sleep(3 * time.Second)
		}
		if peerver > ver {
			log.Printf("Was going to stream %d tokens to client %s but remote version %d > local version %d", len(tokens), p, peerver, ver)
			return
		}
	}

	log.Printf("Streaming %d tokens to %s (ring version %d)", len(tokens), p, ver)
	for _, token := range tokens {
		d.dbL.Lock()
		for k, v := range d.db[token] {
			if err := p.Set(k, v, 0); err != nil {
				log.Printf("Error trying to stream key %q for token %d to client %s: %v", k, token, p, err)
			}
		}
		// We've streamed this token to another client, delete it locally
		d.db[token] = make(map[string]Value)
		d.dbL.Unlock()
	}

}

type NodeStatus struct {
	Addr  string         `json:"addr"`
	RF    int            `json:"replication_factor"`
	Peers map[string]int `json:"peers"`
	Ver   uint64         `json:"ring_version"`
	Parts int            `json:"total_partitions"`
	Local int            `json:"local_partitions"`
	Keys  int            `json:"num_keys"`
	Tombs int            `json:"num_tombstones"`
}

func (d *MemDB) NodeStatus() *NodeStatus {
	n := NodeStatus{
		Addr:  d.addr,
		RF:    d.rf,
		Parts: d.parts,
	}

	d.ringL.Lock()
	n.Ver = d.version
	n.Peers = make(map[string]int, len(d.peers))
	d.ringL.Unlock()

	d.dbL.Lock()
	for token, db := range d.ring {
		if db == nil {
			n.Peers["<unassigned>"]++
			continue
		}
		n.Peers[db.Addr()]++
		if db.Addr() == d.addr {
			n.Local++
			for _, v := range d.db[token] {
				n.Keys++
				if v.Deleted {
					n.Tombs++
				}
			}
		}
	}
	d.dbL.Unlock()

	return &n
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

func Addr(d DB) string {
	if d == nil {
		return ""
	}
	return d.Addr()
}
