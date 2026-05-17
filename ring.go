// Package clustercache adds peer-aware distribution on top of any
// github.com/ubgo/cache backend: a consistent-hash ring decides which node
// owns a key, the owner fills it once (single-flight) via a user loader, and
// peers fetch from the owner over HTTP. This is the groupcache pattern with
// the ubgo/cache interface.
package clustercache

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// Ring is a consistent-hash ring with virtual nodes for even key spread and
// minimal reshuffle when membership changes. Safe for concurrent reads
// (Owner/Peers take the read lock; Add/Remove take the write lock).
//
// Invariant: points is always sorted ascending and every element of points
// has a corresponding owner entry; Owner relies on the sort for its binary
// search. replicas virtual points per peer smooth the keyspace split so each
// peer gets ~1/len(peers) of the ring and a removed peer's share scatters
// across many neighbours rather than landing entirely on one.
type Ring struct {
	mu       sync.RWMutex
	replicas int
	points   []uint32          // sorted hash ring
	owner    map[uint32]string // point -> peer id
	peers    map[string]bool
}

// NewRing builds a ring with the given number of virtual nodes per peer
// (replicas <= 0 defaults to 64).
func NewRing(replicas int, peers ...string) *Ring {
	if replicas <= 0 {
		replicas = 64
	}
	r := &Ring{replicas: replicas, owner: map[uint32]string{}, peers: map[string]bool{}}
	for _, p := range peers {
		r.Add(p)
	}
	return r
}

// hashKey maps any string (a key, or a "peer#i" virtual-node label) onto the
// 32-bit ring. CRC32-IEEE is fast and spreads well enough for load balancing;
// it is not and need not be cryptographic.
func hashKey(s string) uint32 { return crc32.ChecksumIEEE([]byte(s)) }

// Add inserts a peer (idempotent).
func (r *Ring) Add(peer string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.peers[peer] {
		return
	}
	r.peers[peer] = true
	for i := 0; i < r.replicas; i++ {
		h := hashKey(peer + "#" + strconv.Itoa(i))
		r.points = append(r.points, h)
		r.owner[h] = peer
	}
	sort.Slice(r.points, func(i, j int) bool { return r.points[i] < r.points[j] })
}

// Remove drops a peer and its virtual nodes. Only keys whose nearest
// clockwise point was one of this peer's points move (to the next surviving
// point); every other key keeps its owner. This no-rebalance-on-remove
// behaviour is the whole point of consistent hashing versus modulo sharding.
// The filter reuses the backing array (points[:0]) to avoid an allocation.
func (r *Ring) Remove(peer string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.peers[peer] {
		return
	}
	delete(r.peers, peer)
	kept := r.points[:0]
	for _, p := range r.points {
		if r.owner[p] == peer {
			delete(r.owner, p)
			continue
		}
		kept = append(kept, p)
	}
	r.points = kept
}

// Owner returns the peer that owns key, or "" if the ring is empty. The owner
// is the peer of the first ring point clockwise of hashKey(key); sort.Search
// finds the first point >= h, and if h is past the last point the ring wraps
// back to index 0 (the ring is circular).
func (r *Ring) Owner(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 {
		return ""
	}
	h := hashKey(key)
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= h })
	if i == len(r.points) {
		i = 0 // wrap around the ring
	}
	return r.owner[r.points[i]]
}

// Peers returns the current membership (unordered).
func (r *Ring) Peers() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.peers))
	for p := range r.peers {
		out = append(out, p)
	}
	return out
}
