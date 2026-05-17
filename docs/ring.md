# Ring — consistent-hash ring

A `Ring` maps keys to owning peers with virtual nodes for even spread and
minimal reshuffle on membership change. Safe for concurrent use
(`Owner`/`Peers` take a read lock; `Add`/`Remove` take the write lock).

### Ring

`type Ring struct { ... }`

What it is: the consistent-hash ring. Invariant: ring points are always sorted
ascending and every point has an owner entry (`Owner` binary-searches the sort).
`replicas` virtual points per peer smooth the keyspace split so a removed
peer's share scatters across many neighbours rather than landing on one.

Use cases:

- Use the `Node` abstraction for normal clustering (it owns a `Ring`).
- Use a bare `Ring` for custom routing (sharded workers, partitioned queues).

```go
import clustercache "github.com/ubgo/cache-cluster"

r := clustercache.NewRing(64, "node-a", "node-b", "node-c")
owner := r.Owner("user:42")
```

### NewRing

`func NewRing(replicas int, peers ...string) *Ring`

What it is: builds a ring with `replicas` virtual nodes per peer (`replicas <=
0` defaults to 64) and inserts the initial `peers`.

Use cases:

- Bootstrap a ring with the known membership at startup.
- Tune `replicas` higher for smoother distribution with few peers.

```go
r := clustercache.NewRing(128, "10.0.0.1", "10.0.0.2")
```

### Add

`func (r *Ring) Add(peer string)`

What it is: inserts a peer and its virtual nodes (idempotent — re-adding an
existing peer is a no-op).

Use cases:

- Scale-out: register a newly started node.
- Membership discovery callback.

```go
r.Add("10.0.0.3") // newly joined node
```

### Remove

`func (r *Ring) Remove(peer string)`

What it is: drops a peer and its virtual nodes. Only keys whose nearest
clockwise point belonged to that peer move (to the next surviving point); every
other key keeps its owner — the no-rebalance-on-remove property that
distinguishes consistent hashing from modulo sharding.

Use cases:

- Scale-in / graceful drain of a node.
- Eject an unhealthy node detected by health checks.

```go
r.Remove("10.0.0.2") // only that node's share of keys moves
```

### Owner

`func (r *Ring) Owner(key string) string`

What it is: returns the peer that owns `key` (the peer of the first ring point
clockwise of `hash(key)`, wrapping circularly), or `""` if the ring is empty.

Use cases:

- Decide locally whether this node should serve a key or proxy it.
- Pre-compute key→node placement for a bulk migration.

```go
if r.Owner("user:42") == self { /* serve locally */ } else { /* proxy */ }
```

### Peers

`func (r *Ring) Peers() []string`

What it is: returns the current membership (unordered).

Use cases:

- Expose cluster membership on an admin/debug endpoint.
- Fan-out a broadcast to every peer.

```go
for _, p := range r.Peers() {
	fmt.Println("member:", p)
}
```
