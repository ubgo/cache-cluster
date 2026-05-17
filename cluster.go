package clustercache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/ubgo/cache"
)

// Loader fills a key at its owning node on a miss. Required for read-through;
// without it a miss is simply ErrNotFound.
type Loader func(ctx context.Context, key string) ([]byte, error)

// Node is one member of the cluster. It owns the keys the ring assigns to it
// (filled once via the Loader, deduped by single-flight) and proxies other
// keys to their owner over HTTP. Construct with New; expose Handler() so peers
// can reach it.
//
// Routing invariant: every operation resolves ring.Owner(key); owned keys hit
// the local cache.Cache, non-owned keys are proxied to the owner's base URL.
// The loader only ever runs at a key's owner, so a hot key is loaded exactly
// once cluster-wide regardless of how many nodes or goroutines request it.
type Node struct {
	self    string
	local   cache.Cache
	ring    *Ring
	peerURL map[string]string // peer id -> base URL
	loader  Loader
	hc      *http.Client
	ttl     time.Duration

	flight flightGroup
}

// Option configures New.
type Option func(*Node)

// WithPeers sets the full membership as id -> base URL (must include self).
func WithPeers(peers map[string]string) Option {
	return func(n *Node) {
		for id := range peers {
			n.ring.Add(id)
		}
		n.peerURL = peers
	}
}

// WithLoader sets the read-through loader used at a key's owner.
func WithLoader(l Loader) Option { return func(n *Node) { n.loader = l } }

// WithFillTTL sets the TTL used when a loaded value is stored (0 = no expiry).
func WithFillTTL(d time.Duration) Option { return func(n *Node) { n.ttl = d } }

// WithHTTPClient overrides the client used for peer requests.
func WithHTTPClient(c *http.Client) Option { return func(n *Node) { n.hc = c } }

// New builds a node with id self backed by a local cache.Cache. self is added
// to the ring immediately so a single-node cluster owns every key and works
// before any WithPeers call; WithPeers later adds the rest of the membership.
func New(self string, local cache.Cache, opts ...Option) *Node {
	n := &Node{
		self:    self,
		local:   local,
		ring:    NewRing(64),
		peerURL: map[string]string{},
		hc:      &http.Client{Timeout: 5 * time.Second},
	}
	n.ring.Add(self)
	for _, o := range opts {
		o(n)
	}
	return n
}

// owns reports whether this node owns key.
func (n *Node) owns(key string) bool { return n.ring.Owner(key) == n.self }

// Get returns the value for key, fetching from the owning peer if this node
// is not the owner, and load-filling once at the owner on a miss.
func (n *Node) Get(ctx context.Context, key string) ([]byte, error) {
	if n.owns(key) {
		return n.localGetOrLoad(ctx, key)
	}
	return n.remote(ctx, http.MethodGet, key, nil)
}

// localGetOrLoad serves from the local store, or single-flights the loader so
// concurrent requests for the same key fill exactly once. This runs at the
// key's owner (called directly when this node owns the key, and via Handler
// when a peer proxies a GET here), which is why the loader is owner-only.
// A non-ErrNotFound error from the backend is propagated as-is (only a
// genuine miss falls through to the loader).
func (n *Node) localGetOrLoad(ctx context.Context, key string) ([]byte, error) {
	if v, err := n.local.Get(ctx, key); err == nil {
		return v, nil
	} else if !errors.Is(err, cache.ErrNotFound) {
		return nil, err
	}
	if n.loader == nil {
		return nil, cache.ErrNotFound
	}
	v, _, err := n.flight.Do(key, func() (any, error) {
		// Re-check inside the flight: between the miss above and winning the
		// flight slot, a previous flight for this key may have completed and
		// filled the backend. Without this re-check that window would cause a
		// redundant loader call.
		if b, gerr := n.local.Get(ctx, key); gerr == nil {
			return b, nil
		}
		b, lerr := n.loader(ctx, key)
		if lerr != nil {
			return nil, lerr
		}
		_ = n.local.Set(ctx, key, b, n.ttl)
		return b, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

// Set stores key at its owner (locally or via the owning peer).
func (n *Node) Set(ctx context.Context, key string, val []byte) error {
	if n.owns(key) {
		return n.local.Set(ctx, key, val, n.ttl)
	}
	_, err := n.remote(ctx, http.MethodPut, key, val)
	return err
}

// Del removes key at its owner.
func (n *Node) Del(ctx context.Context, key string) error {
	if n.owns(key) {
		return n.local.Del(ctx, key)
	}
	_, err := n.remote(ctx, http.MethodDelete, key, nil)
	return err
}

// Has reports presence (owner-routed; does not trigger the loader).
func (n *Node) Has(ctx context.Context, key string) (bool, error) {
	if n.owns(key) {
		return n.local.Has(ctx, key)
	}
	_, err := n.remote(ctx, http.MethodGet, key, nil)
	if errors.Is(err, cache.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// Close closes the local backend.
func (n *Node) Close() error { return n.local.Close() }

// remote proxies an operation to the key's owning peer over HTTP. It resolves
// the owner via the ring, looks up its base URL from the WithPeers map (an
// "unknown owner" error means the ring and peerURL map disagree, i.e. a
// misconfigured WithPeers), and maps HTTP status back to the cache contract:
// 200 = value, 204 = no content (Set/Del ok), 404 = cache.ErrNotFound, any
// other status = error. The response body is always closed.
func (n *Node) remote(ctx context.Context, method, key string, body []byte) ([]byte, error) {
	owner := n.ring.Owner(key)
	base, ok := n.peerURL[owner]
	if !ok {
		return nil, errors.New("clustercache: unknown owner " + owner)
	}
	u := base + "/_cache?key=" + url.QueryEscape(key)
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return nil, err
	}
	resp, err := n.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		return io.ReadAll(resp.Body)
	case http.StatusNoContent:
		return nil, nil
	case http.StatusNotFound:
		return nil, cache.ErrNotFound
	default:
		return nil, errors.New("clustercache: peer status " + resp.Status)
	}
}

// flightGroup is a minimal single-flight (clustercache-local; the core's is
// unexported so it cannot be reused here). It guarantees that for a given key,
// only one fn runs at a time and all concurrent callers share its result.
type flightGroup struct {
	mu sync.Mutex
	m  map[string]*flightCall
}

// flightCall is one in-progress call. The WaitGroup (count 1) is the barrier
// late callers block on; val/err are written by the leader before Done and
// read by followers after Wait (the WaitGroup provides the happens-before).
type flightCall struct {
	wg  sync.WaitGroup
	val any
	err error
}

// Do runs fn for key exactly once across concurrent callers. The first caller
// (leader) registers a flightCall, runs fn, then removes the entry so a later,
// non-overlapping call re-runs fn (this is a de-dup of concurrent calls, not a
// cache). Followers find the live call, wait, and return its result with
// shared=true.
func (g *flightGroup) Do(key string, fn func() (any, error)) (v any, shared bool, err error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = map[string]*flightCall{}
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, true, c.err
	}
	c := &flightCall{}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	return c.val, false, c.err
}
