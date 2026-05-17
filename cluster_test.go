package clustercache_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	clustercache "github.com/ubgo/cache-cluster"
	"github.com/ubgo/cache/cachetest"
)

type cluster struct {
	nodes map[string]*clustercache.Node
	srvs  []*httptest.Server
}

func (c *cluster) close() {
	for _, s := range c.srvs {
		s.Close()
	}
	for _, n := range c.nodes {
		_ = n.Close()
	}
}

// build3 wires a 3-node cluster of in-process httptest servers sharing one
// loader (call-counted).
func build3(t *testing.T, loads *int64) *cluster {
	t.Helper()
	ids := []string{"n1", "n2", "n3"}
	cl := &cluster{nodes: map[string]*clustercache.Node{}}
	peers := map[string]string{}
	// Two-pass: create nodes, then servers, then inject the peer URL map.
	for _, id := range ids {
		loader := func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt64(loads, 1)
			return []byte("v:" + key), nil
		}
		cl.nodes[id] = clustercache.New(id, cachetest.NewMock(),
			clustercache.WithLoader(loader))
	}
	for _, id := range ids {
		srv := httptest.NewServer(cl.nodes[id].Handler())
		cl.srvs = append(cl.srvs, srv)
		peers[id] = srv.URL
	}
	for _, id := range ids {
		clustercache.WithPeers(peers)(cl.nodes[id])
	}
	return cl
}

func TestPeerFillLoadsOncePerKey(t *testing.T) {
	var loads int64
	cl := build3(t, &loads)
	defer cl.close()
	ctx := context.Background()

	// Every node asks for the same key concurrently; exactly one load total.
	var wg sync.WaitGroup
	for _, n := range cl.nodes {
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(node *clustercache.Node) {
				defer wg.Done()
				v, err := node.Get(ctx, "hot")
				if err != nil || string(v) != "v:hot" {
					t.Errorf("get hot: %q %v", v, err)
				}
			}(n)
		}
	}
	wg.Wait()
	if got := atomic.LoadInt64(&loads); got != 1 {
		t.Fatalf("loader ran %d times across the cluster, want 1", got)
	}
}

func TestOwnershipIsConsistentAcrossNodes(t *testing.T) {
	var loads int64
	cl := build3(t, &loads)
	defer cl.close()
	ctx := context.Background()

	// Whoever we ask, the value is identical (owner is deterministic).
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("k:%d", i)
		want := "v:" + key
		for _, n := range cl.nodes {
			v, err := n.Get(ctx, key)
			if err != nil || string(v) != want {
				t.Fatalf("key %s via a node: %q %v", key, v, err)
			}
		}
	}
}

func TestSetFromNonOwnerRoutesToOwner(t *testing.T) {
	var loads int64
	cl := build3(t, &loads)
	defer cl.close()
	ctx := context.Background()

	// Pick a key and write it from every node; read it back from every node.
	const key = "shared"
	anyNode := func() *clustercache.Node {
		for _, n := range cl.nodes {
			return n
		}
		return nil
	}
	if err := anyNode().Set(ctx, key, []byte("written")); err != nil {
		t.Fatal(err)
	}
	for id, n := range cl.nodes {
		v, err := n.Get(ctx, key)
		if err != nil || string(v) != "written" {
			t.Fatalf("node %s sees %q %v, want written", id, v, err)
		}
	}
	if atomic.LoadInt64(&loads) != 0 {
		t.Fatal("loader should not run when the key was explicitly Set")
	}
}

func TestDelForcesReload(t *testing.T) {
	var loads int64
	cl := build3(t, &loads)
	defer cl.close()
	ctx := context.Background()

	var n *clustercache.Node
	for _, x := range cl.nodes {
		n = x
		break
	}
	if _, err := n.Get(ctx, "k"); err != nil { // load #1
		t.Fatal(err)
	}
	if err := n.Del(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Get(ctx, "k"); err != nil { // load #2 (was deleted)
		t.Fatal(err)
	}
	if got := atomic.LoadInt64(&loads); got != 2 {
		t.Fatalf("want 2 loads (delete invalidates), got %d", got)
	}
}

func TestRingDistributesAndRebalances(t *testing.T) {
	r := clustercache.NewRing(64, "a", "b", "c")
	counts := map[string]int{}
	for i := 0; i < 9000; i++ {
		counts[r.Owner(fmt.Sprintf("key-%d", i))]++
	}
	if len(counts) != 3 {
		t.Fatalf("expected all 3 peers to own keys, got %v", counts)
	}
	for id, c := range counts {
		if c < 1500 { // ~3000 expected; generous lower bound
			t.Fatalf("peer %s badly underweighted: %d", id, c)
		}
	}
	// Removing a peer must not reassign keys that stayed with survivors.
	before := map[string]string{}
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("s-%d", i)
		before[k] = r.Owner(k)
	}
	r.Remove("b")
	moved := 0
	for k, was := range before {
		now := r.Owner(k)
		if was != "b" && now != was {
			moved++
		}
	}
	if moved != 0 {
		t.Fatalf("consistent hashing moved %d keys that should have stayed", moved)
	}
}
