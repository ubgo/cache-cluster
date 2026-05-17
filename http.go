package clustercache

import (
	"errors"
	"io"
	"net/http"

	"github.com/ubgo/cache"
)

// Handler returns the HTTP handler peers use to reach this node. Mount it at
// the base URL advertised in WithPeers:
//
//	http.Handle("/_cache", node.Handler())
//
// Routes (all on /_cache?key=...):
//
//	GET    → value (200) | 404 if absent/no loader
//	PUT    → store request body (204)
//	DELETE → delete (204)
//
// A GET for a key this node owns will load-fill via the Loader, exactly like
// a local Get — that is what makes peer fill work.
func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/_cache", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}
		ctx := r.Context()
		switch r.Method {
		case http.MethodGet:
			// Same path a local Get takes: the loader (and its single-flight)
			// runs here at the owner, so a peer's proxied GET fills exactly
			// once, shared with any concurrent local or peer requests.
			v, err := n.localGetOrLoad(ctx, key)
			if errors.Is(err, cache.ErrNotFound) {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(v)
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := n.local.Set(ctx, key, body, n.ttl); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case http.MethodDelete:
			if err := n.local.Del(ctx, key); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	return mux
}
