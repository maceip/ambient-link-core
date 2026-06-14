// Package webapp serves the glasses companion SPA from a local directory.
package webapp

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// Dir returns an http.Handler that serves static files from root.
// Unknown paths fall back to index.html (SPA).
func Dir(root string) http.Handler {
	root = filepath.Clean(root)
	fs := http.FileServer(http.Dir(root))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "" {
			http.ServeFile(w, r, filepath.Join(root, "index.html"))
			return
		}
		p := filepath.Join(root, filepath.Clean("/"+strings.TrimPrefix(r.URL.Path, "/")))
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			fs.ServeHTTP(w, r)
			return
		}
		http.ServeFile(w, r, filepath.Join(root, "index.html"))
	})
}
