package api

import (
	"net/http"

	"adapter/internal/exec"
	"adapter/internal/sandbox"
)

// NewRouter wires all API endpoints onto a stdlib ServeMux and returns it with
// the middleware stack applied. Pattern-matching syntax requires Go 1.22+.
func NewRouter(mgr *sandbox.Manager, registry *exec.Registry) http.Handler {
	mux := http.NewServeMux()

	sh := &SandboxHandler{Mgr: mgr, Registry: registry}
	eh := &ExecHandler{Mgr: mgr, Registry: registry}

	// Sandbox lifecycle.
	mux.HandleFunc("POST /api/v1/sandboxes", sh.Create)
	mux.HandleFunc("GET /api/v1/sandboxes/{id}", sh.Get)
	mux.HandleFunc("DELETE /api/v1/sandboxes/{id}", sh.Delete)

	// Command execution.
	mux.HandleFunc("POST /api/v1/sandboxes/{id}/exec", eh.Create)
	mux.HandleFunc("GET /api/v1/sandboxes/{id}/exec", eh.List)
	mux.HandleFunc("GET /api/v1/sandboxes/{id}/exec/{exec_id}", eh.Get)
	mux.HandleFunc("GET /api/v1/sandboxes/{id}/exec/{exec_id}/output", eh.GetOutput)
	mux.HandleFunc("POST /api/v1/sandboxes/{id}/exec/{exec_id}/stdin", eh.Stdin)
	mux.HandleFunc("POST /api/v1/sandboxes/{id}/exec/{exec_id}/signal", eh.Signal)
	mux.HandleFunc("GET /api/v1/sandboxes/{id}/exec/{exec_id}/output/stream", eh.GetOutputStream)

	// Liveness probe — not part of the customer API surface.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	// TODO: filesystem, process inspection, image catalog, usage, alerts.

	return Chain(mux,
		RecoverMiddleware,      // outermost: catches panics from anything below
		RequestIDMiddleware,    // assigns a request id used by logging and correlation
		TenantHeadersMiddleware, // extracts X-Tenant-ID, X-Operator
		LoggingMiddleware,      // innermost middleware: logs after the handler runs
	)
}