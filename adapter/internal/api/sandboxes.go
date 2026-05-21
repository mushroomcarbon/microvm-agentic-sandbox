package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"

	"adapter/internal/exec"
	"adapter/internal/sandbox"
)

// SandboxHandler holds the dependencies the lifecycle endpoints need. Mgr is
// the sandbox manager; Registry is the exec registry (so Delete can clear
// records on session end).
type SandboxHandler struct {
	Mgr      *sandbox.Manager
	Registry *exec.Registry
}

// Create implements POST /api/v1/sandboxes. All fields from the request body
// flow into sandbox.CreateSpec and are persisted in the sandboxes row.
func (h *SandboxHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req CreateSandboxRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "could not parse request body: "+err.Error())
		return
	}

	if req.Flavor == "" {
		writeError(w, http.StatusBadRequest, "missing_flavor", "flavor is required")
		return
	}
	if req.ImageID == "" {
		writeError(w, http.StatusBadRequest, "missing_image_id", "image_id is required")
		return
	}

	sessionID := newSessionID()

	// TODO: image catalog lookup (ImageID -> OCI ref). For now the caller
	// passes the literal image reference as image_id, e.g.
	// "sandbox-oss/guest-agent:latest".
	sb, err := h.Mgr.Create(r.Context(), sandbox.CreateSpec{
		ID:                 sessionID,
		Image:              req.ImageID,
		Flavor:             req.Flavor,
		TenantID:           req.TenantID,
		Tags:               req.Tags,
		Environment:        req.Environment,
		NetworkEgress:      req.NetworkEgress,
		MaxSessionSeconds:  req.MaxSessionSeconds,
		IdleTimeoutSeconds: req.IdleTimeoutSeconds,
		IdleAction:         req.IdleAction,
		CallbackURL:        req.CallbackURL,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "create_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, CreateSandboxResponse{
		SessionID:          sb.ID,
		Status:             sb.Status,
		Deadline:           sb.Deadline,
		SessionEventStream: "/api/v1/sandboxes/" + sb.ID + "/events",
	})
}

// Get implements GET /api/v1/sandboxes/{id}. Returns 404 for sandboxes that
// are missing or already ended.
func (h *SandboxHandler) Get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing_id", "id is required")
		return
	}

	sb, err := h.Mgr.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, sandbox.ErrNotFound) {
			writeError(w, http.StatusNotFound, "sandbox_not_found", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "get_failed", err.Error())
		return
	}

	deadline := sb.Deadline
	writeJSON(w, http.StatusOK, GetSandboxResponse{
		SessionID: sb.ID,
		Status:    sb.Status,
		Flavor:    sb.Flavor,
		ImageID:   sb.Image,
		TenantID:  sb.TenantID,
		Tags:      sb.Tags,
		CreatedAt: sb.Created,
		Deadline:  &deadline,
	})
}

// Delete implements DELETE /api/v1/sandboxes/{id}. Idempotent: repeated
// deletes return 204. Exec records are cleared after the pod is gone.
func (h *SandboxHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing_id", "id is required")
		return
	}

	if err := h.Mgr.Delete(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, "delete_failed", err.Error())
		return
	}

	h.Registry.DeleteBySandbox(id)

	w.WriteHeader(http.StatusNoContent)
}

// newSessionID returns a "sb-" prefixed 12-hex-char random identifier.
func newSessionID() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return "sb-" + hex.EncodeToString(b[:])
}