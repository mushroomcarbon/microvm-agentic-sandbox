package api

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"adapter/internal/agent"
	"adapter/internal/exec"
	"adapter/internal/sandbox"
)

// ExecHandler holds the dependencies for the exec endpoints. Mgr is used
// to look up the sandbox's agent client; Registry is the in-memory store of
// exec records.
type ExecHandler struct {
	Mgr      *sandbox.Manager
	Registry *exec.Registry
}

// Create implements POST /api/v1/sandboxes/{id}/exec.
//
// For background=false (the default), this blocks until the exec completes or
// the timeout fires, then returns the full captured output inline. If the
// timeout fires, the response is HTTP 408 with the partial output in the same
// JSON shape as a success.
//
// For background=true, registers the exec, spawns a goroutine to drain the
// agent stream into the record, returns HTTP 202 with just the exec_id.
func (h *ExecHandler) Create(w http.ResponseWriter, r *http.Request) {
	sandboxID := r.PathValue("id")
	sb, err := h.Mgr.Get(r.Context(), sandboxID)
	if err != nil {
		if errors.Is(err, sandbox.ErrNotFound) {
			writeError(w, http.StatusNotFound, "sandbox_not_found", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "get_failed", err.Error())
		return
	}

	var req ExecRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "could not parse request body: "+err.Error())
		return
	}
	if strings.TrimSpace(req.Command) == "" {
		writeError(w, http.StatusBadRequest, "missing_command", "command is required")
		return
	}

	timeoutSec := req.TimeoutSeconds
	if timeoutSec == 0 {
		timeoutSec = 60
	}
	maxOut := req.MaxOutputBytes
	if maxOut == 0 {
		maxOut = 10 * 1024 * 1024 // 10 MiB default
	}

	execID := newExecID()

	// Record stores the raw command string as a single-element slice for the
	// ExecRecord.Command shape. Display joins with space, which for a
	// length-1 slice is just the string back.
	record := exec.NewExecRecord(
		execID, sandboxID,
		[]string{req.Command},
		req.Environment,
		req.CWD,
		req.Background,
		maxOut,
	)
	if err := h.Registry.Register(r.Context(), record); err != nil {
		writeError(w, http.StatusInternalServerError, "register_failed", err.Error())
		return
	}

	// Wrap the command with sh -c so shell features (pipes, redirects, env
	// interpolation, quoting) work without writing an argv parser.
	agentReq := agent.ExecRequest{
		ExecID:  execID,
		Cmd:     []string{"sh", "-c", req.Command},
		Env:     req.Environment,
		Workdir: req.CWD,
	}

	// Foreground execs tie their lifetime to the HTTP request and a hard
	// timeout. Background execs use a detached context so they survive the
	// HTTP response.
	var execCtx context.Context
	var cancel context.CancelFunc
	if req.Background {
		execCtx, cancel = context.WithCancel(context.Background())
	} else {
		execCtx, cancel = context.WithTimeout(r.Context(), time.Duration(timeoutSec)*time.Second)
	}

	chunks, err := sb.Agent.Exec(execCtx, agentReq)
	if err != nil {
		cancel()
		h.Registry.Errored(context.Background(), record, -1, fmt.Sprintf("agent exec start: %v", err))
		writeError(w, http.StatusInternalServerError, "exec_start_failed", err.Error())
		return
	}

	// Initial stdin goes via StdinWrite after the exec is registered on the
	// agent. There's a small race between the adapter receiving the stream
	// handle and the agent reaching its registerExec call, handled by
	// retrying NotFound for up to 500ms.
	if req.Stdin != "" {
		go writeInitialStdin(execCtx, sb.Agent, execID, []byte(req.Stdin))
	}

	if req.Background {
		go drainIntoRecord(chunks, h.Registry, record, cancel)
		writeJSON(w, http.StatusAccepted, ExecResponse{ExecID: execID})
		return
	}

	drainIntoRecord(chunks, h.Registry, record, cancel)

	stdout, stderr := record.Output()
	snap := record.Snapshot()
	exitCode := int(snap.ExitCode)
	resp := ExecResponse{
		ExecID:    execID,
		ExitCode:  &exitCode,
		Stdout:    string(stdout),
		Stderr:    string(stderr),
		Truncated: snap.Truncated,
	}
	if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
		writeJSON(w, http.StatusRequestTimeout, resp)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// Get implements GET /api/v1/sandboxes/{id}/exec/{exec_id}.
func (h *ExecHandler) Get(w http.ResponseWriter, r *http.Request) {
	sandboxID := r.PathValue("id")
	execID := r.PathValue("exec_id")
	rec, err := h.Registry.Get(r.Context(), execID)
	if err != nil {
		if errors.Is(err, exec.ErrExecNotFound) {
			writeError(w, http.StatusNotFound, "exec_not_found", "no exec with that id")
			return
		}
		writeError(w, http.StatusInternalServerError, "exec_get_failed", err.Error())
		return
	}
	snap := rec.Snapshot()
	if snap.SandboxID != sandboxID {
		// Don't leak the existence of execs under other sandboxes.
		writeError(w, http.StatusNotFound, "exec_not_found", "no exec with that id")
		return
	}
	writeJSON(w, http.StatusOK, execStatusFromSnapshot(snap))
}

// GetOutput implements GET /api/v1/sandboxes/{id}/exec/{exec_id}/output.
// Returns whatever output has been captured so far for running execs, or the
// final captured output for completed ones.
func (h *ExecHandler) GetOutput(w http.ResponseWriter, r *http.Request) {
	sandboxID := r.PathValue("id")
	execID := r.PathValue("exec_id")
	rec, err := h.Registry.Get(r.Context(), execID)
	if err != nil {
		if errors.Is(err, exec.ErrExecNotFound) {
			writeError(w, http.StatusNotFound, "exec_not_found", "no exec with that id")
			return
		}
		writeError(w, http.StatusInternalServerError, "exec_get_failed", err.Error())
		return
	}
	snap := rec.Snapshot()
	if snap.SandboxID != sandboxID {
		writeError(w, http.StatusNotFound, "exec_not_found", "no exec with that id")
		return
	}

	stdout, stderr := rec.Output()
	resp := ExecOutput{
		ExecID:    snap.ID,
		Status:    snap.Status.String(),
		Stdout:    string(stdout),
		Stderr:    string(stderr),
		Truncated: snap.Truncated,
	}
	if snap.Status != exec.StatusRunning {
		code := int(snap.ExitCode)
		resp.ExitCode = &code
	}
	writeJSON(w, http.StatusOK, resp)
}

// List implements GET /api/v1/sandboxes/{id}/exec.
func (h *ExecHandler) List(w http.ResponseWriter, r *http.Request) {
	sandboxID := r.PathValue("id")
	if _, err := h.Mgr.Get(r.Context(), sandboxID); err != nil {
		if errors.Is(err, sandbox.ErrNotFound) {
			writeError(w, http.StatusNotFound, "sandbox_not_found", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "get_failed", err.Error())
		return
	}
	records, err := h.Registry.List(r.Context(), sandboxID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "exec_list_failed", err.Error())
		return
	}
	items := make([]ExecListItem, 0, len(records))
	for _, rec := range records {
		snap := rec.Snapshot()
		item := ExecListItem{
			ExecID:    snap.ID,
			Command:   joinCommand(snap.Command),
			Status:    snap.Status.String(),
			StartedAt: snap.StartedAt,
		}
		if snap.Status != exec.StatusRunning {
			code := int(snap.ExitCode)
			item.ExitCode = &code
		}
		items = append(items, item)
	}
	writeJSON(w, http.StatusOK, ExecListResponse{Execs: items})
}

// Stdin implements POST /api/v1/sandboxes/{id}/exec/{exec_id}/stdin.
func (h *ExecHandler) Stdin(w http.ResponseWriter, r *http.Request) {
	sandboxID := r.PathValue("id")
	sb, err := h.Mgr.Get(r.Context(), sandboxID)
	if err != nil {
		if errors.Is(err, sandbox.ErrNotFound) {
			writeError(w, http.StatusNotFound, "sandbox_not_found", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "get_failed", err.Error())
		return
	}
	execID := r.PathValue("exec_id")
	rec, err := h.Registry.Get(r.Context(), execID)
	if err != nil {
		if errors.Is(err, exec.ErrExecNotFound) {
			writeError(w, http.StatusNotFound, "exec_not_found", "no exec with that id")
			return
		}
		writeError(w, http.StatusInternalServerError, "exec_get_failed", err.Error())
		return
	}
	if rec.Snapshot().SandboxID != sandboxID {
		writeError(w, http.StatusNotFound, "exec_not_found", "no exec with that id")
		return
	}

	var req StdinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	data, err := decodeStdinPayload(req.Data, req.Encoding)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_data", err.Error())
		return
	}

	if err := sb.Agent.StdinWrite(r.Context(), execID, data, req.EOF); err != nil {
		if isGRPCNotFound(err) {
			writeError(w, http.StatusNotFound, "exec_not_running", "exec is not currently running on the agent")
			return
		}
		writeError(w, http.StatusInternalServerError, "stdin_failed", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// Signal implements POST /api/v1/sandboxes/{id}/exec/{exec_id}/signal.
func (h *ExecHandler) Signal(w http.ResponseWriter, r *http.Request) {
	sandboxID := r.PathValue("id")
	sb, err := h.Mgr.Get(r.Context(), sandboxID)
	if err != nil {
		if errors.Is(err, sandbox.ErrNotFound) {
			writeError(w, http.StatusNotFound, "sandbox_not_found", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "get_failed", err.Error())
		return
	}
	execID := r.PathValue("exec_id")
	rec, err := h.Registry.Get(r.Context(), execID)
	if err != nil {
		if errors.Is(err, exec.ErrExecNotFound) {
			writeError(w, http.StatusNotFound, "exec_not_found", "no exec with that id")
			return
		}
		writeError(w, http.StatusInternalServerError, "exec_get_failed", err.Error())
		return
	}
	if rec.Snapshot().SandboxID != sandboxID {
		writeError(w, http.StatusNotFound, "exec_not_found", "no exec with that id")
		return
	}

	var req SignalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	signo, err := parseSignal(req.Signal)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_signal", err.Error())
		return
	}

	if err := sb.Agent.Signal(r.Context(), execID, signo); err != nil {
		if isGRPCNotFound(err) {
			writeError(w, http.StatusNotFound, "exec_not_running", "exec is not currently running on the agent")
			return
		}
		writeError(w, http.StatusInternalServerError, "signal_failed", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GetOutputStream implements GET /api/v1/sandboxes/{id}/exec/{exec_id}/output/stream
// as Server-Sent Events.
//
// Each chunk becomes one SSE event with id=<seq>, event=stdout|stderr|done,
// data=<JSON payload>. Reconnect support via the standard SSE Last-Event-ID
// header: the client passes the last seq it saw, the server resumes from
// seq+1 by calling Subscribe with that fromSeq. The terminal "done" event
// ends the stream; clients that miss it will reconnect and see it replayed.
//
// A comment heartbeat fires every 15s during quiet periods so idle-aware
// proxies don't cut the connection.
func (h *ExecHandler) GetOutputStream(w http.ResponseWriter, r *http.Request) {
	sandboxID := r.PathValue("id")
	execID := r.PathValue("exec_id")
	rec, err := h.Registry.Get(r.Context(), execID)
	if err != nil {
		if errors.Is(err, exec.ErrExecNotFound) {
			writeError(w, http.StatusNotFound, "exec_not_found", "no exec with that id")
			return
		}
		writeError(w, http.StatusInternalServerError, "exec_get_failed", err.Error())
		return
	}
	if rec.Snapshot().SandboxID != sandboxID {
		writeError(w, http.StatusNotFound, "exec_not_found", "no exec with that id")
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "no_flusher", "response writer doesn't support streaming")
		return
	}

	// Parse Last-Event-ID for resume. Empty or unparseable means "from seq 0".
	fromSeq := uint64(0)
	if lastID := r.Header.Get("Last-Event-ID"); lastID != "" {
		if n, err := strconv.ParseUint(lastID, 10, 64); err == nil {
			fromSeq = n + 1
		}
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // hint to nginx and friends not to buffer
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ch, cancel := rec.Subscribe(fromSeq)
	defer cancel()

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case chunk, ok := <-ch:
			if !ok {
				return
			}
			if err := writeSSEEvent(w, chunk); err != nil {
				log.Printf("sse write exec %s: %v", execID, err)
				return
			}
			flusher.Flush()
			if chunk.Kind == exec.ChunkDone {
				return
			}
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

// drainIntoRecord consumes the agent's chunk channel until it closes, routing
// each chunk into the ExecRecord. Calls cancel() on exit so the agent context
// is always released. Persists final state through the Registry so the row in
// the execs table reflects what happened. Uses context.Background() for the
// persist write so a cancelled HTTP request can't lose the final UPDATE.
// Marks the record errored if the stream closes without a terminal Done frame.
func drainIntoRecord(chunks <-chan agent.ExecChunk, registry *exec.Registry, record *exec.ExecRecord, cancel context.CancelFunc) {
	defer cancel()
	bgCtx := context.Background()
	gotDone := false
	for chunk := range chunks {
		if len(chunk.Stdout) > 0 {
			record.Append(exec.ChunkStdout, chunk.Stdout)
		}
		if len(chunk.Stderr) > 0 {
			record.Append(exec.ChunkStderr, chunk.Stderr)
		}
		if chunk.Done {
			registry.Complete(bgCtx, record, chunk.ExitCode)
			gotDone = true
		}
	}
	if !gotDone {
		registry.Errored(bgCtx, record, -1, "agent stream ended without Done frame")
	}
}

// writeInitialStdin retries StdinWrite for up to 500ms to handle the small
// race between the adapter calling agent.Exec and the agent reaching its
// registerExec. Sets eof=true so the initial stdin path always closes stdin
// after writing (the caller passed everything in the exec request body, so
// there's no more input coming).
func writeInitialStdin(ctx context.Context, ac *agent.Client, execID string, data []byte) {
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		err := ac.StdinWrite(ctx, execID, data, true)
		if err == nil {
			return
		}
		if !isGRPCNotFound(err) || time.Now().After(deadline) {
			log.Printf("exec %s initial stdin write: %v", execID, err)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// decodeStdinPayload turns the request's data+encoding into raw bytes.
func decodeStdinPayload(data, encoding string) ([]byte, error) {
	switch encoding {
	case "", "utf-8":
		return []byte(data), nil
	case "base64":
		out, err := base64.StdEncoding.DecodeString(data)
		if err != nil {
			return nil, fmt.Errorf("decode base64: %w", err)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("encoding must be utf-8 or base64, got %q", encoding)
	}
}

// parseSignal accepts a POSIX number ("15") or a common name ("SIGTERM").
func parseSignal(s string) (int32, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("signal is required")
	}
	if n, err := strconv.Atoi(s); err == nil {
		if n < 1 || n > 64 {
			return 0, fmt.Errorf("signal out of range: %d", n)
		}
		return int32(n), nil
	}
	switch strings.ToUpper(s) {
	case "SIGHUP", "HUP":
		return 1, nil
	case "SIGINT", "INT":
		return 2, nil
	case "SIGQUIT", "QUIT":
		return 3, nil
	case "SIGKILL", "KILL":
		return 9, nil
	case "SIGUSR1":
		return 10, nil
	case "SIGUSR2":
		return 12, nil
	case "SIGTERM", "TERM":
		return 15, nil
	}
	return 0, fmt.Errorf("unknown signal: %s", s)
}

// joinCommand renders an ExecRecord.Command slice for display. Records store
// the raw command as a single-element slice so this returns just the string.
func joinCommand(cmd []string) string {
	return strings.Join(cmd, " ")
}

// execStatusFromSnapshot builds the GET status response body.
func execStatusFromSnapshot(snap exec.Snapshot) ExecStatus {
	out := ExecStatus{
		ExecID:      snap.ID,
		SandboxID:   snap.SandboxID,
		Command:     joinCommand(snap.Command),
		Status:      snap.Status.String(),
		StartedAt:   snap.StartedAt,
		OutputBytes: snap.OutputBytes,
		Truncated:   snap.Truncated,
	}
	if snap.Status != exec.StatusRunning {
		code := int(snap.ExitCode)
		out.ExitCode = &code
		ct := snap.CompletedAt
		out.CompletedAt = &ct
	}
	return out
}

// newExecID returns an "ex-" prefixed 16-hex-char identifier.
func newExecID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "ex-" + hex.EncodeToString(b[:])
}

// isGRPCNotFound returns true if err is a gRPC NotFound status, used to map
// agent-side "exec not registered" back to HTTP 404.
func isGRPCNotFound(err error) bool {
	s, ok := status.FromError(err)
	return ok && s.Code() == codes.NotFound
}

// sseDataPayload is the JSON body of "stdout" and "stderr" SSE events. Bytes
// are base64-encoded since process output may contain binary that isn't safe
// to embed raw in JSON.
type sseDataPayload struct {
	Data string `json:"data"`
}

// sseDonePayload is the JSON body of the terminal "done" SSE event.
type sseDonePayload struct {
	ExitCode int `json:"exit_code"`
}

// writeSSEEvent serializes one Chunk as a single SSE event with id, event,
// and data lines. Doesn't flush; the caller flushes after each event so the
// bytes hit the wire promptly.
func writeSSEEvent(w http.ResponseWriter, chunk exec.Chunk) error {
	var eventName string
	var payload any
	switch chunk.Kind {
	case exec.ChunkStdout:
		eventName = "stdout"
		payload = sseDataPayload{Data: base64.StdEncoding.EncodeToString(chunk.Data)}
	case exec.ChunkStderr:
		eventName = "stderr"
		payload = sseDataPayload{Data: base64.StdEncoding.EncodeToString(chunk.Data)}
	case exec.ChunkDone:
		eventName = "done"
		payload = sseDonePayload{ExitCode: int(chunk.ExitCode)}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: %s\nid: %d\ndata: %s\n\n", eventName, chunk.Seq, body)
	return err
}