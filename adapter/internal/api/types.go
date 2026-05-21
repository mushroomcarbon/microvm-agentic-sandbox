package api

import (
	"time"
)

// CreateSandboxRequest is the body for POST /sandboxes.
type CreateSandboxRequest struct {
	Flavor   string            `json:"flavor"`
	ImageID  string            `json:"image_id"`
	TenantID string            `json:"tenant_id,omitempty"`
	Tags     map[string]string `json:"tags,omitempty"`

	MaxSessionSeconds  int    `json:"max_session_seconds,omitempty"`
	IdleTimeoutSeconds int    `json:"idle_timeout_seconds,omitempty"`
	IdleAction         string `json:"idle_action,omitempty"` // "pause" or "kill", default "pause"

	Environment     map[string]string `json:"environment,omitempty"`
	NetworkEgress   string            `json:"network_egress,omitempty"` // "none", "limited", "full"
	EgressAllowlist []string          `json:"egress_allowlist,omitempty"`

	Setup *SetupBlock `json:"setup,omitempty"`

	CallbackURL string `json:"callback_url,omitempty"`
}

// SetupBlock is the optional one-shot initialization phase run before the sandbox goes running.
type SetupBlock struct {
	Files    []SetupFile `json:"files,omitempty"`
	Commands []string    `json:"commands,omitempty"`
}

// SetupFile is a single file written into the guest before the sandbox
// transitions to running.
type SetupFile struct {
	Path     string `json:"path"`
	Content  string `json:"content"`
	Encoding string `json:"encoding,omitempty"` // "utf-8" (default) or "base64"
}

// CreateSandboxResponse is the body of a successful POST /sandboxes.
type CreateSandboxResponse struct {
	SessionID          string    `json:"session_id"`
	Status             string    `json:"status"`
	Deadline           time.Time `json:"deadline"`
	SessionEventStream string    `json:"session_event_stream"`
}

// GetSandboxResponse is the body of a successful GET /sandboxes/{id}.
type GetSandboxResponse struct {
	SessionID string            `json:"session_id"`
	Status    string            `json:"status"`
	Flavor    string            `json:"flavor,omitempty"`
	ImageID   string            `json:"image_id,omitempty"`
	TenantID  string            `json:"tenant_id,omitempty"`
	Tags      map[string]string `json:"tags,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	Deadline  *time.Time        `json:"deadline,omitempty"`
}

// ExecRequest is the body for POST /sandboxes/{id}/exec.
type ExecRequest struct {
	Command        string            `json:"command"`
	CWD            string            `json:"cwd,omitempty"`
	Environment    map[string]string `json:"environment,omitempty"`
	Stdin          string            `json:"stdin,omitempty"`
	Background     bool              `json:"background,omitempty"`
	TimeoutSeconds int               `json:"timeout_seconds,omitempty"`
	MaxOutputBytes int64             `json:"max_output_bytes,omitempty"`
	StreamOutput   bool              `json:"stream_output,omitempty"`
}

// ExecResponse is the body of POST /sandboxes/{id}/exec. Foreground returns
// have ExitCode + Stdout + Stderr populated; background returns carry just
// ExecID and the caller polls or subscribes for the rest.
type ExecResponse struct {
	ExecID    string `json:"exec_id"`
	ExitCode  *int   `json:"exit_code,omitempty"`
	Stdout    string `json:"stdout,omitempty"`
	Stderr    string `json:"stderr,omitempty"`
	Truncated bool   `json:"truncated,omitempty"`
}

// ExecStatus is the body of GET /sandboxes/{id}/exec/{exec_id}.
type ExecStatus struct {
	ExecID      string     `json:"exec_id"`
	SandboxID   string     `json:"sandbox_id"`
	Command     string     `json:"command"`
	Status      string     `json:"status"`
	ExitCode    *int       `json:"exit_code,omitempty"`
	StartedAt   time.Time  `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	OutputBytes int64      `json:"output_bytes"`
	Truncated   bool       `json:"truncated,omitempty"`
}

// ExecOutput is the body of GET /sandboxes/{id}/exec/{exec_id}/output.
type ExecOutput struct {
	ExecID    string `json:"exec_id"`
	Status    string `json:"status"`
	ExitCode  *int   `json:"exit_code,omitempty"`
	Stdout    string `json:"stdout"`
	Stderr    string `json:"stderr"`
	Truncated bool   `json:"truncated,omitempty"`
}

// ExecListResponse is the body of GET /sandboxes/{id}/exec.
type ExecListResponse struct {
	Execs []ExecListItem `json:"execs"`
}

// ExecListItem is one row in the exec list.
type ExecListItem struct {
	ExecID    string    `json:"exec_id"`
	Command   string    `json:"command"`
	Status    string    `json:"status"`
	ExitCode  *int      `json:"exit_code,omitempty"`
	StartedAt time.Time `json:"started_at"`
}

// StdinRequest is the body of POST /sandboxes/{id}/exec/{exec_id}/stdin.
type StdinRequest struct {
	Data     string `json:"data"`
	Encoding string `json:"encoding,omitempty"` // "utf-8" (default) or "base64"
	EOF      bool   `json:"eof,omitempty"`      // close stdin after writing data
}

// SignalRequest is the body of POST /sandboxes/{id}/exec/{exec_id}/signal.
type SignalRequest struct {
	Signal string `json:"signal"` // POSIX number ("15") or name ("SIGTERM")
}

// APIError is the standard error envelope for every non-2xx response.
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}