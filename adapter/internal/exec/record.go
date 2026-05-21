package exec

import (
	"context"
	"sync"
	"time"
)

// MaxOutputBytesHard is the adapter-enforced upper bound on captured output per
// exec regardless of the caller's max_output_bytes. Prevents one runaway
// customer from making the adapter hold gigabytes of process output.
const MaxOutputBytesHard = 100 * 1024 * 1024 // 100 MiB

// ChunkKind distinguishes stdout, stderr, and the terminal "done" marker.
type ChunkKind int

const (
	ChunkStdout ChunkKind = iota
	ChunkStderr
	ChunkDone
)

func (k ChunkKind) String() string {
	switch k {
	case ChunkStdout:
		return "stdout"
	case ChunkStderr:
		return "stderr"
	case ChunkDone:
		return "done"
	default:
		return "unknown"
	}
}

// Chunk is a single piece of exec output (or the terminal marker). Seq is a
// monotonic sequence number unique within an ExecRecord, used by SSE
// subscribers to resume from a Last-Event-ID after reconnect.
type Chunk struct {
	Seq      uint64
	Kind     ChunkKind
	Data     []byte
	ExitCode int32 // populated only when Kind == ChunkDone
}

// Status is the lifecycle state of an exec.
type Status int

const (
	StatusRunning Status = iota
	StatusCompleted
	StatusErrored
)

func (s Status) String() string {
	switch s {
	case StatusRunning:
		return "running"
	case StatusCompleted:
		return "completed"
	case StatusErrored:
		return "errored"
	default:
		return "unknown"
	}
}

// ExecRecord captures everything we know about a single exec: request inputs,
// lifecycle state, captured output history with sequence numbers, and live
// subscribers for SSE fan-out.
type ExecRecord struct {
	// Immutable identity and request shape.
	ID             string
	SandboxID      string
	Command        []string
	Env            map[string]string
	CWD            string
	Background     bool
	MaxOutputBytes int64

	mu            sync.RWMutex
	status        Status
	exitCode      int32
	startedAt     time.Time
	completedAt   time.Time
	chunks        []Chunk
	nextSeq       uint64
	outputBytes   int64
	truncated     bool
	subscribers   map[chan<- Chunk]struct{}
	completionErr string
}

// NewExecRecord returns a fresh record in the Running state with no chunks.
// maxOutputBytes is clamped to MaxOutputBytesHard.
func NewExecRecord(id, sandboxID string, cmd []string, env map[string]string, cwd string, background bool, maxOutputBytes int64) *ExecRecord {
	if maxOutputBytes <= 0 || maxOutputBytes > MaxOutputBytesHard {
		maxOutputBytes = MaxOutputBytesHard
	}
	return &ExecRecord{
		ID:             id,
		SandboxID:      sandboxID,
		Command:        cmd,
		Env:            env,
		CWD:            cwd,
		Background:     background,
		MaxOutputBytes: maxOutputBytes,
		status:         StatusRunning,
		startedAt:      time.Now(),
		subscribers:    make(map[chan<- Chunk]struct{}),
	}
}

// NewExecRecordFromDB reconstructs a record from persisted fields. Used by
// the registry when an exec is queried after its in-memory entry was evicted
// (typically after an adapter restart). The stdout and stderr blobs are loaded
// as two chunks (or zero of either if empty) plus a terminal done chunk, so
// Output(), Snapshot(), and Subscribe() all return consistent results.
//
// If status is Running on load, no done chunk is appended; that case is only
// expected pre-Reconcile, which marks orphaned running execs as errored.
func NewExecRecordFromDB(
	id, sandboxID, command, cwd string,
	env map[string]string,
	background bool,
	maxOutputBytes int64,
	status Status,
	exitCode int32,
	stdout, stderr []byte,
	truncated bool,
	completionErr string,
	startedAt, completedAt time.Time,
) *ExecRecord {
	r := &ExecRecord{
		ID:             id,
		SandboxID:      sandboxID,
		Command:        []string{command},
		Env:            env,
		CWD:            cwd,
		Background:     background,
		MaxOutputBytes: maxOutputBytes,
		status:         status,
		exitCode:       exitCode,
		startedAt:      startedAt,
		completedAt:    completedAt,
		truncated:      truncated,
		completionErr:  completionErr,
		subscribers:    make(map[chan<- Chunk]struct{}),
	}
	var seq uint64
	if len(stdout) > 0 {
		r.chunks = append(r.chunks, Chunk{Seq: seq, Kind: ChunkStdout, Data: stdout})
		seq++
		r.outputBytes += int64(len(stdout))
	}
	if len(stderr) > 0 {
		r.chunks = append(r.chunks, Chunk{Seq: seq, Kind: ChunkStderr, Data: stderr})
		seq++
		r.outputBytes += int64(len(stderr))
	}
	if status != StatusRunning {
		r.chunks = append(r.chunks, Chunk{Seq: seq, Kind: ChunkDone, ExitCode: exitCode})
		seq++
	}
	r.nextSeq = seq
	return r
}

// Append records a new output chunk, assigns a sequence number, applies the
// output cap, and fans the chunk out to all live subscribers. Subscribers with
// full channels are dropped (the chunk is skipped for them); they should
// reconnect with Last-Event-ID to resume.
//
// Append is safe to call from a single producer goroutine (the agent stream
// pump). Concurrent producers would interleave seq numbers, which would still
// be correct but the chunk order would be undefined.
func (r *ExecRecord) Append(kind ChunkKind, data []byte) {
	r.mu.Lock()

	// Apply the per-exec cap. Past the cap, drop the chunk entirely and flag
	// the record as truncated. We do not stop the process; the agent keeps
	// emitting and we keep dropping silently.
	remaining := r.MaxOutputBytes - r.outputBytes
	if remaining <= 0 {
		r.truncated = true
		r.mu.Unlock()
		return
	}
	if int64(len(data)) > remaining {
		data = data[:remaining]
		r.truncated = true
	}

	// Copy the data since the caller (e.g. a gRPC stream pump) may reuse its
	// buffer for the next read.
	payload := append([]byte(nil), data...)

	chunk := Chunk{
		Seq:  r.nextSeq,
		Kind: kind,
		Data: payload,
	}
	r.nextSeq++
	r.chunks = append(r.chunks, chunk)
	r.outputBytes += int64(len(payload))

	// Snapshot subscriber set under the lock then fan out without it, so a
	// slow subscriber's full channel doesn't block other subscribers or
	// future Appends.
	subs := make([]chan<- Chunk, 0, len(r.subscribers))
	for ch := range r.subscribers {
		subs = append(subs, ch)
	}
	r.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- chunk:
		default:
			// Subscriber buffer full; drop. They'll reconnect with the last
			// seq they did receive and we replay from there.
		}
	}
}

// Complete marks the exec as finished with the given exit code and emits a
// final ChunkDone to subscribers. Idempotent: calls after the first are no-ops.
func (r *ExecRecord) Complete(exitCode int32) {
	r.finish(StatusCompleted, exitCode, "")
}

// Errored marks the exec as failed and emits a final ChunkDone with the given
// exit code (use -1 if no real exit code was captured). reason is recorded on
// the record for debugging but not exposed in ChunkDone.
func (r *ExecRecord) Errored(exitCode int32, reason string) {
	r.finish(StatusErrored, exitCode, reason)
}

func (r *ExecRecord) finish(status Status, exitCode int32, reason string) {
	r.mu.Lock()
	if r.status != StatusRunning {
		r.mu.Unlock()
		return
	}
	r.status = status
	r.exitCode = exitCode
	r.completedAt = time.Now()
	r.completionErr = reason

	doneChunk := Chunk{
		Seq:      r.nextSeq,
		Kind:     ChunkDone,
		ExitCode: exitCode,
	}
	r.nextSeq++
	r.chunks = append(r.chunks, doneChunk)

	subs := make([]chan<- Chunk, 0, len(r.subscribers))
	for ch := range r.subscribers {
		subs = append(subs, ch)
	}
	r.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- doneChunk:
		default:
		}
	}
}

// Subscribe returns a channel that first replays all chunks with Seq >= fromSeq
// then streams new chunks as they arrive. The returned cancel function removes
// the subscription and closes the channel; callers must invoke it (typically
// via defer) when done.
//
// If the exec is already completed when Subscribe is called, the replay
// includes the terminal ChunkDone and the channel is closed before return.
//
// Replay is performed under the record's lock to guarantee that no live
// Append can interleave with the replay, preserving strict seq ordering on
// the subscriber's channel. Non-blocking sends during replay mean a slow
// subscriber may miss some replay chunks, in which case they should reconnect
// using the last seq they actually received.
func (r *ExecRecord) Subscribe(fromSeq uint64) (<-chan Chunk, func()) {
	ch := make(chan Chunk, 64)

	r.mu.Lock()
	for _, c := range r.chunks {
		if c.Seq < fromSeq {
			continue
		}
		select {
		case ch <- c:
		default:
			// Subscriber buffer overflowed during replay. They'll reconnect
			// with the last successfully-delivered seq and we replay from
			// there.
		}
	}

	stillRunning := r.status == StatusRunning
	if stillRunning {
		r.subscribers[ch] = struct{}{}
	}
	r.mu.Unlock()

	if !stillRunning {
		close(ch)
		return ch, func() {} // already closed; cancel is a no-op
	}

	cancel := func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if _, ok := r.subscribers[ch]; ok {
			delete(r.subscribers, ch)
			close(ch)
		}
	}
	return ch, cancel
}

// Snapshot is a point-in-time view of record metadata, safe to marshal to JSON
// without holding the record lock.
type Snapshot struct {
	ID             string
	SandboxID      string
	Command        []string
	Env            map[string]string
	CWD            string
	Background     bool
	Status         Status
	ExitCode       int32
	StartedAt      time.Time
	CompletedAt    time.Time
	OutputBytes    int64
	Truncated      bool
	MaxOutputBytes int64
	NextSeq        uint64
	CompletionErr  string
}

// Snapshot returns the record's metadata at the moment of the call.
func (r *ExecRecord) Snapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return Snapshot{
		ID:             r.ID,
		SandboxID:      r.SandboxID,
		Command:        r.Command,
		Env:            r.Env,
		CWD:            r.CWD,
		Background:     r.Background,
		Status:         r.status,
		ExitCode:       r.exitCode,
		StartedAt:      r.startedAt,
		CompletedAt:    r.completedAt,
		OutputBytes:    r.outputBytes,
		Truncated:      r.truncated,
		MaxOutputBytes: r.MaxOutputBytes,
		NextSeq:        r.nextSeq,
		CompletionErr:  r.completionErr,
	}
}

// Output returns the full captured stdout and stderr buffers concatenated in
// chunk order. Convenient for foreground exec returns and the GET output
// endpoint after the exec has finished.
func (r *ExecRecord) Output() (stdout, stderr []byte) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, c := range r.chunks {
		switch c.Kind {
		case ChunkStdout:
			stdout = append(stdout, c.Data...)
		case ChunkStderr:
			stderr = append(stderr, c.Data...)
		}
	}
	return
}

// Wait blocks until the exec completes (via Complete or Errored) or ctx is
// cancelled. Returns the final status and exit code. Convenient for the
// foreground exec handler which blocks the HTTP response on completion.
func (r *ExecRecord) Wait(ctx context.Context) (Status, int32, error) {
	ch, cancel := r.Subscribe(0)
	defer cancel()
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				// Channel closed in Subscribe because exec was already done.
				snap := r.Snapshot()
				return snap.Status, snap.ExitCode, nil
			}
			if c.Kind == ChunkDone {
				snap := r.Snapshot()
				return snap.Status, snap.ExitCode, nil
			}
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		}
	}
}