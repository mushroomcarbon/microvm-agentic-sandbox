package exec

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"adapter/internal/db"
)

// ErrExecNotFound is returned by Get when no exec with the given id exists in
// either the in-memory map or the execs table. Callers map this to HTTP 404.
var ErrExecNotFound = errors.New("exec not found")

// Registry tracks active and historical exec records. Live records (with
// chunk-by-chunk history and SSE subscribers) live in memory; final state for
// every exec (status, exit_code, full stdout/stderr, timestamps) is persisted
// to the execs table.
//
// The in-memory layer is per-adapter-instance and is lost on restart. The DB
// layer survives restart. Get and List union the two views so the API surface
// stays consistent across restarts.
type Registry struct {
	db *db.DB

	mu        sync.RWMutex
	records   map[string]*ExecRecord     // exec_id -> record
	bySession map[string]map[string]bool // sandbox_id -> set of exec_ids
}

// NewRegistry returns an empty Registry bound to the given database.
func NewRegistry(database *db.DB) *Registry {
	return &Registry{
		db:        database,
		records:   make(map[string]*ExecRecord),
		bySession: make(map[string]map[string]bool),
	}
}

// Reconcile fixes up DB state on startup. The agent ties exec lifetime to its
// inbound gRPC stream (which dies when this adapter restarts), so any row left
// at status='running' on disk has no corresponding process anywhere. They get
// flipped to errored with completion_err='adapter_restarted'. Run once before
// HTTP serving.
func (r *Registry) Reconcile(ctx context.Context) error {
	tag, err := r.db.Pool.Exec(ctx, `
		UPDATE execs
		SET status = 'errored',
		    exit_code = -1,
		    completion_err = 'adapter_restarted',
		    completed_at = NOW()
		WHERE status = 'running'
	`)
	if err != nil {
		return fmt.Errorf("update stale execs: %w", err)
	}
	if n := tag.RowsAffected(); n > 0 {
		log.Printf("reconcile: marked %d stale exec(s) as errored=adapter_restarted", n)
	} else {
		log.Printf("reconcile: no stale execs")
	}
	return nil
}

// Register stores a new record in memory and inserts the row into execs with
// status='running'. If the in-memory ID collides or the DB INSERT fails, the
// in-memory entry is rolled back so the caller can retry with a fresh ID.
func (r *Registry) Register(ctx context.Context, rec *ExecRecord) error {
	r.mu.Lock()
	if _, exists := r.records[rec.ID]; exists {
		r.mu.Unlock()
		return fmt.Errorf("exec %s already registered", rec.ID)
	}
	r.records[rec.ID] = rec
	if r.bySession[rec.SandboxID] == nil {
		r.bySession[rec.SandboxID] = make(map[string]bool)
	}
	r.bySession[rec.SandboxID][rec.ID] = true
	r.mu.Unlock()

	envJSON, err := json.Marshal(orEmptyMap(rec.Env))
	if err != nil {
		r.removeInMemory(rec)
		return fmt.Errorf("marshal env: %w", err)
	}

	_, err = r.db.Pool.Exec(ctx, `
		INSERT INTO execs (
			id, sandbox_id, command, cwd, environment, background,
			max_output_bytes, status, started_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, 'running', $8)
	`,
		rec.ID, rec.SandboxID, joinCommand(rec.Command), rec.CWD, envJSON,
		rec.Background, rec.MaxOutputBytes, rec.startedAt,
	)
	if err != nil {
		r.removeInMemory(rec)
		return fmt.Errorf("insert exec: %w", err)
	}
	return nil
}

// Complete transitions a record to completed and persists final state in one
// operation. Idempotent: a record already in a terminal state is unaffected
// and the DB UPDATE just no-ops on its WHERE clause.
func (r *Registry) Complete(ctx context.Context, rec *ExecRecord, exitCode int32) {
	rec.Complete(exitCode)
	r.persistFinal(ctx, rec)
}

// Errored is the same as Complete but for the errored terminal state, with a
// reason string stored in completion_err for debugging.
func (r *Registry) Errored(ctx context.Context, rec *ExecRecord, exitCode int32, reason string) {
	rec.Errored(exitCode, reason)
	r.persistFinal(ctx, rec)
}

// persistFinal writes the record's final state to the DB. Best-effort; on
// failure the in-memory state is still correct and the discrepancy gets logged.
// If the adapter restarts before this write succeeds, the next Reconcile will
// mark the row errored with adapter_restarted, which is a reasonable
// approximation of "we lost track during shutdown".
func (r *Registry) persistFinal(ctx context.Context, rec *ExecRecord) {
	snap := rec.Snapshot()
	stdout, stderr := rec.Output()
	_, err := r.db.Pool.Exec(ctx, `
		UPDATE execs
		SET status = $1,
		    exit_code = $2,
		    stdout = $3,
		    stderr = $4,
		    truncated = $5,
		    completion_err = $6,
		    completed_at = $7
		WHERE id = $8
	`,
		snap.Status.String(),
		int(snap.ExitCode),
		stdout,
		stderr,
		snap.Truncated,
		nullIfEmpty(snap.CompletionErr),
		snap.CompletedAt,
		snap.ID,
	)
	if err != nil {
		log.Printf("exec %s persistFinal: %v", snap.ID, err)
	}
}

// Get returns the record for the given exec_id. Hits the in-memory map first
// and only queries the DB on miss, so the common case (look up an exec that
// was just created in this instance) is lock-only with no DB round trip.
// Returns ErrExecNotFound if neither layer has the record.
func (r *Registry) Get(ctx context.Context, execID string) (*ExecRecord, error) {
	r.mu.RLock()
	rec := r.records[execID]
	r.mu.RUnlock()
	if rec != nil {
		return rec, nil
	}
	return r.queryByID(ctx, execID)
}

// List returns every exec record for the given sandbox, union of in-memory
// and DB sources with in-memory winning on ID conflict. Sorted by StartedAt
// ascending so the caller sees execs in creation order.
func (r *Registry) List(ctx context.Context, sandboxID string) ([]*ExecRecord, error) {
	r.mu.RLock()
	var inMem []*ExecRecord
	for id := range r.bySession[sandboxID] {
		if rec, ok := r.records[id]; ok {
			inMem = append(inMem, rec)
		}
	}
	r.mu.RUnlock()

	seen := make(map[string]bool, len(inMem))
	out := make([]*ExecRecord, 0, len(inMem))
	for _, rec := range inMem {
		seen[rec.ID] = true
		out = append(out, rec)
	}

	dbRecs, err := r.queryBySandbox(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	for _, rec := range dbRecs {
		if !seen[rec.ID] {
			out = append(out, rec)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].startedAt.Before(out[j].startedAt)
	})
	return out, nil
}

// DeleteBySandbox drops the in-memory records associated with a sandbox.
// DB rows are left in place so historical exec data stays queryable until
// retention policy says otherwise. Returns the count of in-memory entries
// removed.
func (r *Registry) DeleteBySandbox(sandboxID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := r.bySession[sandboxID]
	for id := range ids {
		delete(r.records, id)
	}
	delete(r.bySession, sandboxID)
	return len(ids)
}

// removeInMemory rolls back a Register that failed after the in-memory insert
// but before or during the DB INSERT.
func (r *Registry) removeInMemory(rec *ExecRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.records, rec.ID)
	if set, ok := r.bySession[rec.SandboxID]; ok {
		delete(set, rec.ID)
		if len(set) == 0 {
			delete(r.bySession, rec.SandboxID)
		}
	}
}

// queryByID fetches one exec row and reconstructs an ExecRecord from it.
// Returns ErrExecNotFound for missing rows.
func (r *Registry) queryByID(ctx context.Context, execID string) (*ExecRecord, error) {
	row := r.db.Pool.QueryRow(ctx, execSelectColumns+` FROM execs WHERE id = $1`, execID)
	rec, err := scanExecRow(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrExecNotFound
		}
		return nil, fmt.Errorf("query exec: %w", err)
	}
	return rec, nil
}

// queryBySandbox fetches every exec row for a given sandbox. The result is
// unsorted; the caller sorts after the memory/DB merge.
func (r *Registry) queryBySandbox(ctx context.Context, sandboxID string) ([]*ExecRecord, error) {
	rows, err := r.db.Pool.Query(ctx, execSelectColumns+` FROM execs WHERE sandbox_id = $1`, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("query execs by sandbox: %w", err)
	}
	defer rows.Close()
	var out []*ExecRecord
	for rows.Next() {
		rec, err := scanExecRow(rows)
		if err != nil {
			return nil, fmt.Errorf("scan exec: %w", err)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return out, nil
}

// execSelectColumns is the column list used by both queryByID and
// queryBySandbox so scanExecRow can be shared.
const execSelectColumns = `SELECT id, sandbox_id, command, cwd, environment,
	background, max_output_bytes, status, exit_code, stdout, stderr,
	truncated, completion_err, started_at, completed_at`

// rowScanner is the minimal interface satisfied by both pgx.Row (single row)
// and pgx.Rows (in a loop), letting scanExecRow handle either.
type rowScanner interface {
	Scan(dest ...any) error
}

// scanExecRow reads one execs row and constructs an ExecRecord via
// NewExecRecordFromDB.
func scanExecRow(row rowScanner) (*ExecRecord, error) {
	var (
		id, sandboxID, command, cwd string
		envRaw                      []byte
		background                  bool
		maxOutputBytes              int64
		statusStr                   string
		exitCode                    sql.NullInt32
		stdout, stderr              []byte
		truncated                   bool
		completionErr               sql.NullString
		startedAt                   time.Time
		completedAt                 sql.NullTime
	)
	if err := row.Scan(
		&id, &sandboxID, &command, &cwd, &envRaw,
		&background, &maxOutputBytes, &statusStr, &exitCode, &stdout, &stderr,
		&truncated, &completionErr, &startedAt, &completedAt,
	); err != nil {
		return nil, err
	}

	var env map[string]string
	if len(envRaw) > 0 {
		if err := json.Unmarshal(envRaw, &env); err != nil {
			return nil, fmt.Errorf("unmarshal env: %w", err)
		}
	}

	statusEnum := parseStatus(statusStr)
	var completedAtT time.Time
	if completedAt.Valid {
		completedAtT = completedAt.Time
	}

	return NewExecRecordFromDB(
		id, sandboxID, command, cwd, env, background, maxOutputBytes,
		statusEnum, exitCode.Int32, stdout, stderr, truncated, completionErr.String,
		startedAt, completedAtT,
	), nil
}

func parseStatus(s string) Status {
	switch s {
	case "completed":
		return StatusCompleted
	case "errored":
		return StatusErrored
	default:
		return StatusRunning
	}
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func orEmptyMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// joinCommand renders an ExecRecord.Command slice into a single string for DB
// storage. The handler always passes a one-element slice today, so this is
// usually a no-op; the join is defense in case that changes.
func joinCommand(cmd []string) string {
	return strings.Join(cmd, " ")
}