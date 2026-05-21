package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"adapter/internal/agent"
	"adapter/internal/db"
	"adapter/internal/k8s"
)

// ErrNotFound is returned by Get when the sandbox row is missing or already
// ended. Handlers should map this to HTTP 404.
var ErrNotFound = errors.New("sandbox not found")

// Sandbox is the runtime view of a sandbox session. Every field except Agent is
// persisted in the sandboxes table. Agent is the dialed gRPC client for the
// guest agent inside the pod, populated when the sandbox is alive in this
// adapter instance and nil otherwise.
type Sandbox struct {
	ID                 string
	PodName            string
	PodIP              string
	Namespace          string
	Image              string
	Flavor             string
	TenantID           string
	Tags               map[string]string
	Environment        map[string]string
	NetworkEgress      string
	MaxSessionSeconds  int
	IdleTimeoutSeconds int
	IdleAction         string
	CallbackURL        string
	Status             string
	EndCause           string
	Created            time.Time
	Deadline           time.Time
	EndedAt            *time.Time

	Agent *agent.Client
}

// CreateSpec carries everything the manager needs to bring a new sandbox up
// and persist its initial row.
type CreateSpec struct {
	ID                 string
	Image              string
	Flavor             string
	TenantID           string
	Tags               map[string]string
	Environment        map[string]string
	NetworkEgress      string
	MaxSessionSeconds  int
	IdleTimeoutSeconds int
	IdleAction         string
	CallbackURL        string
}

// Manager owns sandbox lifecycle. State is persisted in Postgres; the dialed
// gRPC client for each live sandbox is kept in an in-memory map since
// connections can't survive restarts anyway.
type Manager struct {
	db  *db.DB
	k8s *k8s.Client

	agentsMu sync.RWMutex
	agents   map[string]*agent.Client
}

// NewManager wires the manager. Call Reconcile after construction to rediscover
// any sandboxes left over from a previous adapter instance.
func NewManager(database *db.DB, k8sClient *k8s.Client) *Manager {
	return &Manager{
		db:     database,
		k8s:    k8sClient,
		agents: make(map[string]*agent.Client),
	}
}

// Reconcile walks every sandbox row recorded as running, looks up the matching
// pod in the cluster, and either redials the agent (if the pod is back) or
// marks the sandbox ended with host_failed (if the pod is gone). Pods that
// exist but aren't ready yet are left alone so a subsequent check can pick
// them up. Per-sandbox errors are logged and don't fail the overall pass; the
// only fatal case is the initial DB query failing.
//
// TODO: also list pods with the sandbox-oss/managed label whose sandbox row is
// missing or ended, and delete them as orphans. Needs a List method on
// k8s.Client first.
func (m *Manager) Reconcile(ctx context.Context) error {
	rows, err := m.db.Pool.Query(ctx, `
		SELECT id, pod_name, pod_ip, namespace
		FROM sandboxes
		WHERE status = 'running'
	`)
	if err != nil {
		return fmt.Errorf("query running sandboxes: %w", err)
	}
	defer rows.Close()

	type stub struct {
		id, podName, podIP, namespace string
	}
	var stubs []stub
	for rows.Next() {
		var s stub
		var podIP sql.NullString
		if err := rows.Scan(&s.id, &s.podName, &podIP, &s.namespace); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		s.podIP = podIP.String
		stubs = append(stubs, s)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows: %w", err)
	}

	if len(stubs) == 0 {
		log.Printf("reconcile: no running sandboxes")
		return nil
	}
	log.Printf("reconcile: %d running sandboxes to recheck", len(stubs))

	for _, s := range stubs {
		pod, err := m.k8s.GetSandboxPod(ctx, s.id)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				if err := m.markEnded(ctx, s.id, "host_failed"); err != nil {
					log.Printf("reconcile: sandbox %s mark ended: %v", s.id, err)
				} else {
					log.Printf("reconcile: sandbox %s pod gone, marked host_failed", s.id)
				}
				continue
			}
			log.Printf("reconcile: sandbox %s GetPod: %v", s.id, err)
			continue
		}
		if pod.Status.Phase != "Running" || pod.Status.PodIP == "" {
			log.Printf("reconcile: sandbox %s pod phase=%s, leaving for later", s.id, pod.Status.Phase)
			continue
		}
		ac, err := agent.Dial(pod.Status.PodIP + ":50051")
		if err != nil {
			log.Printf("reconcile: sandbox %s dial: %v", s.id, err)
			continue
		}
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = ac.Ping(pingCtx)
		cancel()
		if err != nil {
			log.Printf("reconcile: sandbox %s ping: %v", s.id, err)
			_ = ac.Close()
			continue
		}
		m.agentsMu.Lock()
		m.agents[s.id] = ac
		m.agentsMu.Unlock()
		log.Printf("reconcile: sandbox %s reattached at %s", s.id, pod.Status.PodIP)
	}
	return nil
}

// Create brings up a Kata pod for the sandbox, dials the guest agent, verifies
// it's responding, and writes the sandbox row to Postgres. If anything fails
// before the INSERT, the pod (if created) is deleted. The INSERT is the last
// step so on success there's no partial state to clean up. If the INSERT
// itself fails, the pod is best-effort deleted so we don't leak it.
func (m *Manager) Create(ctx context.Context, spec CreateSpec) (*Sandbox, error) {
	pod, err := m.k8s.CreateSandboxPod(ctx, spec.ID, spec.Image)
	if err != nil {
		return nil, fmt.Errorf("create pod: %w", err)
	}

	ip, err := m.waitForPodReady(ctx, spec.ID)
	if err != nil {
		_ = m.k8s.DeleteSandboxPod(ctx, spec.ID)
		return nil, fmt.Errorf("wait pod ready: %w", err)
	}

	ac, err := agent.Dial(ip + ":50051")
	if err != nil {
		_ = m.k8s.DeleteSandboxPod(ctx, spec.ID)
		return nil, fmt.Errorf("dial agent: %w", err)
	}

	if err := m.pingAgent(ctx, ac); err != nil {
		_ = ac.Close()
		_ = m.k8s.DeleteSandboxPod(ctx, spec.ID)
		return nil, fmt.Errorf("agent ping: %w", err)
	}

	now := time.Now().UTC()
	maxSec := spec.MaxSessionSeconds
	if maxSec == 0 {
		maxSec = 1800
	}
	deadline := now.Add(time.Duration(maxSec) * time.Second)

	tagsJSON, err := json.Marshal(orEmptyMap(spec.Tags))
	if err != nil {
		_ = ac.Close()
		_ = m.k8s.DeleteSandboxPod(ctx, spec.ID)
		return nil, fmt.Errorf("marshal tags: %w", err)
	}
	envJSON, err := json.Marshal(orEmptyMap(spec.Environment))
	if err != nil {
		_ = ac.Close()
		_ = m.k8s.DeleteSandboxPod(ctx, spec.ID)
		return nil, fmt.Errorf("marshal environment: %w", err)
	}
	egress := spec.NetworkEgress
	if egress == "" {
		egress = "none"
	}

	_, err = m.db.Pool.Exec(ctx, `
		INSERT INTO sandboxes (
			id, pod_name, pod_ip, namespace, image, flavor, tenant_id, tags,
			environment, network_egress, idle_timeout_seconds, idle_action,
			max_session_seconds, callback_url, status, created_at, deadline
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8,
			$9, $10, $11, $12,
			$13, $14, 'running', $15, $16
		)
	`,
		spec.ID, pod.Name, ip, pod.Namespace, spec.Image, spec.Flavor, spec.TenantID, tagsJSON,
		envJSON, egress, nullIfZero(spec.IdleTimeoutSeconds), nullIfEmpty(spec.IdleAction),
		maxSec, nullIfEmpty(spec.CallbackURL), now, deadline,
	)
	if err != nil {
		_ = ac.Close()
		_ = m.k8s.DeleteSandboxPod(ctx, spec.ID)
		return nil, fmt.Errorf("insert sandbox: %w", err)
	}

	m.agentsMu.Lock()
	m.agents[spec.ID] = ac
	m.agentsMu.Unlock()

	return &Sandbox{
		ID:                 spec.ID,
		PodName:            pod.Name,
		PodIP:              ip,
		Namespace:          pod.Namespace,
		Image:              spec.Image,
		Flavor:             spec.Flavor,
		TenantID:           spec.TenantID,
		Tags:               spec.Tags,
		Environment:        spec.Environment,
		NetworkEgress:      egress,
		MaxSessionSeconds:  maxSec,
		IdleTimeoutSeconds: spec.IdleTimeoutSeconds,
		IdleAction:         spec.IdleAction,
		CallbackURL:        spec.CallbackURL,
		Status:             "running",
		Created:            now,
		Deadline:           deadline,
		Agent:              ac,
	}, nil
}

// Get reads the sandbox row from Postgres and attaches the cached agent client
// if one exists. Returns ErrNotFound if the row is missing or already ended;
// callers map that to HTTP 404. Future read endpoints that want to surface
// ended sandboxes (for history or billing reconciliation) should call a
// separate accessor rather than relaxing this one.
func (m *Manager) Get(ctx context.Context, id string) (*Sandbox, error) {
	sb, err := m.queryByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if sb.Status == "ended" {
		return nil, ErrNotFound
	}
	m.agentsMu.RLock()
	sb.Agent = m.agents[id]
	m.agentsMu.RUnlock()
	return sb, nil
}

// Delete marks the sandbox ended with cause user_kill, closes the cached agent
// connection, and deletes the pod. Idempotent: a repeated call against an
// already-ended sandbox returns nil without touching the cluster. Pod
// NotFound from k8s is treated as success since the goal is for the pod to
// not exist.
func (m *Manager) Delete(ctx context.Context, id string) error {
	tag, err := m.db.Pool.Exec(ctx, `
		UPDATE sandboxes
		SET status = 'ended', end_cause = 'user_kill', ended_at = NOW()
		WHERE id = $1 AND status != 'ended'
	`, id)
	if err != nil {
		return fmt.Errorf("update sandbox: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Already ended or never existed. Either way, idempotent success.
		return nil
	}

	m.agentsMu.Lock()
	ac, ok := m.agents[id]
	if ok {
		delete(m.agents, id)
	}
	m.agentsMu.Unlock()
	if ac != nil {
		_ = ac.Close()
	}

	if err := m.k8s.DeleteSandboxPod(ctx, id); err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("delete pod: %w", err)
	}
	return nil
}

// queryByID scans a sandbox row into a Sandbox struct. Returns ErrNotFound if
// no row matches. Agent is left nil; callers populate from the agents map if
// needed.
func (m *Manager) queryByID(ctx context.Context, id string) (*Sandbox, error) {
	var (
		sb           Sandbox
		podIP        sql.NullString
		endCause     sql.NullString
		idleTimeout  sql.NullInt32
		idleAction   sql.NullString
		maxSession   sql.NullInt32
		callbackURL  sql.NullString
		endedAt      sql.NullTime
		tagsRaw      []byte
		envRaw       []byte
	)
	err := m.db.Pool.QueryRow(ctx, `
		SELECT id, pod_name, pod_ip, namespace, image, flavor, tenant_id, tags,
		       environment, network_egress, idle_timeout_seconds, idle_action,
		       max_session_seconds, callback_url, status, end_cause,
		       created_at, deadline, ended_at
		FROM sandboxes
		WHERE id = $1
	`, id).Scan(
		&sb.ID, &sb.PodName, &podIP, &sb.Namespace, &sb.Image, &sb.Flavor, &sb.TenantID, &tagsRaw,
		&envRaw, &sb.NetworkEgress, &idleTimeout, &idleAction,
		&maxSession, &callbackURL, &sb.Status, &endCause,
		&sb.Created, &sb.Deadline, &endedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("query sandbox: %w", err)
	}
	sb.PodIP = podIP.String
	sb.EndCause = endCause.String
	sb.IdleTimeoutSeconds = int(idleTimeout.Int32)
	sb.IdleAction = idleAction.String
	sb.MaxSessionSeconds = int(maxSession.Int32)
	sb.CallbackURL = callbackURL.String
	if endedAt.Valid {
		t := endedAt.Time
		sb.EndedAt = &t
	}
	if len(tagsRaw) > 0 {
		if err := json.Unmarshal(tagsRaw, &sb.Tags); err != nil {
			return nil, fmt.Errorf("unmarshal tags: %w", err)
		}
	}
	if len(envRaw) > 0 {
		if err := json.Unmarshal(envRaw, &sb.Environment); err != nil {
			return nil, fmt.Errorf("unmarshal environment: %w", err)
		}
	}
	return &sb, nil
}

// markEnded sets a sandbox row to ended with the given cause. Used by Reconcile
// when a pod is found missing. Safe to call against an already-ended row (the
// UPDATE just affects zero rows).
func (m *Manager) markEnded(ctx context.Context, id, cause string) error {
	_, err := m.db.Pool.Exec(ctx, `
		UPDATE sandboxes
		SET status = 'ended', end_cause = $2, ended_at = NOW()
		WHERE id = $1 AND status != 'ended'
	`, id, cause)
	return err
}

// waitForPodReady polls the cluster until the pod is Running with a non-empty
// pod IP, up to 60 seconds.
func (m *Manager) waitForPodReady(ctx context.Context, id string) (string, error) {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		pod, err := m.k8s.GetSandboxPod(ctx, id)
		if err != nil {
			return "", err
		}
		if pod.Status.Phase == "Running" && pod.Status.PodIP != "" {
			return pod.Status.PodIP, nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return "", fmt.Errorf("timed out waiting for pod %s", id)
}

// pingAgent retries an Exec-based liveness check for up to 30 seconds. Used on
// Create and Reconcile.
func (m *Manager) pingAgent(ctx context.Context, ac *agent.Client) error {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := ac.Ping(pingCtx)
		cancel()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("timed out waiting for agent")
}

// nullIfZero returns nil for 0, the int otherwise. pgx encodes nil as NULL.
func nullIfZero(n int) any {
	if n == 0 {
		return nil
	}
	return n
}

// nullIfEmpty returns nil for "", the string otherwise.
func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// orEmptyMap returns the input or an empty map so json.Marshal produces "{}"
// rather than "null" for nil maps.
func orEmptyMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}