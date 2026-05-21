-- sandbox_oss schema. Applied on every adapter startup; uses IF NOT EXISTS so
-- it's idempotent. Schema migrations beyond CREATE TABLE will need a proper
-- migration tool (goose, atlas) once we start adding columns or changing
-- types, but for the prototype this is enough.

CREATE TABLE IF NOT EXISTS sandboxes (
    id                   TEXT        PRIMARY KEY,
    pod_name             TEXT        NOT NULL,
    pod_ip               TEXT,
    namespace            TEXT        NOT NULL DEFAULT 'default',
    image                TEXT        NOT NULL,
    flavor               TEXT        NOT NULL DEFAULT '',
    tenant_id            TEXT        NOT NULL DEFAULT '',
    tags                 JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status               TEXT        NOT NULL,                       -- "creating" | "running" | "ended"
    end_cause            TEXT,                                       -- "user_kill" | "expired" | "host_failed" | "adapter_restarted" | "system_error"
    environment          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    network_egress       TEXT        NOT NULL DEFAULT 'none',
    idle_timeout_seconds INT,
    idle_action          TEXT,
    max_session_seconds  INT,
    callback_url         TEXT,
    created_at           TIMESTAMPTZ NOT NULL,
    deadline             TIMESTAMPTZ NOT NULL,
    ended_at             TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS sandboxes_tenant_id_idx ON sandboxes(tenant_id);
CREATE INDEX IF NOT EXISTS sandboxes_status_idx    ON sandboxes(status);

CREATE TABLE IF NOT EXISTS execs (
    id               TEXT        PRIMARY KEY,
    sandbox_id       TEXT        NOT NULL REFERENCES sandboxes(id) ON DELETE CASCADE,
    command          TEXT        NOT NULL,
    cwd              TEXT        NOT NULL DEFAULT '',
    environment      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    background       BOOLEAN     NOT NULL DEFAULT FALSE,
    max_output_bytes BIGINT      NOT NULL,
    status           TEXT        NOT NULL,                            -- "running" | "completed" | "errored"
    exit_code        INT,
    stdout           BYTEA,
    stderr           BYTEA,
    truncated        BOOLEAN     NOT NULL DEFAULT FALSE,
    completion_err   TEXT,
    started_at       TIMESTAMPTZ NOT NULL,
    completed_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS execs_sandbox_id_idx ON execs(sandbox_id);
CREATE INDEX IF NOT EXISTS execs_status_idx     ON execs(status);