-- NEXUS PostgreSQL schema (idempotent: safe to run more than once).
-- PostgreSQL stores METADATA only: worker state, metrics, jobs, tasks and
-- execution history. Large data blocks (future HDFS-like storage) will live
-- on the Workers' disks, never in PostgreSQL.

CREATE TABLE IF NOT EXISTS workers (
    worker_id       VARCHAR(64) PRIMARY KEY,
    hostname        VARCHAR(255),
    ip_address      VARCHAR(64),
    cpu_cores       INTEGER,
    memory_total_mb BIGINT,
    cpu_usage       DOUBLE PRECISION NOT NULL DEFAULT 0,   -- percent
    ram_usage       DOUBLE PRECISION NOT NULL DEFAULT 0,   -- percent
    disk_usage      DOUBLE PRECISION NOT NULL DEFAULT 0,   -- percent
    status          VARCHAR(16) NOT NULL DEFAULT 'INACTIVE'
                    CHECK (status IN ('ACTIVE', 'INACTIVE')),
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id       SERIAL PRIMARY KEY,
    job_type     VARCHAR(64) NOT NULL,
    status       VARCHAR(16) NOT NULL DEFAULT 'PENDING'
                 CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')),
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id         SERIAL PRIMARY KEY,
    job_id          INTEGER NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    task_type       VARCHAR(64) NOT NULL,
    params          TEXT,
    assigned_worker VARCHAR(64) REFERENCES workers(worker_id),
    status          VARCHAR(16) NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'ASSIGNED', 'RUNNING', 'COMPLETED', 'FAILED')),
    result          TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    assigned_at     TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS resource_metrics (
    metric_id  BIGSERIAL PRIMARY KEY,
    worker_id  VARCHAR(64) NOT NULL REFERENCES workers(worker_id) ON DELETE CASCADE,
    "timestamp" TIMESTAMPTZ NOT NULL DEFAULT now(),
    cpu_usage  DOUBLE PRECISION,
    ram_usage  DOUBLE PRECISION,
    disk_usage DOUBLE PRECISION,
    status     VARCHAR(16)
);

-- One row per finished task. worker_cpu_at_assign / worker_ram_at_assign and
-- scheduler_policy are recorded now so the future XGBoost model has
-- ready-made training features (task_type, worker load -> execution_time).
CREATE TABLE IF NOT EXISTS execution_history (
    execution_id         SERIAL PRIMARY KEY,
    task_id              INTEGER NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    worker_id            VARCHAR(64) NOT NULL REFERENCES workers(worker_id),
    task_type            VARCHAR(64),
    execution_time       DOUBLE PRECISION,              -- seconds, measured on the Worker
    status               VARCHAR(16) NOT NULL,          -- COMPLETED / FAILED
    worker_cpu_at_assign DOUBLE PRECISION,
    worker_ram_at_assign DOUBLE PRECISION,
    scheduler_policy     VARCHAR(32),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_workers_status       ON workers(status);
CREATE INDEX IF NOT EXISTS idx_tasks_job            ON tasks(job_id);
CREATE INDEX IF NOT EXISTS idx_tasks_worker_status  ON tasks(assigned_worker, status);
CREATE INDEX IF NOT EXISTS idx_metrics_worker_time  ON resource_metrics(worker_id, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_exec_worker          ON execution_history(worker_id);
CREATE INDEX IF NOT EXISTS idx_exec_task_type       ON execution_history(task_type);
