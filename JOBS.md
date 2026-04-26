# JOBS — feature spec

Add a first-class background job system to Colmena, leveraging the existing
distributed SQLite + Raft infrastructure.

## Goal

Provide an idiomatic, ergonomic background job system as part of Colmena
itself, so consumer apps (animux, Fecha, future projects) can register and
run jobs without bringing in Redis, Postgres, River, Asynq, etc.

The system should feel native to Colmena: same `colmena.New(...)` entrypoint,
same `database/sql` philosophy, same "leader handles writes, all nodes share
state" semantics.

## Why integrate into Colmena (not build per-app)

- Reuses Raft consistency: jobs claimed and updated through the same
  log → no separate locking / coordination protocol needed.
- Reuses Litestream-style backup → jobs survive restarts and crashes.
- Reuses leader forwarding → workers on followers can claim jobs and
  coordinate via the leader transparently.
- Single binary, zero external dependencies, fits Colmena's "pure Go"
  philosophy.
- Reusable across Jairo's projects (Fecha, animux, future).

## API surface (proposed)

Idiomatic, similar style to existing `RegisterHandler[Req, Resp]`:

```go
import "github.com/kidandcat/colmena"
import "github.com/kidandcat/colmena/jobs"

node, _ := colmena.New(colmena.Config{
    NodeID:    "node-1",
    DataDir:   "./data",
    Bind:      "0.0.0.0:9000",
    Bootstrap: true,
    Jobs: &jobs.Config{
        Workers:        16,             // worker pool size per node
        PollInterval:   1 * time.Second,
        DefaultTimeout: 5 * time.Minute,
        DefaultMaxAttempts: 5,
    },
})
defer node.Close()

// Register a typed job handler
type ScrapeAniListArgs struct {
    Page int `json:"page"`
}

jobs.Register(node, "scrape_anilist", func(ctx jobs.Context, args ScrapeAniListArgs) error {
    log.Printf("scraping page %d on node %s", args.Page, ctx.NodeID)
    // ... do work ...
    return nil
})

// Enqueue a one-off job (returns once durably persisted via Raft)
id, err := jobs.Enqueue(node, "scrape_anilist", ScrapeAniListArgs{Page: 1})

// Enqueue with options
id, err = jobs.Enqueue(node, "scrape_anilist", ScrapeAniListArgs{Page: 2},
    jobs.WithPriority(jobs.PriorityHigh),
    jobs.WithRunAt(time.Now().Add(10 * time.Minute)),
    jobs.WithMaxAttempts(3),
    jobs.WithUniqueKey("scrape:page:2"), // dedupe — same key while pending = single job
)

// Schedule a recurring job (cron-style)
jobs.Schedule(node, "refresh_airing", "0 */6 * * *", RefreshAiringArgs{})
jobs.Schedule(node, "scrape_news",    "*/15 * * * *", ScrapeNewsArgs{})

// Per-handler concurrency limits (e.g. don't run more than 2 scrapers
// against a single rate-limited domain at the same time, cluster-wide)
jobs.SetConcurrency(node, "scrape_justwatch", 2)

// Per-handler rate limit (token bucket, cluster-wide)
jobs.SetRateLimit(node, "scrape_justwatch", jobs.Rate{Per: time.Minute, N: 30})
```

`jobs.Context` provides:
- `NodeID() string`
- `JobID() string`
- `Attempt() int`
- `EnqueuedAt() time.Time`
- `Done() <-chan struct{}` (cancellation when shutdown)
- `context.Context` embedding

## Job lifecycle states

```
pending  → claimed → running → succeeded
                              → failed       → (retry) → pending
                                             → dead     (after max_attempts)
```

## Schema (auto-migrated when Jobs config is set)

```sql
CREATE TABLE IF NOT EXISTS colmena_jobs (
    id            TEXT PRIMARY KEY,           -- ULID or UUIDv7
    type          TEXT NOT NULL,
    payload       BLOB NOT NULL,              -- JSON-encoded args
    status        TEXT NOT NULL,              -- pending/claimed/running/succeeded/failed/dead
    priority      INTEGER NOT NULL DEFAULT 0, -- higher = sooner
    attempts      INTEGER NOT NULL DEFAULT 0,
    max_attempts  INTEGER NOT NULL DEFAULT 5,
    enqueued_at   INTEGER NOT NULL,           -- unix millis
    run_at        INTEGER NOT NULL,           -- when eligible to run
    claimed_at    INTEGER,
    claimed_by    TEXT,                       -- node id
    started_at    INTEGER,
    finished_at   INTEGER,
    last_error    TEXT,
    unique_key    TEXT,                       -- dedupe pending jobs
    timeout_ms    INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_pending
    ON colmena_jobs(status, run_at, priority DESC)
    WHERE status = 'pending';

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_unique
    ON colmena_jobs(unique_key)
    WHERE unique_key IS NOT NULL AND status IN ('pending', 'claimed', 'running');

CREATE TABLE IF NOT EXISTS colmena_jobs_schedule (
    id            TEXT PRIMARY KEY,
    job_type      TEXT NOT NULL,
    cron_expr     TEXT NOT NULL,
    payload       BLOB NOT NULL,
    next_run_at   INTEGER NOT NULL,
    last_run_at   INTEGER,
    enabled       INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_schedule_next
    ON colmena_jobs_schedule(next_run_at)
    WHERE enabled = 1;
```

## Claim-and-run protocol

To avoid double-execution across nodes:

1. Worker loop on each node polls `colmena_jobs` for `status='pending'
   AND run_at <= now`, ordered by priority DESC, run_at ASC.
2. To claim, worker submits a Raft command via leader-forwarding:
   `UPDATE colmena_jobs SET status='claimed', claimed_at=?, claimed_by=?
    WHERE id=? AND status='pending'`
3. Raft serializes — only one node wins.
4. Winner runs the handler in-process. On completion, another Raft
   command updates status to `succeeded` / `failed`.
5. On panic / timeout: deferred recover updates status with error,
   schedules retry with exponential backoff if attempts < max_attempts.
6. On node crash mid-job: a sweeper periodically reclaims jobs whose
   `claimed_at + timeout_ms < now` and `status IN ('claimed','running')`
   back to `pending`.

## Backoff

Exponential with jitter: `base * 2^attempt + rand(0, base)`.
Defaults: base = 5s, max = 1h.

Configurable per-handler:
```go
jobs.Register(node, "x", handler, jobs.WithBackoff(jobs.ExponentialBackoff{
    Base: 5 * time.Second,
    Max:  1 * time.Hour,
}))
```

## Cron scheduler

Single goroutine per cluster (leader-only) that:
- Reads `colmena_jobs_schedule` rows where `enabled=1`.
- For each, if `next_run_at <= now`, enqueues a job and updates
  `last_run_at` + `next_run_at` (using `robfig/cron/v3` parser).
- Wakes every 30s or on schedule changes (notified via OnApply hook).

## Concurrency limits and rate limits

Cluster-wide limits enforced at claim time via two extra checks in the
Raft `claim` command:

- **Concurrency limit**: count of `running` jobs of this type cluster-wide;
  reject claim if already at limit, retry later.
- **Rate limit**: token bucket per job type, kept in a lightweight Raft-
  replicated counter table; refreshed by background tick.

## Observability

- `jobs.Stats(node) *jobs.Stats` returns counters: jobs by type, by status,
  per-min throughput, error rate.
- Prometheus metrics exposed via existing Colmena `metrics.go`.
- Optional admin HTTP handler:
  ```go
  http.Handle("/admin/jobs/", jobs.AdminHandler(node))
  ```
  Returns simple HTML dashboard listing recent jobs, allowing manual
  retry / cancel / re-enqueue. Read-only by default; write actions
  behind a custom auth middleware injected by the host app.

## Tests required

- Single-node basic enqueue/run/succeed.
- Single-node retry on failure.
- Single-node dead-letter after max_attempts.
- Multi-node claim race (10 nodes claiming same pending job → exactly
  one runs it).
- Multi-node leader change mid-job (claimed_by still releases).
- Sweeper reclaims orphaned jobs after timeout.
- Cron schedules fire exactly once per interval across multi-node.
- Concurrency limit enforced cluster-wide.
- Rate limit enforced cluster-wide.
- Unique key dedupes pending jobs.
- Backup/restore preserves jobs in flight (via Litestream replay).

Target coverage for new code: ≥85% (matches existing Colmena bar).

## Non-goals (explicit, v1)

- Job dependencies / DAGs (Airflow-style). Out of scope.
- Distributed transactions across job + user table. Job handlers do
  their own transactions using `node.DB()`.
- Streaming/long-poll job consumers. Polling-based only.
- Web UI beyond a single read-only HTML page in `AdminHandler`.

## Implementation milestones (for the agent picking this up)

1. **Schema + storage layer** — schema.sql, CRUD functions, migration.
2. **Worker pool + claim loop** — basic single-node enqueue/run.
3. **Multi-node claim via Raft command** — leader-forwarded UPDATE.
4. **Retry + backoff + dead-letter**.
5. **Cron scheduler (leader-only)**.
6. **Concurrency limits + rate limits**.
7. **Sweeper for orphaned jobs**.
8. **Observability: Stats(), Prometheus, AdminHandler**.
9. **Tests covering all above**.
10. **README section + godoc examples**.

## Consumer integration (animux)

Once this lands, animux will:
```go
node, _ := colmena.New(colmena.Config{
    ...
    Jobs: &jobs.Config{Workers: 16},
})

jobs.Register(node, "import_anilist_full",  handlers.ImportAniListFull)
jobs.Register(node, "refresh_airing",        handlers.RefreshAiring)
jobs.Register(node, "scrape_justwatch",      handlers.ScrapeJustWatch)
jobs.Register(node, "scrape_news",           handlers.ScrapeNews)
jobs.Register(node, "ai_curate_news",        handlers.AICurateNews)
jobs.Register(node, "generate_tonight_pick", handlers.GenerateTonightPick)
jobs.Register(node, "generate_anime_wrapped",handlers.GenerateAnimeWrapped)

jobs.SetConcurrency(node, "scrape_justwatch", 2)
jobs.SetRateLimit(node,   "scrape_justwatch", jobs.Rate{Per: time.Minute, N: 30})

jobs.Schedule(node, "refresh_airing", "0 */6 * * *",  nil)
jobs.Schedule(node, "scrape_news",    "*/15 * * * *", nil)
jobs.Schedule(node, "ai_curate_news", "0 7 * * *",    nil) // 7am UTC daily
```

Zero extra infrastructure. Same binary. Backed up. Replicated.
