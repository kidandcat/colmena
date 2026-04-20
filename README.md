<p align="center">
  <img src="banner.png" alt="Colmena" width="700">
</p>

<h1 align="center">Colmena</h1>

<p align="center">
  Distributed SQLite as an embeddable Go library. No CGo, no external processes — just <code>import</code> and go.
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/kidandcat/colmena"><img src="https://pkg.go.dev/badge/github.com/kidandcat/colmena.svg" alt="Go Reference"></a>
  <a href="https://github.com/kidandcat/colmena/actions"><img src="https://img.shields.io/badge/coverage-80.7%25-brightgreen" alt="Coverage"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-BSD--3--Clause-blue" alt="License"></a>
</p>

Colmena combines [hashicorp/raft](https://github.com/hashicorp/raft) for consensus with [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite) for storage, exposing a standard `database/sql` interface. Every node in the cluster holds a full copy of the database.

```go
node, _ := colmena.New(colmena.Config{
    NodeID:    "node-1",
    DataDir:   "./data/node1",
    Bind:      "0.0.0.0:9000",
    Bootstrap: true,
})
defer node.Close()

db := node.DB() // standard *sql.DB
db.Exec("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)")
db.Exec("INSERT INTO kv (key, value) VALUES (?, ?)", "hello", "world")

var value string
db.QueryRow("SELECT value FROM kv WHERE key = ?", "hello").Scan(&value)
```

## Features

- **Pure Go** — no CGo, no C compiler needed. Cross-compiles cleanly.
- **`database/sql` compatible** — drop-in distributed database for Go programs.
- **Automatic leader forwarding** — write from any node, it routes to the leader via RPC.
- **Configurable read consistency** — `None` (local), `Weak` (leader check), `Strong` (quorum verify).
- **Automatic write batching** — concurrent writes are coalesced into a single Raft entry, giving **60×+ throughput** under load (~3,000–4,000 ops/sec on a 3-node cluster, see [Benchmarks](#benchmarks)).
- **Buffered transactions** — `db.Begin()`/`tx.Commit()` batches writes into a single Raft entry.
- **Continuous backup** — Litestream-style WAL streaming to local filesystem or S3.
- **Leader forwarding for custom handlers** — type-safe `Forward[Req, Resp]()` sends any request to the leader via RPC.
- **Reactive hooks** — `OnApply` callback fires on every node after each replicated write.

## Quick Start

### Single node

```go
import "github.com/kidandcat/colmena"

node, err := colmena.New(colmena.Config{
    NodeID:    "node-1",
    DataDir:   "./data",
    Bind:      "0.0.0.0:9000",
    Bootstrap: true,
})
if err != nil {
    log.Fatal(err)
}
defer node.Close()
node.WaitForLeader(10 * time.Second)

db := node.DB()
```

### Three-node cluster

```go
// Node 1 (bootstrap)
node1, _ := colmena.New(colmena.Config{
    NodeID:    "node-1",
    DataDir:   "./data/node1",
    Bind:      "10.0.0.1:9000",
    Bootstrap: true,
})

// Node 2 (join)
node2, _ := colmena.New(colmena.Config{
    NodeID:    "node-2",
    DataDir:   "./data/node2",
    Bind:      "10.0.0.2:9000",
    Join:      []string{"10.0.0.1:9000"},
})

// Node 3 (join)
node3, _ := colmena.New(colmena.Config{
    NodeID:    "node-3",
    DataDir:   "./data/node3",
    Bind:      "10.0.0.3:9000",
    Join:      []string{"10.0.0.1:9000"},
})
```

All three nodes can serve reads. Writes from any node are automatically forwarded to the leader.

## Read Consistency

Colmena has three read consistency levels. The default is **Weak**.

| Level | Where it reads | Guarantee | Latency |
|---|---|---|---|
| **None** | Local SQLite on this node | May be stale if node is behind on replication or partitioned | ~8µs |
| **Weak** | Leader node (forwards if not leader) | Always reads from the node that processes writes. Tiny staleness window (~1s) during leadership transitions | ~90µs |
| **Strong** | Leader node after quorum confirmation | Linearizable — impossible to get stale data, even during leader changes | ~100µs+ |

How each level works on a **follower** node:

- **None** → reads local SQLite directly (follower's copy, may lag behind leader)
- **Weak** → forwards the query to the leader via RPC, leader reads its local SQLite
- **Strong** → forwards to leader, leader contacts quorum to confirm it's still leader, then reads

How each level works on the **leader** node:

- **None** → reads local SQLite directly
- **Weak** → reads local SQLite directly (it believes it's the leader)
- **Strong** → confirms leadership with quorum first, then reads local SQLite

```go
// Read from local SQLite, no network. Fast but may be stale on followers.
ctx := colmena.WithConsistency(ctx, colmena.ConsistencyNone)

// Read from the leader. Fresh data, minimal overhead. (default)
ctx := colmena.WithConsistency(ctx, colmena.ConsistencyWeak)

// Linearizable read. Leader verifies with quorum before responding.
ctx := colmena.WithConsistency(ctx, colmena.ConsistencyStrong)

rows, err := db.QueryRowContext(ctx, "SELECT ...")
```

## Write Batching

Concurrent writes are automatically coalesced into a single Raft log entry. This amortizes the consensus round-trip across many statements and is what makes Colmena usable under load — without it, Raft fsync latency would cap throughput at a few dozen writes per second.

Batching is **on by default** with a 2ms window. Any writes that land within the same 2ms window are merged into one Raft apply; the batch also flushes early once it reaches `BatchMaxSize` (default 128).

```go
node, _ := colmena.New(colmena.Config{
    // ...
    BatchWindow:  2 * time.Millisecond, // default, can omit
    BatchMaxSize: 128,                  // default, can omit
})
```

Trade-off: a single write with no concurrency waits up to `BatchWindow` before being applied. For latency-sensitive workloads with serial writes, lower `BatchWindow` (e.g. `500 * time.Microsecond`) or disable it by setting a negative value:

```go
colmena.Config{
    BatchWindow: -1, // disable batching entirely
}
```

See the [Benchmarks](#benchmarks) section for the scaling curve.

## Transactions

Buffered transactions batch multiple writes into a single atomic Raft entry:

```go
tx, _ := db.Begin()
tx.Exec("INSERT INTO accounts (id, balance) VALUES (?, ?)", "alice", 100)
tx.Exec("INSERT INTO accounts (id, balance) VALUES (?, ?)", "bob", 200)
tx.Commit() // single Raft round-trip for both inserts
```

Or use `ExecMulti` directly:

```go
node.ExecMulti([]colmena.Statement{
    {SQL: "UPDATE accounts SET balance = balance - 50 WHERE id = ?", Args: []any{"alice"}},
    {SQL: "UPDATE accounts SET balance = balance + 50 WHERE id = ?", Args: []any{"bob"}},
})
```

## Continuous Backup

Enable Litestream-style backup to local filesystem or S3:

```go
backend, _ := colmena.NewLocalBackend("./backups")

node, _ := colmena.New(colmena.Config{
    // ...
    Backup: &colmena.BackupConfig{
        Backend:          backend,
        SyncInterval:     1 * time.Second,    // WAL sync frequency
        SnapshotInterval: 1 * time.Hour,      // full snapshot frequency
    },
})
```

Restore from backup:

```go
colmena.Restore(ctx, backend, "./data/restored-node")
```

S3-compatible backend (AWS, MinIO, R2, B2):

```go
import "github.com/kidandcat/colmena/backup/s3"

backend, _ := s3.NewBackend(s3.Config{
    Endpoint:     "s3.amazonaws.com",
    Bucket:       "my-backups",
    Prefix:       "colmena/prod",
    Region:       "us-east-1",
    AccessKey:    os.Getenv("AWS_ACCESS_KEY_ID"),
    SecretKey:    os.Getenv("AWS_SECRET_ACCESS_KEY"),
    UsePathStyle: false,
})
```

## Reactive Hooks

`OnApply` fires on every node (leader and followers) after each replicated write. Useful for real-time notifications, WebSocket broadcasts, cache invalidation, etc.

```go
node, _ := colmena.New(colmena.Config{
    // ...
    OnApply: func(db string, stmts []colmena.Statement, results []colmena.ExecResult) {
        for _, stmt := range stmts {
            if strings.HasPrefix(stmt.SQL, "INSERT INTO events") {
                notifySubscribers(db, stmt.Args)
            }
        }
    },
})
```

## Leader Forwarding for Custom Handlers

Register typed handlers that always execute on the leader, regardless of which node receives the request. Useful for MQTT command processing, webhook handling, or any logic that must run on a single node.

```go
// Define a typed key — the type parameters enforce compile-time safety.
var ProcessCmd = colmena.NewHandlerKey[CommandReq, CommandResp]("device.command")

// Register the handler on every node (same binary, same code).
colmena.RegisterHandler(node, ProcessCmd, func(req CommandReq) (CommandResp, error) {
    // This only executes on the leader.
    result, err := processDeviceCommand(req.DeviceID, req.Payload)
    if err != nil {
        return CommandResp{}, err
    }
    return CommandResp{Status: "ok", Result: result}, nil
})

// Forward from any node — routed to the leader automatically.
resp, err := colmena.Forward(node, ProcessCmd, CommandReq{
    DeviceID: "sensor-42",
    Payload:  []byte(`{"action":"reboot"}`),
})
```

How it works:
- If the node **is** the leader, the handler runs locally with no network hop.
- If the node **is not** the leader, the request is serialized and forwarded to the leader via the existing RPC pool (same connection pool used for SQL forwarding).
- The `HandlerKey[Req, Resp]` ensures that request and response types match at compile time — no raw strings or `interface{}` at the call site.
- Handlers must be registered before calling `Forward`. Registering the same key twice panics.

## Multiple Databases

A single Raft cluster can host multiple independent SQLite databases, each with its own default consistency level. Each database maps to a separate `.db` file on disk.

```go
node, _ := colmena.New(colmena.Config{...})

mainDB     := node.OpenDB("main", colmena.ConsistencyWeak)
logsDB     := node.OpenDB("logs", colmena.ConsistencyNone)
accountsDB := node.OpenDB("accounts", colmena.ConsistencyStrong)

// Backward compatible — same as node.OpenDB("default", config.Consistency)
defaultDB := node.DB()
```

Each database is fully isolated: tables created in one database are not visible in another. Writes to different databases go through the same Raft log, so they share the cluster's write throughput. Reads are independent since each database has its own reader pool.

You can still override the consistency level per-query using `WithConsistency`:

```go
ctx := colmena.WithConsistency(ctx, colmena.ConsistencyStrong)
rows, _ := logsDB.QueryContext(ctx, "SELECT ...")
```

## Configuration

```go
colmena.Config{
    NodeID            string            // Required. Unique node identifier.
    DataDir           string            // Required. Raft logs, snapshots, SQLite data.
    Bind              string            // Required. Raft transport + RPC address.
    Advertise         string            // Address advertised to peers. Default: Bind.
    Bootstrap         bool              // Bootstrap new cluster (first node only).
    Join              []string          // Addresses of existing nodes to join.
    Consistency       ConsistencyLevel  // Default read consistency. Default: Weak.
    HeartbeatTimeout  time.Duration     // Default: 1s.
    ElectionTimeout   time.Duration     // Default: 1s.
    SnapshotInterval  time.Duration     // Default: 2m.
    SnapshotThreshold uint64            // Raft log entries before snapshot. Default: 1024.
    ApplyTimeout      time.Duration     // Raft apply timeout. Default: 10s.
    MaxPool           int               // Raft TCP connection pool. Default: 3.
    SQLiteReadConns   int               // Reader pool size. Default: 4.
    BatchWindow       time.Duration     // Write batching window. Default: 2ms. Negative disables.
    BatchMaxSize      int               // Max commands per batch. Default: 128.
    UnsafeNoRaftLogFsync bool           // Skip fsync on Raft log. Faster but lossy. Default: false.
    LogOutput         io.Writer         // Raft log output. Default: os.Stderr.
    Backup            *BackupConfig     // Continuous backup config. Optional.
    OnApply           func(string, []Statement, []ExecResult)  // Reactive hook. Optional.
}
```

## How It Works

```
        Write from any node
               │
               ▼
    ┌─── Am I the leader? ───┐
    │ yes                  no │
    ▼                         ▼
  raft.Apply()          Forward via RPC
    │                         │
    ▼                         ▼
  Quorum ack ◄─── Raft replication ───► Quorum ack
    │
    ▼
  FSM.Apply() on EVERY node
    │
    ├─► Execute SQL on local SQLite
    └─► Fire OnApply callback
```

- **Writes** go through Raft consensus (leader serializes, quorum acknowledges, all nodes apply).
- **Concurrent writes are batched** on the leader: a `WriteBatcher` collects commands for up to `BatchWindow` (2ms default) and submits them as a single `ExecuteMulti` Raft entry. Results are fanned back to each caller. This is how Colmena reaches 5,000+ writes/sec despite Raft's per-entry fsync cost.
- **Reads** hit local SQLite directly (configurable consistency level).
- **Custom handlers** follow the same forwarding path as writes — any node can call `Forward()`, and the request is routed to the leader via RPC.
- **Snapshots** use SQLite's Online Backup API for consistent point-in-time copies.
- **RPC** uses `net/rpc` on port+1 for leader forwarding, handler forwarding, and cluster join.
- **SQLite writer** runs in WAL mode with `synchronous=NORMAL` — safe because Raft already provides the durability guarantee across the cluster.

## Benchmarks

Measured on Apple M1 Pro, 3-node cluster running on localhost, colmena v0.6.2. All numbers use the default config (`BatchWindow=2ms`, `BatchMaxSize=128`) unless noted. Network latency in production will increase write times proportionally.

### Write throughput scales with concurrency

This is the headline result. Because the batcher coalesces concurrent writes into a single Raft entry, throughput grows with the number of concurrent writers rather than being capped by Raft's per-entry fsync.

| Concurrent writers | Batching disabled | `BatchWindow=2ms` | `BatchWindow=5ms` | Batch + `UnsafeNoRaftLogFsync` |
|---:|---:|---:|---:|---:|
| 4 | 50 | 92 | — | — |
| 32 | 125 | 828 | — | — |
| 128 | — | **3,154** | **4,224** | **69,273** |

Numbers in ops/sec, 3-node cluster, 5-second sustained writes. Rule of thumb: if your workload has even a handful of concurrent writers, you get 1,000+ ops/sec; at 100+ writers you saturate SQLite on the leader, not Raft. A single serial writer is the worst case — the batcher has nothing to coalesce with, so throughput falls to ~80 ops/sec.

### Per-operation benchmarks

| Operation | ns/op | Parallel throughput (8 cores) |
|---|---:|---:|
| Write (1 node, 1 writer) | 12.5ms | — |
| Write (1 node, parallel) | 1.43ms | ~5,600 ops/sec |
| Write (3 nodes, 1 writer on leader) | 26.0ms | — |
| Write (3 nodes, 1 writer from follower) | 25.4ms | — |
| Write (3 nodes, parallel on leader) | 3.24ms | ~2,500 ops/sec |
| Transaction (3 stmts, 1 node) | 11.9ms | — |
| Read (1 node) | 6.17µs | 162,000 ops/sec |
| Read (3 nodes, follower local) | 6.10µs | 164,000 ops/sec |

### Latency distribution (3 nodes, 200 samples)

| Operation | P50 | P95 | P99 | Max |
|---|---:|---:|---:|---:|
| Write (leader) | 39ms | 52ms | 66ms | 66ms |
| Write (follower→leader) | 42ms | 55ms | 62ms | 65ms |
| Read (strong, quorum verify) | 128µs | 255µs | 363µs | 1.8ms |
| Read (local, follower) | **11µs** | 15µs | 103µs | 735µs |

Serial-writer latency picks up ~2ms from the default batch window (the batcher waits for a potential co-traveler before flushing). If your workload is a single writer issuing statements serially and latency matters more than throughput, set `BatchWindow: 500 * time.Microsecond` or `BatchWindow: -1` to disable.

**Key takeaways:**
- Local reads from a follower match single-node raw SQLite speed (~6µs), so read-heavy workloads scale horizontally for free.
- Single-writer write latency is dominated by Raft consensus (~40ms P50 for quorum round-trip + fsync on localhost).
- Throughput scales through write batching: a single Raft entry carries up to `BatchMaxSize` (128) statements. Expect ~60× lift between 4 and 128 concurrent writers at the default 2ms window.
- Leader forwarding overhead is negligible (~3ms) — writing from a follower is as fast as writing on the leader, and the `TaggedValue` wire format (v0.6.1+) preserves driver types like `time.Time` across the hop.
- `UnsafeNoRaftLogFsync` gives ~20× additional throughput by skipping BoltDB fsync, at the cost of losing the log tail on a crash. Safe with 3+ node clusters (peers re-replicate missing entries on restart) or ephemeral deployments.

Run benchmarks yourself:

```bash
go test -bench=. -benchmem -benchtime=3s -timeout 600s .
go test -run TestBatchingThroughput -v -timeout 300s .
go test -run TestLatencyDistribution -v -timeout 120s .
```

## Versioning & Upgrades

Colmena wraps every replicated/persisted blob in a 10-byte self-describing envelope (`COLMENA\x00` magic + kind + version) so nodes can detect format skew and refuse to silently misinterpret data written by a future release.

- **Commands** in the Raft log are tagged with `CommandFormatVersion`.
- **FSM snapshots** are tagged with `SnapshotFormatVersion`.
- The **RPC handshake** (`Colmena.Hello`) runs on every new connection and logs a warning when peers disagree on any version.

Each Colmena release keeps the decoder for the previous envelope version in place for **at least one release**, so rolling upgrades (stop node → upgrade → rejoin, one at a time) work without wiping data. Legacy pre-envelope shapes (raw JSON commands, raw-SQLite v0.2.0 snapshots, unenveloped tar v0.3–v0.5 snapshots) are still recognized for backward compatibility. A node that sees an **unknown newer** envelope version returns `ErrUnsupportedFormatVersion` instead of corrupting state — upgrade the lagging node before proceeding.

Call `node.Version()` to read the current library version at runtime.

## Limitations

- **No non-deterministic SQL functions** — `RANDOM()`, `datetime('now')`, etc. produce different values on each node. Pass values as parameters instead.
- **Single writer** — all writes serialize through the Raft leader. This is inherent to Raft consensus.
- **Statement-level replication** — SQL statements (not WAL pages) are replicated. This is simpler but means the above limitation applies.
- **Reads in transactions** — `tx.Query()` reads from local SQLite and won't see uncommitted writes buffered in the same transaction.
- **Minimum 3 nodes** for fault tolerance — 2 nodes is worse than 1 (no quorum if either fails).

## License

BSD-3-Clause
