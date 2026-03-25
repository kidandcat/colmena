# Colmena

Distributed SQLite as an embeddable Go library. No CGo, no external processes — just `import` and go.

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
- **Buffered transactions** — `db.Begin()`/`tx.Commit()` batches writes into a single Raft entry.
- **Continuous backup** — Litestream-style WAL streaming to local filesystem or S3.
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
- **Reads** hit local SQLite directly (configurable consistency level).
- **Snapshots** use `VACUUM INTO` for consistent point-in-time copies.
- **RPC** uses `net/rpc` on port+1 for leader forwarding and cluster join.

## Benchmarks

Measured on Apple M3 Pro, 3-node cluster running on localhost. Network latency in production will increase write times proportionally.

### Throughput

| Operation | ops/sec | Latency (P50) | Allocations |
|---|---|---|---|
| Write (1 node, sequential) | 116 | 8.6ms | 132 allocs/op |
| Write (1 node, parallel/8) | 416 | 2.4ms | 86 allocs/op |
| Write (3 nodes, on leader) | 44 | 22.7ms | 671 allocs/op |
| Write (3 nodes, from follower) | 46 | 21.7ms | 693 allocs/op |
| Transaction (3 stmts, 1 node) | 117 | 8.5ms | 169 allocs/op |
| Read (1 node) | 161,000 | 6.2µs | 24 allocs/op |
| Read (3 nodes, follower local) | 165,000 | 6.1µs | 23 allocs/op |

### Latency Distribution (3 nodes)

| Operation | P50 | P95 | P99 | Max |
|---|---|---|---|---|
| Write (leader) | 23ms | 28ms | 32ms | 37ms |
| Write (follower→leader) | 24ms | 31ms | 37ms | 38ms |
| Read (strong, quorum verify) | 92µs | 163µs | 186µs | 767µs |
| Read (local, follower) | **8µs** | 12µs | 17µs | 400µs |

### Sustained Throughput (5 seconds)

| Operation | ops/sec |
|---|---|
| Writes (4 goroutines) | 59 |
| Reads (8 goroutines, local) | **145,324** |

**Key takeaways:**
- Local reads are essentially raw SQLite speed (~8µs).
- Write latency is dominated by Raft consensus (~23ms for quorum round-trip).
- Leader forwarding overhead is minimal (~1ms extra).
- Batch transactions cost the same as a single write (bottleneck is Raft, not SQLite).

Run benchmarks yourself:

```bash
go test -bench=. -benchmem -benchtime=3s -timeout 300s .
go test -run TestLatencyDistribution -v -timeout 120s .
```

## Limitations

- **No non-deterministic SQL functions** — `RANDOM()`, `datetime('now')`, etc. produce different values on each node. Pass values as parameters instead.
- **Single writer** — all writes serialize through the Raft leader. This is inherent to Raft consensus.
- **Statement-level replication** — SQL statements (not WAL pages) are replicated. This is simpler but means the above limitation applies.
- **Reads in transactions** — `tx.Query()` reads from local SQLite and won't see uncommitted writes buffered in the same transaction.
- **Minimum 3 nodes** for fault tolerance — 2 nodes is worse than 1 (no quorum if either fails).

## License

BSD-3-Clause
