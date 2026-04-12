# Colmena Improvement Plan

Recommendations based on live benchmarks against a 3-node cluster (Fly.io leader + 2 OVH followers) and full code review.

## Benchmark Context

Cluster tested: anonat (ephemeral encrypted chat), running on `shared-cpu-1x` Fly.io + 2 OVH VPS (8GB RAM each).

| Metric | Result |
|---|---|
| HTTP read throughput | 1,181 RPS (200 concurrent) |
| HTTP write throughput | 363 RPS (100 concurrent) |
| WebSocket single-room (50 writers) | 2,800 msgs, 0% errors |
| Multi-room burst (60 VUs) | 405 rooms + 2,020 msgs, 0.24% error |
| Data integrity | **100%** (900/900 messages verified) |
| Raft replication | All 3 nodes synced to index 16,230 |
| SQLite consistency | 2,535 rooms / 4,150 messages — identical on all nodes |

Internal bench results (colmena_bench_test.go):

| Scenario | ops/sec | P50 |
|---|---|---|
| Write (1 node) | 116 | 8.6ms |
| Write (1 node, parallel/8) | 416 | 2.4ms |
| Write (3 nodes, leader) | 44 | 22.7ms |
| Write (3 nodes, follower) | 46 | 21.7ms |
| Read (local) | 161,000 | 6.2µs |

---

## 1. TLS/Auth on Raft + RPC Transport

**Priority:** Critical
**Effort:** Medium
**Impact:** Security

### Problem

Raft and RPC ports are unauthenticated plaintext TCP. During benchmarks, VPS2 (port 9100) received port-scan traffic that produced errors:

```
[ERROR] raft-net: failed to decode incoming command: error="unknown rpc type 64"
[ERROR] raft-net: failed to decode incoming command: error="unknown rpc type 71"
[ERROR] raft-net: failed to decode incoming command: error="read tcp ...->66.132.172.179:20304: connection reset by peer"
```

Bytes 64 (`@`) and 71 (`G`) are fragments of HTTP probes from bots hitting the Raft port. An attacker with network access could attempt to inject Raft commands or join the cluster as a voter.

### Approach

hashicorp/raft supports custom `StreamLayer` implementations. Wrap the existing `TCPTransport` with mutual TLS:

```go
type Config struct {
    // ...existing fields...
    TLSConfig *tls.Config // Optional: enables mTLS on Raft + RPC
}
```

- Generate a shared CA for the cluster
- Each node gets a cert signed by that CA
- Raft transport uses `tls.Listen` / `tls.Dial` with `RequireAndVerifyClientCert`
- RPC server uses the same TLS config
- Nodes without valid certs are rejected at the TCP level

Fallback (if mTLS is too heavy for some deployments): a shared secret verified during the first bytes of the connection handshake. Simpler but less robust.

### Files to change

- `config.go` — add TLS fields
- `node.go` — wrap TCPTransport and RPC listener with TLS
- `colmena_test.go` — test with TLS enabled cluster

---

## 2. Write Batching

**Priority:** High
**Effort:** Medium
**Impact:** 10-100x write throughput

### Problem

Every individual SQL statement goes through a full Raft Apply cycle:

```
JSON encode → raft.Apply() → consensus (network roundtrip) → FSM.Apply on all nodes
```

In anonat, each chat message triggers 2 Raft applies: `InsertMessage` + `TrimHistory`. At 44 ops/sec (3-node bench), this caps write throughput at ~22 messages/sec.

### Approach

Accumulate writes within a configurable time window (1-5ms) and submit them as a single `CommandExecuteMulti`:

```go
type WriteBatcher struct {
    node     *Node
    window   time.Duration // default: 2ms
    maxBatch int           // default: 100
    pending  []batchEntry
    mu       sync.Mutex
    timer    *time.Timer
}

type batchEntry struct {
    stmt   Statement
    result chan batchResult
}
```

Flow:
1. Write arrives → added to pending slice, caller blocks on result channel
2. Timer fires after `window` duration OR `maxBatch` reached
3. All pending statements submitted as one `ExecMulti`
4. Results distributed back to individual callers

This is the same pattern rqlite uses ("queued writes") and yields 10-100x throughput improvement because Raft consensus cost is amortized across many statements.

### Configuration

```go
type Config struct {
    // ...existing fields...
    BatchWindow   time.Duration // 0 = disabled (default for backwards compat)
    BatchMaxSize  int           // max statements per batch
}
```

### Files to change

- New file: `batcher.go`
- `node.go` — integrate batcher into write path
- `config.go` — add batch config fields
- `colmena_bench_test.go` — add batched write benchmarks

---

## 3. Binary Serialization (JSON → msgpack/protobuf)

**Priority:** Medium
**Effort:** Low
**Impact:** ~60% smaller Raft log, lower serialization latency

### Problem

Commands are JSON-encoded, including SQL strings and all arguments. For workloads with binary/base64 data (like anonat's encrypted ciphertext), JSON is verbose. The `raft.db` on the OVH nodes is 36MB — with binary encoding it would be ~14MB.

JSON encoding/decoding also adds CPU overhead on every Apply across all nodes.

### Approach

Replace `json.Marshal/Unmarshal` in `command.go` with msgpack (recommended) or protobuf:

```go
// command.go
func (c *Command) Marshal() ([]byte, error) {
    return msgpack.Marshal(c)
}

func UnmarshalCommand(data []byte) (*Command, error) {
    var c Command
    err := msgpack.Unmarshal(data, &c)
    return &c, err
}
```

msgpack is preferred over protobuf because:
- Drop-in replacement (no .proto files, no codegen)
- Handles `interface{}` args naturally (SQL args are `[]interface{}`)
- ~3x faster than JSON for encode/decode

### Migration

Add a version byte prefix to the command payload:
- `0x00` = JSON (existing, for backwards compat during rolling upgrade)
- `0x01` = msgpack

FSM.Apply reads the prefix and uses the right decoder. After all nodes are upgraded, new writes use msgpack.

### Files to change

- `command.go` — serialization logic
- `fsm.go` — deserialization in Apply/RPC
- `go.mod` — add msgpack dependency

---

## 4. Incremental Snapshots

**Priority:** Medium
**Effort:** Medium
**Impact:** Scales to large databases

### Problem

`VACUUM INTO` creates a full copy of the database file during snapshot. For the current 15MB colmena.db this takes <200ms. But as the DB grows:

- 1GB DB → several seconds of I/O, temporary disk doubling
- `VACUUM INTO` rebuilds the entire B-tree (not just a file copy)
- During snapshot, write throughput may degrade

### Approach

Use SQLite's Online Backup API instead of `VACUUM INTO`. modernc.org/sqlite exposes it:

```go
func (s *store) snapshot(path string) error {
    destDB, _ := sql.Open("sqlite", path)
    // Use sqlite3_backup_init / sqlite3_backup_step / sqlite3_backup_finish
    // via modernc.org/sqlite's backup interface
    // This copies pages incrementally and doesn't block readers
}
```

Benefits:
- Copies only used pages (not free pages like VACUUM)
- Doesn't block concurrent readers
- Can be done in steps with `sqlite3_backup_step(N)` to yield CPU
- No temporary disk doubling

### Backup manager

The `backupManager.takeSnapshot()` also uses `VACUUM INTO` — same fix applies. Additionally, the `PRAGMA wal_checkpoint(TRUNCATE)` after backup blocks writers. Consider `PASSIVE` checkpoint instead (non-blocking, best-effort).

### Files to change

- `store.go` — replace `VACUUM INTO` with backup API
- `backup.go` — update backup snapshot method
- `backup_local.go` — if snapshot format changes

---

## 5. Read Lease for Followers

**Priority:** Medium
**Effort:** Medium
**Impact:** ~15x faster reads on followers with ConsistencyWeak

### Problem

`ConsistencyWeak` reads on followers require an RPC roundtrip to the leader (~90µs). `ConsistencyNone` reads locally (~6µs) but provides no freshness guarantee.

Most applications want "reasonably fresh" reads without the RPC cost on every query.

### Approach

Implement a time-based read lease. The leader piggybacks a lease timestamp on Raft heartbeats (already sent every ~heartbeat_timeout/10). Followers can serve reads locally as long as the lease hasn't expired:

```go
type readLease struct {
    mu       sync.RWMutex
    validUntil time.Time
}

func (l *readLease) valid() bool {
    l.mu.RLock()
    defer l.mu.RUnlock()
    return time.Now().Before(l.validUntil)
}
```

New consistency level:

```go
ConsistencyLease  // Local reads while lease is valid, fallback to Weak
```

Lease duration = `HeartbeatTimeout / 2` (default 500ms). If a follower hasn't heard from the leader in 500ms, it falls back to RPC. This gives ~6µs reads with at most 500ms staleness.

### Files to change

- `consistency.go` — add `ConsistencyLease` level
- `node.go` — track lease from heartbeats, use in read path
- `fsm.go` — extend heartbeat/AppendEntries observation

---

## 6. Observability: Metrics Export

**Priority:** Medium
**Effort:** Low
**Impact:** Operational visibility

### Problem

No metrics are exported. During benchmarks, the only way to know replication state was reading journalctl logs and comparing snapshot indices manually. In production, you need dashboards and alerts.

### Approach

Expose a `Metrics()` method that returns structured data, and optionally integrate with Prometheus:

```go
type Metrics struct {
    // Raft
    RaftState         string        // leader/follower/candidate
    RaftTerm          uint64
    RaftLastIndex     uint64
    RaftCommitIndex   uint64
    RaftAppliedIndex  uint64
    RaftFSMPending    int
    
    // Performance
    ApplyLatency      time.Duration // rolling average
    RPCLatency        time.Duration
    SnapshotDuration  time.Duration // last snapshot
    
    // Throughput
    WritesTotal       uint64
    ReadsTotal        uint64
    RPCForwardsTotal  uint64
    
    // Health
    LastContact       time.Duration // time since last leader contact
    Peers             int
    SnapshotIndex     uint64
}
```

hashicorp/raft already exposes most of this via `raft.Stats()` — it's a matter of structuring it and adding the colmena-specific counters.

For Prometheus, expose an optional `MetricsHandler() http.Handler` that serves `/metrics`.

### Files to change

- New file: `metrics.go`
- `node.go` — instrument write/read paths
- `fsm.go` — instrument Apply

---

## 7. RPC Reconnection and Pool Health

**Priority:** Low
**Effort:** Low
**Impact:** Cluster resilience

### Problem

RPC clients in `node.rpcClients` are cached after first dial. If a TCP connection drops (network blip between Fly.io and OVH), the cached client is dead and subsequent RPC calls fail until... it's unclear when recovery happens.

### Approach

```go
type rpcPool struct {
    mu      sync.RWMutex
    clients map[string]*rpcEntry
}

type rpcEntry struct {
    client    *rpc.Client
    lastUsed  time.Time
    failures  int
}

func (p *rpcPool) get(addr string) (*rpc.Client, error) {
    // Return cached client if healthy
    // If last call failed, redial
    // If idle > 30s, health-check with a ping before returning
    // Evict entries idle > 5m
}
```

Also add a simple RPC ping method for health checks:

```go
func (n *Node) RPCPing() error { return nil }
```

### Files to change

- `node.go` — replace `rpcClients` map with pool struct

---

## Implementation Order

```
Phase 1 (security + quick wins):
  [1] TLS/Auth on Raft + RPC
  [3] Binary serialization (msgpack)
  [6] Metrics export

Phase 2 (performance):
  [2] Write batching
  [5] Read lease

Phase 3 (scale):
  [4] Incremental snapshots
  [7] RPC reconnection pool
```
