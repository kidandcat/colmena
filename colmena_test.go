package colmena

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// testNode creates a bootstrapped single node for testing.
func testNode(t *testing.T, opts ...func(*Config)) *Node {
	t.Helper()
	dir := t.TempDir()
	port := freePort(t)

	cfg := Config{
		NodeID:    fmt.Sprintf("test-node-%d", port),
		DataDir:   dir,
		Bind:      fmt.Sprintf("127.0.0.1:%d", port),
		Bootstrap: true,
		// Speed up Raft for tests.
		HeartbeatTimeout:  200 * time.Millisecond,
		ElectionTimeout:   200 * time.Millisecond,
		SnapshotInterval:  5 * time.Second,
		SnapshotThreshold: 100,
		ApplyTimeout:      5 * time.Second,
	}
	for _, o := range opts {
		o(&cfg)
	}

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	t.Cleanup(func() { node.Close() })

	if err := node.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("wait for leader: %v", err)
	}
	return node
}

// testJoinNode creates a node that joins an existing cluster.
func testJoinNode(t *testing.T, joinAddr string) *Node {
	t.Helper()
	dir := t.TempDir()
	port := freePort(t)

	cfg := Config{
		NodeID:           fmt.Sprintf("test-node-%d", port),
		DataDir:          dir,
		Bind:             fmt.Sprintf("127.0.0.1:%d", port),
		Join:             []string{joinAddr},
		HeartbeatTimeout: 200 * time.Millisecond,
		ElectionTimeout:  200 * time.Millisecond,
		ApplyTimeout:     5 * time.Second,
	}

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("create join node: %v", err)
	}
	t.Cleanup(func() { node.Close() })
	return node
}

func freePort(t *testing.T) int {
	t.Helper()
	// Use port 0 to get a free port, but we need two consecutive ports (Raft + RPC).
	// Find a pair of free ports by trying.
	for i := 0; i < 100; i++ {
		port := 19000 + (os.Getpid()+i*2)%10000
		// Check both port and port+1 are available.
		ln1, err1 := (&net.ListenConfig{}).Listen(context.Background(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err1 != nil {
			continue
		}
		ln1.Close()
		ln2, err2 := (&net.ListenConfig{}).Listen(context.Background(), "tcp", fmt.Sprintf("127.0.0.1:%d", port+1))
		if err2 != nil {
			continue
		}
		ln2.Close()
		return port
	}
	t.Fatal("could not find free port pair")
	return 0
}

// --- Single Node Tests ---

func TestSingleNode_CreateTable(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Verify table exists.
	var count int
	err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='test'").Scan(&count)
	if err != nil {
		t.Fatalf("query sqlite_master: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 table, got %d", count)
	}
}

func TestSingleNode_InsertAndSelect(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Insert.
	result, err := db.Exec("INSERT INTO kv (key, value) VALUES (?, ?)", "hello", "world")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		t.Fatalf("expected 1 row affected, got %d", rows)
	}

	// Wait briefly for Raft apply to propagate to reader.
	time.Sleep(100 * time.Millisecond)

	// Select.
	var value string
	err = db.QueryRow("SELECT value FROM kv WHERE key = ?", "hello").Scan(&value)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if value != "world" {
		t.Fatalf("expected 'world', got %q", value)
	}
}

func TestSingleNode_MultipleInserts(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	for i := 0; i < 50; i++ {
		_, err = db.Exec("INSERT INTO items (name) VALUES (?)", fmt.Sprintf("item-%d", i))
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	var count int
	err = db.QueryRow("SELECT count(*) FROM items").Scan(&count)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 50 {
		t.Fatalf("expected 50 rows, got %d", count)
	}
}

func TestSingleNode_Transaction(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE accounts (id TEXT PRIMARY KEY, balance INTEGER)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Use ExecMulti for atomic batch insert.
	_, err = node.ExecMulti([]Statement{
		{SQL: "INSERT INTO accounts (id, balance) VALUES (?, ?)", Args: []any{"alice", 100}},
		{SQL: "INSERT INTO accounts (id, balance) VALUES (?, ?)", Args: []any{"bob", 200}},
		{SQL: "INSERT INTO accounts (id, balance) VALUES (?, ?)", Args: []any{"charlie", 300}},
	})
	if err != nil {
		t.Fatalf("exec multi: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	var total int
	err = db.QueryRow("SELECT sum(balance) FROM accounts").Scan(&total)
	if err != nil {
		t.Fatalf("sum: %v", err)
	}
	if total != 600 {
		t.Fatalf("expected total 600, got %d", total)
	}
}

func TestSingleNode_TransactionViaTx(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Begin transaction via database/sql.
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	tx.Exec("INSERT INTO data (val) VALUES (?)", "one")
	tx.Exec("INSERT INTO data (val) VALUES (?)", "two")
	tx.Exec("INSERT INTO data (val) VALUES (?)", "three")
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	var count int
	err = db.QueryRow("SELECT count(*) FROM data").Scan(&count)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 rows, got %d", count)
	}
}

func TestSingleNode_TransactionRollback(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	tx.Exec("INSERT INTO data (val) VALUES (?)", "one")
	tx.Exec("INSERT INTO data (val) VALUES (?)", "two")
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	var count int
	err = db.QueryRow("SELECT count(*) FROM data").Scan(&count)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows after rollback, got %d", count)
	}
}

func TestSingleNode_ConsistencyStrong(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO kv (key, value) VALUES (?, ?)", "k1", "v1")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Strong consistency read.
	ctx := WithConsistency(context.Background(), ConsistencyStrong)
	var value string
	err = db.QueryRowContext(ctx, "SELECT value FROM kv WHERE key = ?", "k1").Scan(&value)
	if err != nil {
		t.Fatalf("strong read: %v", err)
	}
	if value != "v1" {
		t.Fatalf("expected 'v1', got %q", value)
	}
}

// --- Multi Node Tests ---

func TestMultiNode_ThreeNodeCluster(t *testing.T) {
	// Bootstrap leader.
	leader := testNode(t)
	leaderAddr := leader.config.Bind

	// Give leader time to stabilize.
	time.Sleep(500 * time.Millisecond)

	// Join two followers.
	follower1 := testJoinNode(t, leaderAddr)
	follower2 := testJoinNode(t, leaderAddr)

	// Wait for cluster to stabilize.
	time.Sleep(1 * time.Second)

	// Verify cluster has 3 nodes.
	servers, err := leader.Nodes()
	if err != nil {
		t.Fatalf("get nodes: %v", err)
	}
	if len(servers) != 3 {
		t.Fatalf("expected 3 servers, got %d", len(servers))
	}

	// Write on leader.
	db := leader.DB()
	_, err = db.Exec("CREATE TABLE cluster_test (id INTEGER PRIMARY KEY, node TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO cluster_test (node) VALUES (?)", "leader")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Wait for replication.
	time.Sleep(500 * time.Millisecond)

	// Read from each follower (None consistency = local read).
	for i, f := range []*Node{follower1, follower2} {
		fdb := f.DB()
		ctx := WithConsistency(context.Background(), ConsistencyNone)
		var node string
		err = fdb.QueryRowContext(ctx, "SELECT node FROM cluster_test WHERE id = 1").Scan(&node)
		if err != nil {
			t.Fatalf("follower%d read: %v", i+1, err)
		}
		if node != "leader" {
			t.Fatalf("follower%d: expected 'leader', got %q", i+1, node)
		}
	}
}

func TestMultiNode_LeaderForwarding(t *testing.T) {
	// Bootstrap leader.
	leader := testNode(t)
	leaderAddr := leader.config.Bind

	time.Sleep(500 * time.Millisecond)

	// Join a follower.
	follower := testJoinNode(t, leaderAddr)

	time.Sleep(1 * time.Second)

	// Create table on leader.
	_, err := leader.DB().Exec("CREATE TABLE forward_test (id INTEGER PRIMARY KEY, source TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Write FROM the follower — should be forwarded to leader.
	fdb := follower.DB()
	_, err = fdb.Exec("INSERT INTO forward_test (source) VALUES (?)", "from-follower")
	if err != nil {
		t.Fatalf("follower write (forwarded): %v", err)
	}

	// Wait for replication.
	time.Sleep(500 * time.Millisecond)

	// Verify data on leader.
	var source string
	err = leader.DB().QueryRow("SELECT source FROM forward_test WHERE id = 1").Scan(&source)
	if err != nil {
		t.Fatalf("leader read: %v", err)
	}
	if source != "from-follower" {
		t.Fatalf("expected 'from-follower', got %q", source)
	}

	// Verify data on follower (local read).
	ctx := WithConsistency(context.Background(), ConsistencyNone)
	err = fdb.QueryRowContext(ctx, "SELECT source FROM forward_test WHERE id = 1").Scan(&source)
	if err != nil {
		t.Fatalf("follower read: %v", err)
	}
	if source != "from-follower" {
		t.Fatalf("expected 'from-follower', got %q", source)
	}
}

func TestMultiNode_SnapshotRestore(t *testing.T) {
	// This test verifies that the FSM snapshot/restore path works correctly.
	// It forces a Raft snapshot on the leader and then joins a follower which
	// must receive the snapshot to catch up (since old logs are compacted).

	// Bootstrap leader with very low snapshot threshold.
	leader := testNode(t, func(cfg *Config) {
		cfg.SnapshotThreshold = 4
		cfg.SnapshotInterval = 1 * time.Second
	})
	leaderAddr := leader.config.Bind
	time.Sleep(500 * time.Millisecond)

	// Write enough data to trigger at least one snapshot.
	db := leader.DB()
	_, err := db.Exec("CREATE TABLE snap_test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < 20; i++ {
		_, err = db.Exec("INSERT INTO snap_test (val) VALUES (?)", fmt.Sprintf("row-%d", i))
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// Wait for Raft to take a snapshot and compact logs.
	time.Sleep(3 * time.Second)

	// Verify the leader has data.
	var count int
	err = db.QueryRow("SELECT count(*) FROM snap_test").Scan(&count)
	if err != nil {
		t.Fatalf("leader count: %v", err)
	}
	if count != 20 {
		t.Fatalf("leader: expected 20 rows, got %d", count)
	}

	// Now join a follower. Since old logs may be compacted, Raft should send
	// an InstallSnapshot to bring the follower up to date.
	follower := testJoinNode(t, leaderAddr)

	// Wait for snapshot transfer and replication.
	time.Sleep(3 * time.Second)

	// Read from follower.
	fdb := follower.DB()
	ctx := WithConsistency(context.Background(), ConsistencyNone)
	var fCount int
	err = fdb.QueryRowContext(ctx, "SELECT count(*) FROM snap_test").Scan(&fCount)
	if err != nil {
		t.Fatalf("follower count: %v", err)
	}
	if fCount != 20 {
		t.Fatalf("follower: expected 20 rows, got %d", fCount)
	}
}

// --- Backup Tests ---

func TestBackup_LocalBackendSnapshotAndRestore(t *testing.T) {
	backupDir := filepath.Join(t.TempDir(), "backups")
	backend, err := NewLocalBackend(backupDir)
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}

	// Create node with backup enabled.
	node := testNode(t, func(cfg *Config) {
		cfg.Backup = &BackupConfig{
			Backend:          backend,
			SyncInterval:     200 * time.Millisecond,
			SnapshotInterval: 10 * time.Minute, // won't trigger during test
		}
	})

	db := node.DB()

	// Create table and insert data.
	_, err = db.Exec("CREATE TABLE backup_test (id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	for i := 0; i < 20; i++ {
		_, err = db.Exec("INSERT INTO backup_test (data) VALUES (?)", fmt.Sprintf("row-%d", i))
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// Wait for WAL sync to happen.
	time.Sleep(1 * time.Second)

	// Verify a generation exists.
	gens, err := backend.Generations(context.Background())
	if err != nil {
		t.Fatalf("list generations: %v", err)
	}
	if len(gens) == 0 {
		t.Fatal("expected at least 1 generation")
	}

	// Close the original node.
	node.Close()

	// Restore to a new directory.
	restoreDir := t.TempDir()
	if err := Restore(context.Background(), backend, restoreDir); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Verify restored database has the data.
	restoredDBPath := filepath.Join(restoreDir, dbFileName)
	restoredDB, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)", restoredDBPath))
	if err != nil {
		t.Fatalf("open restored db: %v", err)
	}
	defer restoredDB.Close()

	var count int
	err = restoredDB.QueryRow("SELECT count(*) FROM backup_test").Scan(&count)
	if err != nil {
		t.Fatalf("count restored: %v", err)
	}
	if count != 20 {
		t.Fatalf("expected 20 rows in restored db, got %d", count)
	}
}

func TestBackup_RestoreAndBootstrapNewNode(t *testing.T) {
	backupDir := filepath.Join(t.TempDir(), "backups")
	backend, err := NewLocalBackend(backupDir)
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}

	// Create original node with data.
	node := testNode(t, func(cfg *Config) {
		cfg.Backup = &BackupConfig{
			Backend:          backend,
			SyncInterval:     200 * time.Millisecond,
			SnapshotInterval: 10 * time.Minute,
		}
	})

	db := node.DB()
	_, err = db.Exec("CREATE TABLE restore_test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO restore_test (val) VALUES (?)", "original-data")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Wait for backup.
	time.Sleep(1 * time.Second)
	node.Close()

	// Restore to new location and bootstrap a new node from it.
	newDataDir := t.TempDir()
	if err := Restore(context.Background(), backend, newDataDir); err != nil {
		t.Fatalf("restore: %v", err)
	}

	port := freePort(t)
	newNode, err := New(Config{
		NodeID:           fmt.Sprintf("restored-%d", port),
		DataDir:          newDataDir,
		Bind:             fmt.Sprintf("127.0.0.1:%d", port),
		Bootstrap:        true,
		HeartbeatTimeout: 200 * time.Millisecond,
		ElectionTimeout:  200 * time.Millisecond,
		ApplyTimeout:     5 * time.Second,
	})
	if err != nil {
		t.Fatalf("create restored node: %v", err)
	}
	defer newNode.Close()

	if err := newNode.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("wait for leader: %v", err)
	}

	// Verify data is there.
	time.Sleep(200 * time.Millisecond)
	var val string
	err = newNode.DB().QueryRow("SELECT val FROM restore_test WHERE id = 1").Scan(&val)
	if err != nil {
		t.Fatalf("query restored node: %v", err)
	}
	if val != "original-data" {
		t.Fatalf("expected 'original-data', got %q", val)
	}
}

// --- Node Status Tests ---

func TestNode_IsLeader(t *testing.T) {
	node := testNode(t)
	if !node.IsLeader() {
		t.Fatal("single bootstrapped node should be leader")
	}
}

func TestNode_Stats(t *testing.T) {
	node := testNode(t)
	stats := node.Stats()
	if stats["state"] != "Leader" {
		t.Fatalf("expected state 'Leader', got %q", stats["state"])
	}
}

func TestNode_ClusterInfo(t *testing.T) {
	node := testNode(t)
	servers, err := node.Nodes()
	if err != nil {
		t.Fatalf("get nodes: %v", err)
	}
	if len(servers) != 1 {
		t.Fatalf("expected 1 server, got %d", len(servers))
	}
	if string(servers[0].ID) != node.NodeID() {
		t.Fatalf("expected node ID %q, got %q", node.NodeID(), servers[0].ID)
	}
}
