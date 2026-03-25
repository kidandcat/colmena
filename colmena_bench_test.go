package colmena

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- Single Node Benchmarks ---

func BenchmarkSingleNode_WriteSequential(b *testing.B) {
	node := benchNode(b)
	db := node.DB()
	db.Exec("CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := db.Exec("INSERT INTO bench (val) VALUES (?)", fmt.Sprintf("val-%d", i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSingleNode_ReadSequential(b *testing.B) {
	node := benchNode(b)
	db := node.DB()
	db.Exec("CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
	// Seed data.
	for i := 0; i < 1000; i++ {
		db.Exec("INSERT INTO bench (val) VALUES (?)", fmt.Sprintf("val-%d", i))
	}
	time.Sleep(200 * time.Millisecond)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var val string
		err := db.QueryRow("SELECT val FROM bench WHERE id = ?", (i%1000)+1).Scan(&val)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSingleNode_WriteParallel(b *testing.B) {
	node := benchNode(b)
	db := node.DB()
	db.Exec("CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, err := db.Exec("INSERT INTO bench (val) VALUES (?)", fmt.Sprintf("val-%d", i))
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

func BenchmarkSingleNode_ReadParallel(b *testing.B) {
	node := benchNode(b)
	db := node.DB()
	db.Exec("CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
	for i := 0; i < 1000; i++ {
		db.Exec("INSERT INTO bench (val) VALUES (?)", fmt.Sprintf("val-%d", i))
	}
	time.Sleep(200 * time.Millisecond)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			var val string
			err := db.QueryRow("SELECT val FROM bench WHERE id = ?", r.Intn(1000)+1).Scan(&val)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSingleNode_Transaction(b *testing.B) {
	node := benchNode(b)
	db := node.DB()
	db.Exec("CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := node.ExecMulti([]Statement{
			{SQL: "INSERT INTO bench (val) VALUES (?)", Args: []any{fmt.Sprintf("a-%d", i)}},
			{SQL: "INSERT INTO bench (val) VALUES (?)", Args: []any{fmt.Sprintf("b-%d", i)}},
			{SQL: "INSERT INTO bench (val) VALUES (?)", Args: []any{fmt.Sprintf("c-%d", i)}},
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- Three Node Benchmarks ---

func BenchmarkThreeNode_WriteOnLeader(b *testing.B) {
	leader, _, _ := benchCluster(b)
	db := leader.DB()
	db.Exec("CREATE TABLE IF NOT EXISTS bench3 (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
	time.Sleep(500 * time.Millisecond)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := db.Exec("INSERT INTO bench3 (val) VALUES (?)", fmt.Sprintf("val-%d", i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkThreeNode_WriteFromFollower(b *testing.B) {
	leader, follower1, _ := benchCluster(b)
	leader.DB().Exec("CREATE TABLE IF NOT EXISTS bench3f (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
	time.Sleep(500 * time.Millisecond)
	fdb := follower1.DB()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := fdb.Exec("INSERT INTO bench3f (val) VALUES (?)", fmt.Sprintf("val-%d", i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkThreeNode_ReadFromFollower(b *testing.B) {
	leader, follower1, _ := benchCluster(b)
	ldb := leader.DB()
	ldb.Exec("CREATE TABLE IF NOT EXISTS bench3r (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
	for i := 0; i < 1000; i++ {
		ldb.Exec("INSERT INTO bench3r (val) VALUES (?)", fmt.Sprintf("val-%d", i))
	}
	time.Sleep(1 * time.Second)

	fdb := follower1.DB()
	ctx := WithConsistency(context.Background(), ConsistencyNone)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var val string
		err := fdb.QueryRowContext(ctx, "SELECT val FROM bench3r WHERE id = ?", (i%1000)+1).Scan(&val)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- Latency Distribution Test ---

func TestLatencyDistribution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping latency test in short mode")
	}

	leader, follower1, _ := benchCluster(t)
	ldb := leader.DB()
	fdb := follower1.DB()

	ldb.Exec("CREATE TABLE IF NOT EXISTS latency_test (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
	time.Sleep(500 * time.Millisecond)

	// Seed some data for reads.
	for i := 0; i < 100; i++ {
		ldb.Exec("INSERT INTO latency_test (val) VALUES (?)", fmt.Sprintf("seed-%d", i))
	}
	time.Sleep(500 * time.Millisecond)

	const samples = 200

	// Write latency on leader.
	writeLeader := measureLatency(samples, func() error {
		_, err := ldb.Exec("INSERT INTO latency_test (val) VALUES (?)", "bench")
		return err
	})

	// Write latency from follower (forwarded).
	writeFollower := measureLatency(samples, func() error {
		_, err := fdb.Exec("INSERT INTO latency_test (val) VALUES (?)", "bench")
		return err
	})

	// Read latency on leader (strong).
	ctx := WithConsistency(context.Background(), ConsistencyStrong)
	readStrong := measureLatency(samples, func() error {
		var v string
		return ldb.QueryRowContext(ctx, "SELECT val FROM latency_test WHERE id = ?", rand.Intn(100)+1).Scan(&v)
	})

	// Read latency on follower (none/local).
	ctxNone := WithConsistency(context.Background(), ConsistencyNone)
	readLocal := measureLatency(samples, func() error {
		var v string
		return fdb.QueryRowContext(ctxNone, "SELECT val FROM latency_test WHERE id = ?", rand.Intn(100)+1).Scan(&v)
	})

	t.Logf("\n=== Latency Distribution (%d samples each) ===\n", samples)
	t.Logf("%-30s %10s %10s %10s %10s", "Operation", "P50", "P95", "P99", "Max")
	t.Logf("%-30s %10s %10s %10s %10s", "-----", "---", "---", "---", "---")
	printLatency(t, "Write (leader)", writeLeader)
	printLatency(t, "Write (follower→leader)", writeFollower)
	printLatency(t, "Read (strong, leader)", readStrong)
	printLatency(t, "Read (local, follower)", readLocal)

	// Throughput test.
	t.Logf("\n=== Throughput (5s sustained) ===")

	// Write throughput.
	writeThroughput := measureThroughput(5*time.Second, 4, func() error {
		_, err := ldb.Exec("INSERT INTO latency_test (val) VALUES (?)", "tp")
		return err
	})
	t.Logf("Write throughput (leader, 4 goroutines): %d ops/sec", writeThroughput)

	// Read throughput.
	readThroughput := measureThroughput(5*time.Second, 8, func() error {
		var v string
		return fdb.QueryRowContext(ctxNone, "SELECT val FROM latency_test WHERE id = ?", rand.Intn(100)+1).Scan(&v)
	})
	t.Logf("Read throughput (follower local, 8 goroutines): %d ops/sec", readThroughput)
}

// --- Helpers ---

func benchNode(b testing.TB) *Node {
	b.Helper()
	port := freePort(b)

	cfg := Config{
		NodeID:            fmt.Sprintf("bench-%d", port),
		DataDir:           b.TempDir(),
		Bind:              fmt.Sprintf("127.0.0.1:%d", port),
		Bootstrap:         true,
		HeartbeatTimeout:  200 * time.Millisecond,
		ElectionTimeout:   200 * time.Millisecond,
		SnapshotThreshold: 8192,
		ApplyTimeout:      5 * time.Second,
		LogOutput:         io.Discard,
	}

	node, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { node.Close() })
	if err := node.WaitForLeader(5 * time.Second); err != nil {
		b.Fatal(err)
	}
	return node
}

func benchCluster(t testing.TB) (leader, follower1, follower2 *Node) {
	t.Helper()

	port1 := freePort(t)
	leader, err := New(Config{
		NodeID:            fmt.Sprintf("bench-leader-%d", port1),
		DataDir:           t.TempDir(),
		Bind:              fmt.Sprintf("127.0.0.1:%d", port1),
		Bootstrap:         true,
		HeartbeatTimeout:  200 * time.Millisecond,
		ElectionTimeout:   200 * time.Millisecond,
		SnapshotThreshold: 8192,
		ApplyTimeout:      5 * time.Second,
		LogOutput:         io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { leader.Close() })
	if err := leader.WaitForLeader(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	leaderAddr := leader.config.Bind
	mkFollower := func() *Node {
		p := freePort(t)
		n, err := New(Config{
			NodeID:           fmt.Sprintf("bench-follower-%d", p),
			DataDir:          t.TempDir(),
			Bind:             fmt.Sprintf("127.0.0.1:%d", p),
			Join:             []string{leaderAddr},
			HeartbeatTimeout: 200 * time.Millisecond,
			ElectionTimeout:  200 * time.Millisecond,
			ApplyTimeout:     5 * time.Second,
			LogOutput:        io.Discard,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { n.Close() })
		return n
	}

	follower1 = mkFollower()
	follower2 = mkFollower()
	time.Sleep(1 * time.Second) // let cluster stabilize
	return
}

type latencyResult struct {
	p50, p95, p99, max time.Duration
}

func measureLatency(n int, fn func() error) latencyResult {
	durations := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		start := time.Now()
		if err := fn(); err != nil {
			continue
		}
		durations = append(durations, time.Since(start))
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	l := len(durations)
	if l == 0 {
		return latencyResult{}
	}
	return latencyResult{
		p50: durations[l*50/100],
		p95: durations[l*95/100],
		p99: durations[min(l*99/100, l-1)],
		max: durations[l-1],
	}
}

func printLatency(t *testing.T, label string, r latencyResult) {
	t.Logf("%-30s %10s %10s %10s %10s", label, r.p50.Round(time.Microsecond), r.p95.Round(time.Microsecond), r.p99.Round(time.Microsecond), r.max.Round(time.Microsecond))
}

func measureThroughput(duration time.Duration, goroutines int, fn func() error) int64 {
	var ops atomic.Int64
	var wg sync.WaitGroup
	done := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					if fn() == nil {
						ops.Add(1)
					}
				}
			}
		}()
	}

	time.Sleep(duration)
	close(done)
	wg.Wait()

	return ops.Load() / int64(duration.Seconds())
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
