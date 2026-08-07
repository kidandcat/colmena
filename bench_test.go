package colmena

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// BenchmarkWriteThroughput measures single-writer INSERT rate through
// Colmena's *sql.DB (the 1000 WPS target is an app-level batching concern;
// this bench is the floor for unbatched inserts on the host).
func BenchmarkWriteThroughput(b *testing.B) {
	node, err := New(Config{DataDir: b.TempDir(), LogOutput: io.Discard})
	if err != nil {
		b.Fatal(err)
	}
	defer node.Close()
	db := node.DB()
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		b.Fatal(err)
	}
	// Warm-up
	for i := 0; i < 100; i++ {
		if _, err := db.Exec(`INSERT INTO t (v) VALUES (?)`, "warm"); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`INSERT INTO t (v) VALUES (?)`, fmt.Sprintf("r%d", i)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWriteBatched is closer to a real 1000 WPS workload: many rows
// per transaction.
func BenchmarkWriteBatched(b *testing.B) {
	const batch = 50
	node, err := New(Config{DataDir: b.TempDir(), LogOutput: io.Discard})
	if err != nil {
		b.Fatal(err)
	}
	defer node.Close()
	db := node.DB()
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < batch; j++ {
			if _, err := tx.Exec(`INSERT INTO t (v) VALUES (?)`, fmt.Sprintf("r%d-%d", i, j)); err != nil {
				tx.Rollback()
				b.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(batch), "rows/op")
}

// BenchmarkSnapshotStream times a streaming gzip snapshot of a ~N-MB DB.
// Use -benchtime / COLMENA_BENCH_MB to scale; default is small for CI.
func BenchmarkSnapshotStream(b *testing.B) {
	mb := 8
	if v := os.Getenv("COLMENA_BENCH_MB"); v != "" {
		fmt.Sscanf(v, "%d", &mb)
	}
	dir := b.TempDir()
	backend, err := NewLocalBackend(filepath.Join(dir, "bak"))
	if err != nil {
		b.Fatal(err)
	}
	node, err := New(Config{DataDir: filepath.Join(dir, "data"), LogOutput: io.Discard})
	if err != nil {
		b.Fatal(err)
	}
	defer node.Close()
	db := node.DB()
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)`); err != nil {
		b.Fatal(err)
	}
	// ~1 KiB per row → mb * 1024 rows roughly fills mb MiB of payload.
	row := make([]byte, 1024)
	for i := range row {
		row[i] = byte(i)
	}
	tx, _ := db.Begin()
	for i := 0; i < mb*1024; i++ {
		if _, err := tx.Exec(`INSERT INTO t (v) VALUES (?)`, row); err != nil {
			b.Fatal(err)
		}
	}
	tx.Commit()
	// Checkpoint so main file holds the data.
	db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)

	st, err := node.stores.get("default")
	if err != nil {
		b.Fatal(err)
	}
	cfg := BackupConfig{
		NewBackend: func(string) (BackupBackend, error) { return backend, nil },
		now:        time.Now,
	}
	cfg.applyDefaults()
	bm, err := newBackupManager("default", st, cfg, func(string, ...any) {})
	if err != nil {
		b.Fatal(err)
	}
	st.writer.Exec("PRAGMA wal_autocheckpoint = 0")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := bm.takeSnapshot(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(mb), "db_mb")
}

// BenchmarkSyncSpoolAndUpload measures one sync cycle after a batch of writes.
func BenchmarkSyncSpoolAndUpload(b *testing.B) {
	dir := b.TempDir()
	backend, _ := NewLocalBackend(filepath.Join(dir, "bak"))
	node, err := New(Config{DataDir: filepath.Join(dir, "data"), LogOutput: io.Discard})
	if err != nil {
		b.Fatal(err)
	}
	defer node.Close()
	db := node.DB()
	db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
	st, _ := node.stores.get("default")
	cfg := BackupConfig{
		NewBackend: func(string) (BackupBackend, error) { return backend, nil },
		now:        time.Now,
	}
	cfg.applyDefaults()
	bm, err := newBackupManager("default", st, cfg, func(string, ...any) {})
	if err != nil {
		b.Fatal(err)
	}
	st.writer.Exec("PRAGMA wal_autocheckpoint = 0")
	if err := bm.takeSnapshot(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for j := 0; j < 20; j++ {
			db.Exec(`INSERT INTO t (v) VALUES (?)`, fmt.Sprintf("b%d-%d", i, j))
		}
		b.StartTimer()
		if err := bm.sync(); err != nil {
			b.Fatal(err)
		}
	}
}
