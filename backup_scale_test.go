package colmena

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// failWALBackend wraps a backend and fails WriteWALSegment while the
// fail flag is set. Snapshots always succeed so generations can open.
type failWALBackend struct {
	BackupBackend
	fail atomic.Bool
	puts atomic.Int64
}

func (f *failWALBackend) WriteWALSegment(ctx context.Context, generation string, seg WALSegmentInfo, r io.Reader, size int64) error {
	f.puts.Add(1)
	if f.fail.Load() {
		io.Copy(io.Discard, r)
		return fmt.Errorf("simulated s3 outage")
	}
	return f.BackupBackend.WriteWALSegment(ctx, generation, seg, r, size)
}

func TestBackupChunkedSegmentsRestore(t *testing.T) {
	// Force many small segments, then restore.
	tb := newTestBackup(t, BackupConfig{
		SegmentMaxBytes:     200, // tiny
		CheckpointThreshold: 1 << 20,
	})
	var want []string
	for i := 0; i < 40; i++ {
		v := fmt.Sprintf("chunked-%04d-%s", i, strings.Repeat("x", 32))
		tb.insert(t, v)
		want = append(want, v)
	}
	tb.clock.advance(time.Second)
	tb.sync(t)

	segs, err := tb.backend.WALSegments(context.Background(), tb.bm.Status().Generation)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 2 {
		t.Fatalf("expected multiple WAL segments, got %d", len(segs))
	}
	// Offsets must be strictly increasing within index 0.
	for i := 1; i < len(segs); i++ {
		if segs[i].Index == segs[i-1].Index && segs[i].Offset <= segs[i-1].Offset {
			t.Fatalf("segments not ordered: %+v then %+v", segs[i-1], segs[i])
		}
	}

	got := tb.restoreValues(t, time.Time{})
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("restored %d rows, want %d (tail got=%v want=%v)",
			len(got), len(want), got[max(0, len(got)-3):], want[max(0, len(want)-3):])
	}
}

func TestBackupCheckpointDespiteUploadFailure(t *testing.T) {
	// Even when remote Put fails, fully spooled WAL past MaxWALBytes must
	// still TRUNCATE so the live process cannot OOM on an unbounded WAL.
	inner, err := NewLocalBackend(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fb := &failWALBackend{BackupBackend: inner}
	fb.fail.Store(true)

	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	node, err := New(Config{DataDir: t.TempDir(), LogOutput: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { node.Close() })
	st, err := node.stores.get("default")
	if err != nil {
		t.Fatal(err)
	}
	cfg := BackupConfig{
		NewBackend:          func(db string) (BackupBackend, error) { return fb, nil },
		CheckpointThreshold: 1,
		MaxWALBytes:         1,
		SegmentMaxBytes:     1 << 20,
		now:                 clock.now,
	}
	cfg.applyDefaults()
	cfg.now = clock.now
	bm, err := newBackupManager("default", st, cfg, func(string, ...any) {})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.writer.Exec("PRAGMA wal_autocheckpoint = 0"); err != nil {
		t.Fatal(err)
	}
	// Snapshot must succeed (does not go through failWAL).
	if err := bm.takeSnapshot(); err != nil {
		t.Fatal(err)
	}
	db := node.DB()
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		if _, err := db.Exec(`INSERT INTO t (v) VALUES (?)`, fmt.Sprintf("row-%d-%s", i, strings.Repeat("z", 64))); err != nil {
			t.Fatal(err)
		}
	}

	// sync: spool + checkpoint succeed; upload fails.
	err = bm.sync()
	if err == nil {
		t.Fatal("expected upload error")
	}
	if !strings.Contains(err.Error(), "simulated s3 outage") {
		t.Fatalf("unexpected error: %v", err)
	}

	stStatus := bm.Status()
	if stStatus.PendingSpool == 0 {
		t.Fatal("expected pending spool segments after failed upload")
	}
	// WAL should have been checkpointed (file gone or tiny) because MaxWALBytes=1
	// and everything was spooled.
	walPath := st.dbPath + "-wal"
	if info, err := os.Stat(walPath); err == nil && info.Size() > 4096 {
		t.Fatalf("WAL still large after forced checkpoint: %d bytes", info.Size())
	}

	// Recover: clear fail flag and sync again — pending must upload, restore works.
	fb.fail.Store(false)
	clock.advance(time.Second)
	if err := bm.sync(); err != nil {
		t.Fatalf("recovery sync: %v", err)
	}
	if bm.Status().PendingSpool != 0 {
		t.Fatalf("pending spool after recovery = %d", bm.Status().PendingSpool)
	}

	dir := t.TempDir()
	if err := Restore(context.Background(), inner, dir); err != nil {
		t.Fatal(err)
	}
	rdb, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "default.db")+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()
	var n int
	if err := rdb.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 30 {
		t.Fatalf("restored rows = %d, want 30", n)
	}
}

func TestBackupStreamingSnapshotLargeish(t *testing.T) {
	// ~a few MB of payload: enough to exercise streaming gzip path without
	// making CI painful. Integrity + restore is the contract.
	tb := newTestBackup(t, BackupConfig{})
	payload := strings.Repeat("0123456789abcdef", 256) // 4 KiB
	for i := 0; i < 500; i++ {
		tb.insert(t, fmt.Sprintf("%d-%s", i, payload))
	}
	tb.clock.advance(time.Hour)
	if err := tb.bm.takeSnapshot(); err != nil {
		t.Fatal(err)
	}
	// Post-snapshot WAL data
	tb.insert(t, "after")
	tb.clock.advance(time.Second)
	tb.sync(t)

	got := tb.restoreValues(t, time.Time{})
	if len(got) != 501 || got[len(got)-1] != "after" {
		t.Fatalf("restore size=%d tail=%v", len(got), got[max(0, len(got)-2):])
	}
}

func TestStoreMemoryPragmas(t *testing.T) {
	node, err := New(Config{DataDir: t.TempDir(), LogOutput: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { node.Close() })
	db := node.DB()
	var cache, mmap int64
	if err := db.QueryRow(`PRAGMA cache_size`).Scan(&cache); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`PRAGMA mmap_size`).Scan(&mmap); err != nil {
		t.Fatal(err)
	}
	// Negative cache_size is KiB; we set -65536 (64 MiB).
	if cache != -int64(defaultCacheSizeKiB) {
		t.Fatalf("cache_size = %d, want %d", cache, -defaultCacheSizeKiB)
	}
	if mmap != int64(defaultMmapSize) {
		t.Fatalf("mmap_size = %d, want %d", mmap, defaultMmapSize)
	}
}

func TestCopyFileGzipRoundTrip(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "in.bin")
	dst := filepath.Join(dir, "out.gz")
	data := []byte(strings.Repeat("colmena-stream-", 1000))
	if err := os.WriteFile(src, data, 0o644); err != nil {
		t.Fatal(err)
	}
	n, err := copyFileGzip(src, dst)
	if err != nil {
		t.Fatal(err)
	}
	if n <= 0 {
		t.Fatal("gzip size 0")
	}
	// Gunzip and compare.
	f, err := os.Open(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	outPath := filepath.Join(dir, "out.bin")
	if err := writeGunzipped(outPath, f); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("round-trip mismatch len got=%d want=%d", len(got), len(data))
	}
}

func TestBackupNoLockDuringUpload(t *testing.T) {
	// A slow upload must not block concurrent DB writes. We inject a backend
	// that blocks WriteWALSegment until a write has been observed.
	inner, _ := NewLocalBackend(t.TempDir())
	var (
		blocked = make(chan struct{})
		proceed = make(chan struct{})
		once    sync.Once
	)
	slow := &blockingWALBackend{
		BackupBackend: inner,
		onWAL: func() {
			once.Do(func() {
				close(blocked)
				<-proceed
			})
		},
	}

	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	node, err := New(Config{DataDir: t.TempDir(), LogOutput: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { node.Close() })
	st, _ := node.stores.get("default")
	cfg := BackupConfig{
		NewBackend: func(db string) (BackupBackend, error) { return slow, nil },
		now:        clock.now,
	}
	cfg.applyDefaults()
	cfg.now = clock.now
	bm, err := newBackupManager("default", st, cfg, func(string, ...any) {})
	if err != nil {
		t.Fatal(err)
	}
	st.writer.Exec("PRAGMA wal_autocheckpoint = 0")
	if err := bm.takeSnapshot(); err != nil {
		t.Fatal(err)
	}
	db := node.DB()
	db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
	db.Exec(`INSERT INTO t (v) VALUES ('pre')`)

	// Start sync in background (will block in upload).
	errCh := make(chan error, 1)
	go func() {
		clock.advance(time.Second)
		errCh <- bm.sync()
	}()

	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("upload never blocked")
	}

	// While upload is stuck, writes must still succeed promptly.
	done := make(chan error, 1)
	go func() {
		_, err := db.Exec(`INSERT INTO t (v) VALUES ('during-upload')`)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write during upload: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write blocked by backup upload lock")
	}

	close(proceed)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

type blockingWALBackend struct {
	BackupBackend
	onWAL func()
}

func (b *blockingWALBackend) WriteWALSegment(ctx context.Context, generation string, seg WALSegmentInfo, r io.Reader, size int64) error {
	if b.onWAL != nil {
		b.onWAL()
	}
	return b.BackupBackend.WriteWALSegment(ctx, generation, seg, r, size)
}
