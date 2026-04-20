package colmena

import (
	"archive/tar"
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// TestCommandEnvelope_RoundTrip verifies that a command marshaled with the
// current envelope decodes back to the same structure.
func TestCommandEnvelope_RoundTrip(t *testing.T) {
	cmd := &Command{
		Type: CommandExecute,
		DB:   "default",
		Statements: []Statement{
			{SQL: "INSERT INTO t(x) VALUES(?)", Args: []interface{}{42}},
		},
	}
	data, err := marshalCommand(cmd)
	if err != nil {
		t.Fatalf("marshalCommand: %v", err)
	}
	if !hasEnvelopeMagic(data) {
		t.Fatalf("expected envelope magic at start of marshaled command")
	}
	if data[8] != byte(FormatKindCommand) {
		t.Fatalf("expected kind=Command, got %d", data[8])
	}
	if data[9] != CommandFormatVersion {
		t.Fatalf("expected version=%d, got %d", CommandFormatVersion, data[9])
	}
	got, err := unmarshalCommand(data)
	if err != nil {
		t.Fatalf("unmarshalCommand: %v", err)
	}
	if got.Type != cmd.Type || got.DB != cmd.DB || len(got.Statements) != 1 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

// TestCommandEnvelope_LegacyJSON verifies that unmarshalCommand still reads
// payloads written by pre-v0.6 Colmena (raw JSON, no envelope). This is the
// backward-compat guarantee that lets a cluster's existing Raft log survive
// an upgrade to v0.6+.
func TestCommandEnvelope_LegacyJSON(t *testing.T) {
	// Byte-identical to what v0.5.x wrote: json.Marshal of a Command.
	legacy := []byte(`{"type":0,"db":"default","stmts":[{"sql":"SELECT 1"}]}`)
	got, err := unmarshalCommand(legacy)
	if err != nil {
		t.Fatalf("unmarshalCommand legacy: %v", err)
	}
	if got.DB != "default" || len(got.Statements) != 1 || got.Statements[0].SQL != "SELECT 1" {
		t.Fatalf("legacy decode mismatch: %+v", got)
	}
}

// TestCommandEnvelope_UnknownVersion verifies we refuse future envelope
// versions instead of silently producing a zero-value Command.
func TestCommandEnvelope_UnknownVersion(t *testing.T) {
	// Envelope with kind=Command, version=99 — simulates a future release.
	payload := []byte(`{"type":0,"db":"d","stmts":[]}`)
	data := encodeEnvelope(FormatKindCommand, 99, payload)
	_, err := unmarshalCommand(data)
	if err == nil || !errors.Is(err, ErrUnsupportedFormatVersion) {
		t.Fatalf("expected ErrUnsupportedFormatVersion, got %v", err)
	}
}

// TestCommandEnvelope_Garbage verifies random bytes that look neither like
// JSON nor an envelope are rejected (not interpreted as legacy JSON that
// happens to parse into a zero Command).
func TestCommandEnvelope_Garbage(t *testing.T) {
	_, err := unmarshalCommand([]byte{0xff, 0xee, 0xdd})
	if err == nil {
		t.Fatalf("expected error on garbage payload")
	}
}

// TestSnapshotEnvelope_RoundTrip writes a snapshot with the current envelope
// and restores it into a second manager, verifying both the byte shape and
// the restored data.
func TestSnapshotEnvelope_RoundTrip(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	src := newStoreManager(srcDir, 4)
	defer src.close()

	st, err := src.get("default")
	if err != nil {
		t.Fatalf("get default: %v", err)
	}
	if _, err := st.execute(Statement{SQL: "CREATE TABLE t(x INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := st.execute(Statement{SQL: "INSERT INTO t VALUES(7)"}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var buf bytes.Buffer
	if err := src.snapshot(&buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if !hasEnvelopeMagic(buf.Bytes()) {
		t.Fatalf("expected envelope magic at start of snapshot")
	}
	if buf.Bytes()[8] != byte(FormatKindSnapshot) {
		t.Fatalf("expected kind=Snapshot, got %d", buf.Bytes()[8])
	}

	dst := newStoreManager(dstDir, 4)
	defer dst.close()
	if err := dst.restore(&buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	assertRowExists(t, dst, "default", "SELECT x FROM t WHERE x=7", 7)
}

// TestSnapshotEnvelope_LegacyTar verifies that unenveloped tar snapshots
// (shape written by v0.3..v0.5) are still accepted on restore.
func TestSnapshotEnvelope_LegacyTar(t *testing.T) {
	// Build a legitimate SQLite file using the real driver — we can't craft
	// a valid SQLite page-for-page header by hand, so write, then archive.
	srcDir := t.TempDir()
	dbPath := filepath.Join(srcDir, "default.db")
	seedSQLiteFile(t, dbPath, "CREATE TABLE t(x INTEGER); INSERT INTO t VALUES(13);")
	dbBytes, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db: %v", err)
	}

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: "default.db", Size: int64(len(dbBytes)), Mode: 0644}); err != nil {
		t.Fatalf("tar header: %v", err)
	}
	if _, err := tw.Write(dbBytes); err != nil {
		t.Fatalf("tar write: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}

	dstDir := t.TempDir()
	dst := newStoreManager(dstDir, 4)
	defer dst.close()
	if err := dst.restore(&buf); err != nil {
		t.Fatalf("restore legacy tar: %v", err)
	}
	assertRowExists(t, dst, "default", "SELECT x FROM t WHERE x=13", 13)
}

// TestSnapshotEnvelope_LegacyRawSQLite verifies that v0.2.0 single-file
// snapshots (raw SQLite, no tar) are restored as the "default" store.
func TestSnapshotEnvelope_LegacyRawSQLite(t *testing.T) {
	srcDir := t.TempDir()
	dbPath := filepath.Join(srcDir, "legacy.db")
	seedSQLiteFile(t, dbPath, "CREATE TABLE t(x INTEGER); INSERT INTO t VALUES(99);")
	dbBytes, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db: %v", err)
	}
	if !bytes.HasPrefix(dbBytes, sqliteMagic) {
		t.Fatalf("expected SQLite magic in seeded file, got %q", dbBytes[:16])
	}

	dstDir := t.TempDir()
	dst := newStoreManager(dstDir, 4)
	defer dst.close()
	if err := dst.restore(bytes.NewReader(dbBytes)); err != nil {
		t.Fatalf("restore legacy raw: %v", err)
	}
	assertRowExists(t, dst, "default", "SELECT x FROM t WHERE x=99", 99)
}

// TestSnapshotEnvelope_UnknownVersion verifies unknown snapshot versions are
// rejected rather than silently misinterpreted.
func TestSnapshotEnvelope_UnknownVersion(t *testing.T) {
	buf := bytes.NewBuffer(encodeEnvelope(FormatKindSnapshot, 99, []byte("not a tar")))
	sm := newStoreManager(t.TempDir(), 4)
	defer sm.close()
	err := sm.restore(buf)
	if err == nil || !errors.Is(err, ErrUnsupportedFormatVersion) {
		t.Fatalf("expected ErrUnsupportedFormatVersion, got %v", err)
	}
}

// --- helpers ---

func seedSQLiteFile(t *testing.T, path, sqlText string) {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(DELETE)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open seed db: %v", err)
	}
	defer db.Close()
	for _, stmt := range strings.Split(sqlText, ";") {
		s := strings.TrimSpace(stmt)
		if s == "" {
			continue
		}
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed exec %q: %v", s, err)
		}
	}
}

func assertRowExists(t *testing.T, sm *storeManager, db, query string, want int) {
	t.Helper()
	st, err := sm.get(db)
	if err != nil {
		t.Fatalf("get %s: %v", db, err)
	}
	rows, err := st.query(query)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("expected at least one row for %q", query)
	}
	var got int
	if err := rows.Scan(&got); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if got != want {
		t.Fatalf("row value = %d, want %d", got, want)
	}
}
