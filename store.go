package colmena

import (
	"archive/tar"
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

const dbFileName = "colmena.db"

// store manages the local SQLite database with separate writer and reader pools.
type store struct {
	dbPath    string
	writer    *sql.DB
	reader    *sql.DB
	readConns int
	mu        sync.RWMutex // protects writer/reader during restore
}

func newStore(dataDir string, readConns int) (*store, error) {
	return newStoreAt(filepath.Join(dataDir, dbFileName), readConns)
}

func newStoreAt(dbPath string, readConns int) (*store, error) {

	// Writer: single connection, WAL mode, immediate transactions.
	writerDSN := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_txlock=immediate", dbPath)
	writer, err := sql.Open("sqlite", writerDSN)
	if err != nil {
		return nil, fmt.Errorf("colmena: open writer: %w", err)
	}
	writer.SetMaxOpenConns(1)

	// Verify WAL mode is active.
	var journalMode string
	if err := writer.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		writer.Close()
		return nil, fmt.Errorf("colmena: check journal_mode: %w", err)
	}
	if journalMode != "wal" {
		writer.Close()
		return nil, fmt.Errorf("colmena: expected WAL mode, got %q", journalMode)
	}

	// Reader: multiple connections, read-only.
	readerDSN := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&mode=ro", dbPath)
	reader, err := sql.Open("sqlite", readerDSN)
	if err != nil {
		writer.Close()
		return nil, fmt.Errorf("colmena: open reader: %w", err)
	}
	reader.SetMaxOpenConns(readConns)

	return &store{
		dbPath:    dbPath,
		writer:    writer,
		reader:    reader,
		readConns: readConns,
	}, nil
}

// execute runs a write statement on the writer connection.
func (s *store) execute(stmt Statement) (ExecResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result, err := s.writer.Exec(stmt.SQL, stmt.Args...)
	if err != nil {
		return ExecResult{}, err
	}
	lastID, _ := result.LastInsertId()
	rows, _ := result.RowsAffected()
	return ExecResult{LastInsertID: lastID, RowsAffected: rows}, nil
}

// executeMulti runs multiple statements atomically in a single transaction.
func (s *store) executeMulti(stmts []Statement) ([]ExecResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tx, err := s.writer.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	results := make([]ExecResult, len(stmts))
	for i, stmt := range stmts {
		result, err := tx.Exec(stmt.SQL, stmt.Args...)
		if err != nil {
			return nil, fmt.Errorf("statement %d: %w", i, err)
		}
		lastID, _ := result.LastInsertId()
		rows, _ := result.RowsAffected()
		results[i] = ExecResult{LastInsertID: lastID, RowsAffected: rows}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return results, nil
}

// query runs a read query on the reader pool.
func (s *store) query(sqlStr string, args ...any) (*sql.Rows, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.reader.Query(sqlStr, args...)
}

// snapshot writes a full copy of the database to w using SQLite's VACUUM INTO.
// VACUUM INTO creates a standalone, consistent copy of the database without
// needing a transaction (it is not allowed inside a transaction).
func (s *store) snapshot(w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tmpPath := s.dbPath + ".snapshot"
	defer os.Remove(tmpPath)

	// VACUUM INTO creates a consistent, standalone copy of the database.
	// It must NOT be called inside a transaction.
	if _, err := s.reader.Exec(fmt.Sprintf("VACUUM INTO '%s'", tmpPath)); err != nil {
		return fmt.Errorf("colmena: snapshot vacuum: %w", err)
	}

	f, err := os.Open(tmpPath)
	if err != nil {
		return fmt.Errorf("colmena: snapshot open: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(w, f); err != nil {
		return fmt.Errorf("colmena: snapshot copy: %w", err)
	}
	return nil
}

// restore replaces the database with data from r.
func (s *store) restore(r io.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close existing connections.
	s.writer.Close()
	s.reader.Close()

	// Write the snapshot to the database file.
	f, err := os.Create(s.dbPath)
	if err != nil {
		return fmt.Errorf("colmena: restore create: %w", err)
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return fmt.Errorf("colmena: restore copy: %w", err)
	}
	f.Close()

	// Remove any leftover WAL/SHM files.
	os.Remove(s.dbPath + "-wal")
	os.Remove(s.dbPath + "-shm")

	// Re-open connections with the same readConns as original.
	ns, err := newStoreAt(s.dbPath, s.readConns)
	if err != nil {
		return fmt.Errorf("colmena: restore reopen: %w", err)
	}
	s.writer = ns.writer
	s.reader = ns.reader
	return nil
}

func (s *store) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	if err := s.reader.Close(); err != nil {
		firstErr = err
	}
	if err := s.writer.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// storeManager manages multiple named SQLite stores sharing one Raft cluster.
type storeManager struct {
	mu        sync.RWMutex
	stores    map[string]*store
	dataDir   string
	readConns int
}

func newStoreManager(dataDir string, readConns int) *storeManager {
	return &storeManager{
		stores:    make(map[string]*store),
		dataDir:   dataDir,
		readConns: readConns,
	}
}

// get returns the named store, creating it if it does not already exist.
// The SQLite file for database "foo" lives at dataDir/foo.db.
func (sm *storeManager) get(name string) (*store, error) {
	sm.mu.RLock()
	if s, ok := sm.stores[name]; ok {
		sm.mu.RUnlock()
		return s, nil
	}
	sm.mu.RUnlock()

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Double-check after acquiring write lock.
	if s, ok := sm.stores[name]; ok {
		return s, nil
	}

	dbPath := filepath.Join(sm.dataDir, name+".db")
	s, err := newStoreAt(dbPath, sm.readConns)
	if err != nil {
		return nil, fmt.Errorf("colmena: open store %q: %w", name, err)
	}
	sm.stores[name] = s
	return s, nil
}

// snapshot writes a tar archive containing all database files to w.
// Each entry is named "<dbname>.db".
func (sm *storeManager) snapshot(w io.Writer) error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	tw := tar.NewWriter(w)

	for name, st := range sm.stores {
		var buf bytes.Buffer
		if err := st.snapshot(&buf); err != nil {
			tw.Close()
			return fmt.Errorf("colmena: snapshot store %q: %w", name, err)
		}
		data := buf.Bytes()
		hdr := &tar.Header{
			Name: name + ".db",
			Size: int64(len(data)),
			Mode: 0644,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			tw.Close()
			return fmt.Errorf("colmena: tar header %q: %w", name, err)
		}
		if _, err := tw.Write(data); err != nil {
			tw.Close()
			return fmt.Errorf("colmena: tar write %q: %w", name, err)
		}
	}
	return tw.Close()
}

// restore closes all stores, extracts a tar archive of database files, and
// reopens the stores.
func (sm *storeManager) restore(r io.Reader) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Close all existing stores.
	for _, st := range sm.stores {
		st.close()
	}
	sm.stores = make(map[string]*store)

	// Extract tar entries.
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("colmena: tar read: %w", err)
		}

		// Entry name is "foo.db", derive the db name.
		name := strings.TrimSuffix(hdr.Name, ".db")
		dbPath := filepath.Join(sm.dataDir, hdr.Name)

		// Remove leftover WAL/SHM files.
		os.Remove(dbPath + "-wal")
		os.Remove(dbPath + "-shm")

		f, err := os.Create(dbPath)
		if err != nil {
			return fmt.Errorf("colmena: restore create %q: %w", name, err)
		}
		if _, err := io.Copy(f, tr); err != nil {
			f.Close()
			return fmt.Errorf("colmena: restore write %q: %w", name, err)
		}
		f.Close()
	}

	// Reopen all database files found in the data directory.
	entries, err := os.ReadDir(sm.dataDir)
	if err != nil {
		return fmt.Errorf("colmena: readdir after restore: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".db") {
			continue
		}
		// Skip raft.db and any non-colmena files.
		if e.Name() == "raft.db" {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".db")
		dbPath := filepath.Join(sm.dataDir, e.Name())
		s, err := newStoreAt(dbPath, sm.readConns)
		if err != nil {
			return fmt.Errorf("colmena: restore reopen %q: %w", name, err)
		}
		sm.stores[name] = s
	}

	return nil
}

// close closes all managed stores.
func (sm *storeManager) close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	var firstErr error
	for _, st := range sm.stores {
		if err := st.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// defaultStore returns the store named "default", creating it if needed.
// This preserves backward compatibility.
func (sm *storeManager) defaultStore() (*store, error) {
	return sm.get("default")
}
