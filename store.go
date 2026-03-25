package colmena

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	dbPath := filepath.Join(dataDir, dbFileName)

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
	dataDir := filepath.Dir(s.dbPath)
	ns, err := newStore(dataDir, s.readConns)
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
