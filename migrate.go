package colmena

import (
	"fmt"
	"log"
	"strings"
	"time"
)

// Migration represents a single database migration.
type Migration struct {
	Version int
	Name    string
	SQL     string // Can contain multiple statements separated by ;
}

// Migrate applies pending migrations to the default database in order.
// It creates a _migrations tracking table if it does not exist, checks which
// migrations have already been applied, and executes any new ones sequentially.
// Each migration's SQL can contain multiple statements separated by semicolons.
// ALTER TABLE statements that fail with "duplicate column" are silently ignored
// to support idempotent re-runs.
func (n *Node) Migrate(migrations []Migration) error {
	db := n.DB()

	// 1. Create the migrations tracking table. applied_at has no DEFAULT —
	// the leader stamps the value and replicates it so every node sees the
	// same time (datetime('now') would be evaluated per-replica).
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS _migrations (
		version    INTEGER PRIMARY KEY,
		name       TEXT,
		applied_at DATETIME
	)`)
	if err != nil {
		return fmt.Errorf("colmena: create _migrations table: %w", err)
	}

	// 2. Find the highest applied version.
	var maxVersion int
	row := db.QueryRow("SELECT COALESCE(MAX(version), 0) FROM _migrations")
	if err := row.Scan(&maxVersion); err != nil {
		return fmt.Errorf("colmena: query max migration version: %w", err)
	}

	// 3. Apply pending migrations in order.
	for _, m := range migrations {
		if m.Version <= maxVersion {
			continue
		}

		log.Printf("colmena: applying migration %d: %s", m.Version, m.Name)

		// Split SQL into individual statements and execute each one.
		stmts := strings.Split(m.SQL, ";")
		for _, raw := range stmts {
			s := strings.TrimSpace(raw)
			if s == "" {
				continue
			}
			_, err := db.Exec(s)
			if err != nil {
				// If this is an ALTER TABLE that failed because the column
				// already exists, treat it as a no-op for idempotency.
				if isAlterTable(s) && isDuplicateColumn(err) {
					log.Printf("colmena: migration %d: skipping duplicate column in: %s", m.Version, s)
					continue
				}
				return fmt.Errorf("colmena: migration %d (%s) failed: %w", m.Version, m.Name, err)
			}
		}

		// Record the migration as applied. Timestamp is computed on the
		// caller (leader) side and replicated as a parameter.
		_, err := db.Exec("INSERT INTO _migrations (version, name, applied_at) VALUES (?, ?, ?)", m.Version, m.Name, time.Now().UTC())
		if err != nil {
			return fmt.Errorf("colmena: record migration %d (%s): %w", m.Version, m.Name, err)
		}

		log.Printf("colmena: migration %d applied successfully", m.Version)
	}

	return nil
}

// isAlterTable returns true if the SQL statement is an ALTER TABLE statement.
func isAlterTable(sql string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	return strings.HasPrefix(upper, "ALTER TABLE")
}

// isDuplicateColumn returns true if the error indicates a duplicate column.
func isDuplicateColumn(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate column")
}
