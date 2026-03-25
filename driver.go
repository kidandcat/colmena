package colmena

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

// colmenaConnector implements driver.Connector, used with sql.OpenDB.
type colmenaConnector struct {
	node *Node
}

func (c *colmenaConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return &colmenaConn{node: c.node}, nil
}

func (c *colmenaConnector) Driver() driver.Driver {
	return &colmenaDriver{}
}

// colmenaDriver satisfies driver.Driver but is only used via Connector.
type colmenaDriver struct{}

func (d *colmenaDriver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("colmena: use sql.OpenDB with Node.DB() instead of sql.Open")
}

// colmenaConn implements the database/sql driver.Conn interface.
type colmenaConn struct {
	node     *Node
	closed   bool
	activeTx *colmenaTx // non-nil when in a transaction
}

func (c *colmenaConn) Prepare(query string) (driver.Stmt, error) {
	return &colmenaStmt{conn: c, query: query}, nil
}

func (c *colmenaConn) Close() error {
	c.closed = true
	return nil
}

func (c *colmenaConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts a buffered transaction. Writes are collected and applied
// atomically through Raft on Commit.
func (c *colmenaConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.ReadOnly {
		return nil, errors.New("colmena: read-only transactions not supported")
	}
	tx := &colmenaTx{conn: c}
	c.activeTx = tx
	return tx, nil
}

// ExecContext implements driver.ExecerContext for direct exec without Prepare.
func (c *colmenaConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	iArgs := namedToInterface(args)

	// If inside a transaction, buffer the statement.
	if c.activeTx != nil {
		c.activeTx.mu.Lock()
		c.activeTx.stmts = append(c.activeTx.stmts, Statement{SQL: query, Args: iArgs})
		c.activeTx.mu.Unlock()
		return driver.RowsAffected(0), nil
	}

	cmd := &Command{
		Type:       CommandExecute,
		Statements: []Statement{{SQL: query, Args: iArgs}},
	}

	result, err := c.node.execute(cmd)
	if err != nil {
		return nil, err
	}
	if len(result.Results) == 0 {
		return driver.RowsAffected(0), nil
	}
	return &execResult{r: result.Results[0]}, nil
}

// QueryContext implements driver.QueryerContext for direct query without Prepare.
func (c *colmenaConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	consistency := consistencyFromContext(ctx, c.node.config.Consistency)
	iArgs := namedToAny(args)

	switch consistency {
	case ConsistencyNone:
		// Read directly from local SQLite.
		return c.localQuery(query, iArgs)

	case ConsistencyWeak:
		// Check if we believe we're the leader, then read locally.
		if c.node.IsLeader() {
			return c.localQuery(query, iArgs)
		}
		// Not leader: read locally anyway (weak = best effort).
		return c.localQuery(query, iArgs)

	case ConsistencyStrong:
		if c.node.IsLeader() {
			// Verify we're still the leader with a quorum check.
			if err := c.node.verifyLeader(); err != nil {
				return nil, fmt.Errorf("colmena: leader verification failed: %w", err)
			}
			return c.localQuery(query, iArgs)
		}
		// Forward to leader.
		return c.leaderQuery(query, iArgs)

	default:
		return c.localQuery(query, iArgs)
	}
}

func (c *colmenaConn) localQuery(query string, args []any) (driver.Rows, error) {
	rows, err := c.node.store.query(query, args...)
	if err != nil {
		return nil, err
	}
	return newWrappedRows(rows)
}

func (c *colmenaConn) leaderQuery(query string, args []any) (driver.Rows, error) {
	resp, err := c.node.forwardQuery(query, args)
	if err != nil {
		return nil, err
	}
	return &rpcRows{
		columns: resp.Columns,
		data:    resp.Rows,
		pos:     0,
	}, nil
}

// --- Transaction ---

// colmenaTx buffers writes and applies them atomically on Commit.
type colmenaTx struct {
	conn  *colmenaConn
	stmts []Statement
	mu    sync.Mutex
	done  bool
}

func (tx *colmenaTx) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.done {
		return errors.New("colmena: transaction already completed")
	}
	tx.done = true
	tx.conn.activeTx = nil

	if len(tx.stmts) == 0 {
		return nil
	}

	cmd := &Command{
		Type:       CommandExecuteMulti,
		Statements: tx.stmts,
	}
	_, err := tx.conn.node.execute(cmd)
	return err
}

func (tx *colmenaTx) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.done {
		return errors.New("colmena: transaction already completed")
	}
	tx.done = true
	tx.conn.activeTx = nil
	tx.stmts = nil
	return nil
}

// --- Statement ---

type colmenaStmt struct {
	conn  *colmenaConn
	query string
}

func (s *colmenaStmt) Close() error { return nil }

func (s *colmenaStmt) NumInput() int { return -1 } // unknown

func (s *colmenaStmt) Exec(args []driver.Value) (driver.Result, error) {
	named := valuesToNamed(args)
	return s.conn.ExecContext(context.Background(), s.query, named)
}

func (s *colmenaStmt) Query(args []driver.Value) (driver.Rows, error) {
	named := valuesToNamed(args)
	return s.conn.QueryContext(context.Background(), s.query, named)
}

// --- Result ---

type execResult struct {
	r ExecResult
}

func (r *execResult) LastInsertId() (int64, error) { return r.r.LastInsertID, nil }
func (r *execResult) RowsAffected() (int64, error) { return r.r.RowsAffected, nil }

// --- Rows wrappers ---

// wrappedRows wraps sql.Rows to implement driver.Rows.
type wrappedRows struct {
	sqlRows *sql.Rows
	cols    []string
}

func newWrappedRows(rows *sql.Rows) (*wrappedRows, error) {
	cols, err := rows.Columns()
	if err != nil {
		rows.Close()
		return nil, err
	}
	return &wrappedRows{sqlRows: rows, cols: cols}, nil
}

func (r *wrappedRows) Columns() []string { return r.cols }

func (r *wrappedRows) Close() error { return r.sqlRows.Close() }

func (r *wrappedRows) Next(dest []driver.Value) error {
	if !r.sqlRows.Next() {
		if err := r.sqlRows.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	scanArgs := make([]interface{}, len(dest))
	for i := range dest {
		scanArgs[i] = &dest[i]
	}
	return r.sqlRows.Scan(scanArgs...)
}

// rpcRows wraps RPC query results to implement driver.Rows.
type rpcRows struct {
	columns []string
	data    [][]json.RawMessage
	pos     int
}

func (r *rpcRows) Columns() []string { return r.columns }

func (r *rpcRows) Close() error { return nil }

func (r *rpcRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		return io.EOF
	}
	row := r.data[r.pos]
	r.pos++
	for i, raw := range row {
		var v interface{}
		json.Unmarshal(raw, &v)
		dest[i] = v
	}
	return nil
}

// --- Helpers ---

func namedToInterface(args []driver.NamedValue) []interface{} {
	result := make([]interface{}, len(args))
	for i, a := range args {
		result[i] = a.Value
	}
	return result
}

func namedToAny(args []driver.NamedValue) []any {
	result := make([]any, len(args))
	for i, a := range args {
		result[i] = a.Value
	}
	return result
}

func valuesToNamed(args []driver.Value) []driver.NamedValue {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return named
}
