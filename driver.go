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

type colmenaConnector struct {
	node        *Node
	dbName      string
	consistency ConsistencyLevel
}

func (c *colmenaConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return &colmenaConn{node: c.node, dbName: c.dbName, consistency: c.consistency}, nil
}

func (c *colmenaConnector) Driver() driver.Driver { return &colmenaDriver{} }

type colmenaDriver struct{}

func (d *colmenaDriver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("colmena: use Node.DB() or Node.OpenDB() instead of sql.Open")
}

type colmenaConn struct {
	node        *Node
	dbName      string
	consistency ConsistencyLevel
	closed      bool
	activeTx    *colmenaTx
}

func (c *colmenaConn) Prepare(query string) (driver.Stmt, error) {
	return &colmenaStmt{conn: c, query: query}, nil
}

func (c *colmenaConn) Close() error { c.closed = true; return nil }

func (c *colmenaConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *colmenaConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.ReadOnly {
		return nil, errors.New("colmena: read-only transactions not supported")
	}
	tx := &colmenaTx{conn: c}
	c.activeTx = tx
	return tx, nil
}

func (c *colmenaConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	iArgs := namedToAny(args)

	if c.activeTx != nil {
		c.activeTx.mu.Lock()
		c.activeTx.stmts = append(c.activeTx.stmts, Statement{SQL: query, Args: iArgs})
		c.activeTx.mu.Unlock()
		return driver.RowsAffected(0), nil
	}

	cmd := &Command{
		Type:       CommandExecute,
		DB:         c.dbName,
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

func (c *colmenaConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	consistency := consistencyFromContext(ctx, c.consistency)
	iArgs := namedToAny(args)

	switch consistency {
	case ConsistencyNone:
		return c.localQuery(query, iArgs)
	case ConsistencyWeak:
		if c.node.IsLeader() {
			return c.localQuery(query, iArgs)
		}
		return c.leaderQuery(query, iArgs)
	case ConsistencyStrong:
		if c.node.IsLeader() {
			if err := c.node.verifyLeader(); err != nil {
				return nil, fmt.Errorf("colmena: leader verification failed: %w", err)
			}
			return c.localQuery(query, iArgs)
		}
		return c.leaderQuery(query, iArgs)
	case ConsistencyLease:
		if c.node.IsLeader() {
			return c.localQuery(query, iArgs)
		}
		if c.node.lease.valid() {
			return c.localQuery(query, iArgs)
		}
		return c.leaderQuery(query, iArgs)
	default:
		return c.localQuery(query, iArgs)
	}
}

func (c *colmenaConn) localQuery(query string, args []any) (driver.Rows, error) {
	st, err := c.node.stores.get(c.dbName)
	if err != nil {
		return nil, err
	}
	rows, err := st.query(query, args...)
	if err != nil {
		return nil, err
	}
	c.node.metrics.readsTotal.Add(1)
	return newWrappedRows(rows)
}

func (c *colmenaConn) leaderQuery(query string, args []any) (driver.Rows, error) {
	resp, err := c.node.forwardQuery(c.dbName, query, args)
	if err != nil {
		return nil, err
	}
	c.node.metrics.readsTotal.Add(1)
	return &rpcRows{columns: resp.Columns, tagged: resp.TaggedRows, legacy: resp.Rows}, nil
}

// --- Transaction ---

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
	cmd := &Command{Type: CommandExecuteMulti, DB: tx.conn.dbName, Statements: tx.stmts}
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

func (s *colmenaStmt) Close() error  { return nil }
func (s *colmenaStmt) NumInput() int { return -1 }

func (s *colmenaStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.conn.ExecContext(context.Background(), s.query, valuesToNamed(args))
}

func (s *colmenaStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.conn.QueryContext(context.Background(), s.query, valuesToNamed(args))
}

// --- Result ---

type execResult struct{ r ExecResult }

func (r *execResult) LastInsertId() (int64, error) { return r.r.LastInsertID, nil }
func (r *execResult) RowsAffected() (int64, error) { return r.r.RowsAffected, nil }

// --- Rows ---

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
func (r *wrappedRows) Close() error      { return r.sqlRows.Close() }

func (r *wrappedRows) Next(dest []driver.Value) error {
	if !r.sqlRows.Next() {
		if err := r.sqlRows.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	holders := make([]any, len(dest))
	scanArgs := make([]any, len(dest))
	for i := range holders {
		scanArgs[i] = &holders[i]
	}
	if err := r.sqlRows.Scan(scanArgs...); err != nil {
		return err
	}
	for i, v := range holders {
		dest[i] = v
	}
	return nil
}

type rpcRows struct {
	columns []string
	tagged  [][]TaggedValue   // v0.6.1+ type-preserving payload (preferred)
	legacy  [][]json.RawMessage // v0.6.0 peer fallback
	pos     int
}

func (r *rpcRows) Columns() []string { return r.columns }
func (r *rpcRows) Close() error      { return nil }

func (r *rpcRows) Next(dest []driver.Value) error {
	if len(r.tagged) > 0 {
		if r.pos >= len(r.tagged) {
			return io.EOF
		}
		row := r.tagged[r.pos]
		r.pos++
		for i, tv := range row {
			v, err := decodeTaggedValue(tv)
			if err != nil {
				return err
			}
			dest[i] = v
		}
		return nil
	}
	if r.pos >= len(r.legacy) {
		return io.EOF
	}
	row := r.legacy[r.pos]
	r.pos++
	for i, raw := range row {
		var v any
		json.Unmarshal(raw, &v)
		dest[i] = v
	}
	return nil
}

// --- Helpers ---

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
