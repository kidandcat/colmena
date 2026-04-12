package colmena

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

// Config holds the configuration for a Colmena node.
type Config struct {
	// NodeID is a unique identifier for this node in the cluster.
	NodeID string

	// DataDir is the directory where Raft logs, snapshots, and SQLite data are stored.
	DataDir string

	// Bind is the address for Raft transport and RPC (e.g., "0.0.0.0:9000").
	Bind string

	// Advertise is the address advertised to other nodes. If empty, Bind is used.
	// Useful when running behind NAT or in containers.
	Advertise string

	// Bootstrap indicates this node should bootstrap a new cluster.
	// Only set this on the first node.
	Bootstrap bool

	// Join is a list of existing node addresses to join (e.g., ["10.0.0.2:9000", "10.0.0.3:9000"]).
	Join []string

	// Consistency is the default read consistency level.
	Consistency ConsistencyLevel

	// HeartbeatTimeout is the Raft heartbeat timeout. Default: 1s.
	HeartbeatTimeout time.Duration

	// ElectionTimeout is the Raft election timeout. Default: 1s.
	ElectionTimeout time.Duration

	// SnapshotInterval is how often Raft checks if a snapshot is needed. Default: 2m.
	SnapshotInterval time.Duration

	// SnapshotThreshold is the number of Raft log entries before triggering a snapshot. Default: 8192.
	SnapshotThreshold uint64

	// ApplyTimeout is the timeout for Raft Apply operations. Default: 10s.
	ApplyTimeout time.Duration

	// MaxPool is the maximum number of connections in the Raft TCP transport pool. Default: 3.
	MaxPool int

	// SQLiteReadConns is the number of SQLite reader connections. Default: 4.
	SQLiteReadConns int

	// TLSConfig enables mutual TLS on Raft transport and RPC connections.
	// When set, both Raft inter-node traffic and RPC forwarding are encrypted
	// and authenticated using the provided TLS configuration. The config must
	// include a certificate, private key, and CA root pool. When nil (default),
	// all connections use plaintext TCP as before.
	TLSConfig *tls.Config

	// BatchWindow is the maximum time to wait before flushing a write batch.
	// Set to 0 (default) to disable batching and apply each write individually.
	// Typical values: 1-5ms. Batching amortizes Raft consensus cost across
	// many statements, yielding 10-100x throughput improvement.
	BatchWindow time.Duration

	// BatchMaxSize is the maximum number of commands in a single batch.
	// When reached, the batch is flushed immediately regardless of the window.
	// Default: 128 (only used when BatchWindow > 0).
	BatchMaxSize int

	// Backup enables continuous backup when set. The backup engine streams
	// WAL changes and takes periodic snapshots to the configured backend.
	Backup *BackupConfig

	// LogOutput controls where Raft logs go. Default: os.Stderr.
	// Set to io.Discard to suppress logs.
	LogOutput io.Writer

	// OnApply is called after each command is applied to the local SQLite,
	// on every node (leader and followers). Useful for reactive applications
	// that need to respond to replicated writes (e.g., broadcasting WebSocket
	// messages when a new row is inserted).
	// The callback receives the database name, applied statements, and their results.
	// It is called synchronously in the Raft apply path, so keep it fast.
	OnApply func(db string, statements []Statement, results []ExecResult)
}

func (c *Config) validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("colmena: NodeID is required")
	}
	if c.DataDir == "" {
		return fmt.Errorf("colmena: DataDir is required")
	}
	if c.Bind == "" {
		return fmt.Errorf("colmena: Bind address is required")
	}
	if _, _, err := net.SplitHostPort(c.Bind); err != nil {
		return fmt.Errorf("colmena: invalid Bind address %q: %w", c.Bind, err)
	}
	if c.Bootstrap && len(c.Join) > 0 {
		return fmt.Errorf("colmena: Bootstrap and Join are mutually exclusive")
	}
	if !c.Bootstrap && len(c.Join) == 0 {
		return fmt.Errorf("colmena: either Bootstrap or Join must be set")
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.Consistency == 0 {
		c.Consistency = ConsistencyWeak
	}
	if c.HeartbeatTimeout == 0 {
		c.HeartbeatTimeout = 1 * time.Second
	}
	if c.ElectionTimeout == 0 {
		c.ElectionTimeout = 1 * time.Second
	}
	if c.SnapshotInterval == 0 {
		c.SnapshotInterval = 2 * time.Minute
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = 1024
	}
	if c.ApplyTimeout == 0 {
		c.ApplyTimeout = 10 * time.Second
	}
	if c.MaxPool == 0 {
		c.MaxPool = 3
	}
	if c.SQLiteReadConns == 0 {
		c.SQLiteReadConns = 4
	}
	if c.BatchMaxSize == 0 {
		c.BatchMaxSize = 128
	}
	if c.Advertise == "" {
		c.Advertise = c.Bind
	}
	if c.LogOutput == nil {
		c.LogOutput = os.Stderr
	}
}
