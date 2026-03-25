package colmena

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// ErrNotLeader is returned when a write is attempted on a non-leader node
// and the leader address is unknown for forwarding.
var ErrNotLeader = errors.New("colmena: not the leader")

// Node is a single member of a Colmena distributed SQLite cluster.
// Create one with New().
type Node struct {
	config Config
	store  *store
	raft   *raft.Raft
	fsm    *fsm

	rpcServer   *rpc.Server
	rpcListener net.Listener

	rpcClients   map[string]*rpc.Client
	rpcClientsMu sync.Mutex

	backup *backupManager

	db       *sql.DB
	dbMu     sync.Once
	closed   bool
	closedMu sync.Mutex
}

// New creates and starts a new Colmena node. The node will either bootstrap
// a new cluster or join an existing one based on the config.
func New(cfg Config) (*Node, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	// Ensure data directory exists.
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("colmena: create data dir: %w", err)
	}

	// Create SQLite store.
	st, err := newStore(cfg.DataDir, cfg.SQLiteReadConns)
	if err != nil {
		return nil, err
	}

	// Create FSM.
	f := &fsm{store: st, onApply: cfg.OnApply}

	// Setup Raft.
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.Logger = hclog.New(&hclog.LoggerOptions{
		Output: cfg.LogOutput,
		Level:  hclog.Warn,
	})
	raftConfig.HeartbeatTimeout = cfg.HeartbeatTimeout
	raftConfig.ElectionTimeout = cfg.ElectionTimeout
	raftConfig.LeaderLeaseTimeout = cfg.HeartbeatTimeout
	raftConfig.SnapshotInterval = cfg.SnapshotInterval
	raftConfig.SnapshotThreshold = cfg.SnapshotThreshold

	// Raft transport.
	advertise, err := net.ResolveTCPAddr("tcp", cfg.Advertise)
	if err != nil {
		st.close()
		return nil, fmt.Errorf("colmena: resolve advertise addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.Bind, advertise, cfg.MaxPool, 10*time.Second, cfg.LogOutput)
	if err != nil {
		st.close()
		return nil, fmt.Errorf("colmena: create transport: %w", err)
	}

	// Raft log store (BoltDB).
	logStore, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(cfg.DataDir, "raft.db"),
	})
	if err != nil {
		st.close()
		return nil, fmt.Errorf("colmena: create log store: %w", err)
	}

	// Snapshot store.
	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, cfg.LogOutput)
	if err != nil {
		st.close()
		return nil, fmt.Errorf("colmena: create snapshot store: %w", err)
	}

	// Create Raft instance.
	r, err := raft.NewRaft(raftConfig, f, logStore, logStore, snapshotStore, transport)
	if err != nil {
		st.close()
		return nil, fmt.Errorf("colmena: create raft: %w", err)
	}

	node := &Node{
		config:     cfg,
		store:      st,
		raft:       r,
		fsm:        f,
		rpcClients: make(map[string]*rpc.Client),
	}

	// Bootstrap or join.
	if cfg.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(cfg.NodeID),
					Address: raft.ServerAddress(cfg.Advertise),
				},
			},
		}
		f := r.BootstrapCluster(configuration)
		if err := f.Error(); err != nil && err != raft.ErrCantBootstrap {
			node.Close()
			return nil, fmt.Errorf("colmena: bootstrap: %w", err)
		}
	}

	// Start RPC server for leader forwarding.
	if err := node.startRPC(); err != nil {
		node.Close()
		return nil, err
	}

	// Join existing cluster if needed.
	if len(cfg.Join) > 0 {
		if err := node.join(); err != nil {
			node.Close()
			return nil, err
		}
	}

	// Start continuous backup if configured.
	if cfg.Backup != nil && cfg.Backup.Backend != nil {
		bm := newBackupManager(st, *cfg.Backup)
		if err := bm.start(); err != nil {
			node.Close()
			return nil, fmt.Errorf("colmena: start backup: %w", err)
		}
		node.backup = bm
	}

	return node, nil
}

// DB returns a *sql.DB that routes writes through Raft and reads to local SQLite.
// The returned DB is safe for concurrent use and is cached (same instance on repeated calls).
func (n *Node) DB() *sql.DB {
	n.dbMu.Do(func() {
		connector := &colmenaConnector{node: n}
		n.db = sql.OpenDB(connector)
	})
	return n.db
}

// IsLeader returns true if this node is the current Raft leader.
func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// LeaderAddr returns the address of the current leader, or empty if unknown.
func (n *Node) LeaderAddr() string {
	_, id := n.raft.LeaderWithID()
	return string(id)
}

// NodeID returns this node's ID.
func (n *Node) NodeID() string {
	return n.config.NodeID
}

// Close shuts down the node, leaving the cluster gracefully. Safe to call multiple times.
func (n *Node) Close() error {
	n.closedMu.Lock()
	if n.closed {
		n.closedMu.Unlock()
		return nil
	}
	n.closed = true
	n.closedMu.Unlock()

	var firstErr error

	// Stop backup before closing anything else.
	if n.backup != nil {
		n.backup.stop()
	}

	if n.db != nil {
		if err := n.db.Close(); err != nil {
			firstErr = err
		}
	}

	if n.rpcListener != nil {
		n.rpcListener.Close()
	}

	n.rpcClientsMu.Lock()
	for _, c := range n.rpcClients {
		c.Close()
	}
	n.rpcClientsMu.Unlock()

	if n.raft != nil {
		if err := n.raft.Shutdown().Error(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if n.store != nil {
		if err := n.store.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// execute applies a write command through Raft. If this node is not the leader,
// it forwards the request to the leader via RPC.
func (n *Node) execute(cmd *Command) (*ApplyResult, error) {
	data, err := marshalCommand(cmd)
	if err != nil {
		return nil, err
	}

	if n.raft.State() == raft.Leader {
		return n.applyRaft(data)
	}

	// Forward to leader.
	return n.forwardExecute(data)
}

func (n *Node) applyRaft(data []byte) (*ApplyResult, error) {
	future := n.raft.Apply(data, n.config.ApplyTimeout)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("colmena: raft apply: %w", err)
	}

	resp, ok := future.Response().(*ApplyResult)
	if !ok {
		return nil, fmt.Errorf("colmena: unexpected apply response type")
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	return resp, nil
}

// verifyLeader checks that this node is still the leader (for strong consistency reads).
func (n *Node) verifyLeader() error {
	future := n.raft.VerifyLeader()
	return future.Error()
}

// join requests that each address in the Join list adds this node to the cluster.
func (n *Node) join() error {
	for _, addr := range n.config.Join {
		client, err := n.getRPCClient(addr)
		if err != nil {
			log.Printf("colmena: failed to connect to %s for join: %v", addr, err)
			continue
		}

		req := &RPCJoinRequest{
			NodeID:  n.config.NodeID,
			Address: n.config.Advertise,
		}
		var resp RPCJoinResponse
		if err := client.Call("Colmena.Join", req, &resp); err != nil {
			log.Printf("colmena: join RPC to %s failed: %v", addr, err)
			continue
		}
		if resp.Error != "" {
			// If error says not leader, try the leader address.
			if resp.LeaderAddr != "" {
				client2, err := n.getRPCClient(resp.LeaderAddr)
				if err == nil {
					var resp2 RPCJoinResponse
					if err := client2.Call("Colmena.Join", req, &resp2); err == nil && resp2.Error == "" {
						return nil
					}
				}
			}
			log.Printf("colmena: join via %s: %s", addr, resp.Error)
			continue
		}
		return nil
	}
	return fmt.Errorf("colmena: failed to join cluster via any address")
}

func (n *Node) getRPCClient(addr string) (*rpc.Client, error) {
	n.rpcClientsMu.Lock()
	defer n.rpcClientsMu.Unlock()

	if c, ok := n.rpcClients[addr]; ok {
		return c, nil
	}

	// Derive RPC port: Raft port + 1.
	rpcAddr, err := rpcAddrFrom(addr)
	if err != nil {
		return nil, err
	}

	c, err := rpc.Dial("tcp", rpcAddr)
	if err != nil {
		return nil, fmt.Errorf("colmena: dial RPC %s: %w", rpcAddr, err)
	}
	n.rpcClients[addr] = c
	return c, nil
}

func (n *Node) forwardExecute(data []byte) (*ApplyResult, error) {
	leaderAddr, _ := n.raft.LeaderWithID()
	if leaderAddr == "" {
		return nil, ErrNotLeader
	}

	client, err := n.getRPCClient(string(leaderAddr))
	if err != nil {
		return nil, fmt.Errorf("colmena: connect to leader: %w", err)
	}

	req := &RPCExecuteRequest{Command: data}
	var resp RPCExecuteResponse
	if err := client.Call("Colmena.Execute", req, &resp); err != nil {
		return nil, fmt.Errorf("colmena: forward execute: %w", err)
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	return &ApplyResult{Results: resp.Results}, nil
}

func (n *Node) forwardQuery(sqlStr string, args []any) (*RPCQueryResponse, error) {
	leaderAddr, _ := n.raft.LeaderWithID()
	if leaderAddr == "" {
		return nil, ErrNotLeader
	}

	client, err := n.getRPCClient(string(leaderAddr))
	if err != nil {
		return nil, fmt.Errorf("colmena: connect to leader: %w", err)
	}

	// Convert args to []any for JSON serialization.
	iArgs := make([]any, len(args))
	copy(iArgs, args)

	req := &RPCQueryRequest{SQL: sqlStr, Args: iArgs}
	var resp RPCQueryResponse
	if err := client.Call("Colmena.Query", req, &resp); err != nil {
		return nil, fmt.Errorf("colmena: forward query: %w", err)
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	return &resp, nil
}

// WaitForLeader blocks until a leader is elected or the timeout expires.
func (n *Node) WaitForLeader(timeout time.Duration) error {
	deadline := time.After(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return fmt.Errorf("colmena: timeout waiting for leader")
		case <-ticker.C:
			addr, _ := n.raft.LeaderWithID()
			if addr != "" {
				return nil
			}
		}
	}
}

// ExecMulti executes multiple statements atomically as a single Raft log entry.
// All statements succeed or all fail (transaction semantics).
func (n *Node) ExecMulti(stmts []Statement) ([]ExecResult, error) {
	cmd := &Command{
		Type:       CommandExecuteMulti,
		Statements: stmts,
	}
	result, err := n.execute(cmd)
	if err != nil {
		return nil, err
	}
	return result.Results, nil
}

// Stats returns Raft cluster statistics.
func (n *Node) Stats() map[string]string {
	return n.raft.Stats()
}

// Nodes returns the current cluster configuration.
func (n *Node) Nodes() ([]raft.Server, error) {
	f := n.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return nil, err
	}
	return f.Configuration().Servers, nil
}

// RemoveNode removes a node from the cluster. Must be called on the leader.
func (n *Node) RemoveNode(nodeID string) error {
	f := n.raft.RemoveServer(raft.ServerID(nodeID), 0, n.config.ApplyTimeout)
	return f.Error()
}

// --- RPC types for join ---

type RPCJoinRequest struct {
	NodeID  string
	Address string
}

type RPCJoinResponse struct {
	Error      string
	LeaderAddr string
}

// RPCService handles RPC calls from other nodes.
type RPCService struct {
	node *Node
}

func (s *RPCService) Execute(req *RPCExecuteRequest, resp *RPCExecuteResponse) error {
	if s.node.raft.State() != raft.Leader {
		resp.Error = "not the leader"
		return nil
	}

	result, err := s.node.applyRaft(req.Command)
	if err != nil {
		resp.Error = err.Error()
		return nil
	}
	resp.Results = result.Results
	return nil
}

func (s *RPCService) Query(req *RPCQueryRequest, resp *RPCQueryResponse) error {
	// Execute the query on the leader's local store.
	iArgs := make([]any, len(req.Args))
	copy(iArgs, req.Args)

	rows, err := s.node.store.query(req.SQL, iArgs...)
	if err != nil {
		resp.Error = err.Error()
		return nil
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		resp.Error = err.Error()
		return nil
	}
	resp.Columns = cols

	for rows.Next() {
		values := make([]any, len(cols))
		valuePtrs := make([]any, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			resp.Error = err.Error()
			return nil
		}
		rowData := make([]json.RawMessage, len(cols))
		for i, v := range values {
			b, _ := json.Marshal(v)
			rowData[i] = b
		}
		resp.Rows = append(resp.Rows, rowData)
	}
	if err := rows.Err(); err != nil {
		resp.Error = err.Error()
	}
	return nil
}

func (s *RPCService) Join(req *RPCJoinRequest, resp *RPCJoinResponse) error {
	if s.node.raft.State() != raft.Leader {
		leaderAddr, _ := s.node.raft.LeaderWithID()
		resp.Error = "not the leader"
		resp.LeaderAddr = string(leaderAddr)
		return nil
	}

	f := s.node.raft.AddVoter(
		raft.ServerID(req.NodeID),
		raft.ServerAddress(req.Address),
		0,
		s.node.config.ApplyTimeout,
	)
	if err := f.Error(); err != nil {
		resp.Error = err.Error()
	}
	return nil
}

func (n *Node) startRPC() error {
	rpcAddr, err := rpcAddrFrom(n.config.Bind)
	if err != nil {
		return err
	}

	n.rpcServer = rpc.NewServer()
	service := &RPCService{node: n}
	if err := n.rpcServer.RegisterName("Colmena", service); err != nil {
		return fmt.Errorf("colmena: register RPC: %w", err)
	}

	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return fmt.Errorf("colmena: listen RPC on %s: %w", rpcAddr, err)
	}
	n.rpcListener = ln

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			go n.rpcServer.ServeConn(conn)
		}
	}()

	return nil
}

// rpcAddrFrom derives the RPC address from a Raft bind address (port + 1).
func rpcAddrFrom(raftAddr string) (string, error) {
	host, portStr, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return "", fmt.Errorf("colmena: parse addr %q: %w", raftAddr, err)
	}
	var port int
	fmt.Sscanf(portStr, "%d", &port)
	return fmt.Sprintf("%s:%d", host, port+1), nil
}
