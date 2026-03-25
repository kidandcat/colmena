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
type Node struct {
	config Config
	stores *storeManager
	raft   *raft.Raft
	fsm    *fsm

	rpcServer   *rpc.Server
	rpcListener net.Listener
	rpcClients   map[string]*rpc.Client
	rpcClientsMu sync.Mutex

	backup   *backupManager
	dbs      map[string]*sql.DB
	dbsMu    sync.Mutex
	closed   bool
	closedMu sync.Mutex
}

// New creates and starts a new Colmena node.
func New(cfg Config) (*Node, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("colmena: create data dir: %w", err)
	}

	sm := newStoreManager(cfg.DataDir, cfg.SQLiteReadConns)
	f := &fsm{stores: sm, onApply: cfg.OnApply}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.Logger = hclog.New(&hclog.LoggerOptions{Output: cfg.LogOutput, Level: hclog.Warn})
	raftConfig.HeartbeatTimeout = cfg.HeartbeatTimeout
	raftConfig.ElectionTimeout = cfg.ElectionTimeout
	raftConfig.LeaderLeaseTimeout = cfg.HeartbeatTimeout
	raftConfig.SnapshotInterval = cfg.SnapshotInterval
	raftConfig.SnapshotThreshold = cfg.SnapshotThreshold

	advertise, err := net.ResolveTCPAddr("tcp", cfg.Advertise)
	if err != nil {
		sm.close()
		return nil, fmt.Errorf("colmena: resolve advertise addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.Bind, advertise, cfg.MaxPool, 10*time.Second, cfg.LogOutput)
	if err != nil {
		sm.close()
		return nil, fmt.Errorf("colmena: create transport: %w", err)
	}

	logStore, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(cfg.DataDir, "raft.db")})
	if err != nil {
		sm.close()
		return nil, fmt.Errorf("colmena: create log store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, cfg.LogOutput)
	if err != nil {
		sm.close()
		return nil, fmt.Errorf("colmena: create snapshot store: %w", err)
	}

	r, err := raft.NewRaft(raftConfig, f, logStore, logStore, snapshotStore, transport)
	if err != nil {
		sm.close()
		return nil, fmt.Errorf("colmena: create raft: %w", err)
	}

	node := &Node{
		config:     cfg,
		stores:     sm,
		raft:       r,
		fsm:        f,
		rpcClients: make(map[string]*rpc.Client),
		dbs:        make(map[string]*sql.DB),
	}

	if cfg.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{{
				ID:      raft.ServerID(cfg.NodeID),
				Address: raft.ServerAddress(cfg.Advertise),
			}},
		}
		bf := r.BootstrapCluster(configuration)
		if err := bf.Error(); err != nil && err != raft.ErrCantBootstrap {
			node.Close()
			return nil, fmt.Errorf("colmena: bootstrap: %w", err)
		}
	}

	if err := node.startRPC(); err != nil {
		node.Close()
		return nil, err
	}

	if len(cfg.Join) > 0 {
		if err := node.join(); err != nil {
			node.Close()
			return nil, err
		}
	}

	if cfg.Backup != nil && cfg.Backup.Backend != nil {
		defStore, err := sm.get("default")
		if err != nil {
			node.Close()
			return nil, fmt.Errorf("colmena: init default store for backup: %w", err)
		}
		bm := newBackupManager(defStore, *cfg.Backup)
		if err := bm.start(); err != nil {
			node.Close()
			return nil, fmt.Errorf("colmena: start backup: %w", err)
		}
		node.backup = bm
	}

	return node, nil
}

// DB returns a *sql.DB for the "default" database with the node's default consistency.
func (n *Node) DB() *sql.DB {
	return n.OpenDB("default", n.config.Consistency)
}

// OpenDB returns a *sql.DB for the named database with the given default consistency.
// Each database maps to a separate SQLite file. Cached: same name returns same instance.
func (n *Node) OpenDB(name string, consistency ConsistencyLevel) *sql.DB {
	n.dbsMu.Lock()
	defer n.dbsMu.Unlock()
	if db, ok := n.dbs[name]; ok {
		return db
	}
	db := sql.OpenDB(&colmenaConnector{node: n, dbName: name, consistency: consistency})
	n.dbs[name] = db
	return db
}

func (n *Node) IsLeader() bool            { return n.raft.State() == raft.Leader }
func (n *Node) LeaderAddr() string         { _, id := n.raft.LeaderWithID(); return string(id) }
func (n *Node) NodeID() string             { return n.config.NodeID }
func (n *Node) Stats() map[string]string   { return n.raft.Stats() }

// Close shuts down the node gracefully. Safe to call multiple times.
func (n *Node) Close() error {
	n.closedMu.Lock()
	if n.closed { n.closedMu.Unlock(); return nil }
	n.closed = true
	n.closedMu.Unlock()

	var firstErr error
	if n.backup != nil { n.backup.stop() }

	n.dbsMu.Lock()
	for _, db := range n.dbs {
		if err := db.Close(); err != nil && firstErr == nil { firstErr = err }
	}
	n.dbsMu.Unlock()

	if n.rpcListener != nil { n.rpcListener.Close() }
	n.rpcClientsMu.Lock()
	for _, c := range n.rpcClients { c.Close() }
	n.rpcClientsMu.Unlock()

	if n.raft != nil {
		if err := n.raft.Shutdown().Error(); err != nil && firstErr == nil { firstErr = err }
	}
	if n.stores != nil {
		if err := n.stores.close(); err != nil && firstErr == nil { firstErr = err }
	}
	return firstErr
}

func (n *Node) execute(cmd *Command) (*ApplyResult, error) {
	data, err := marshalCommand(cmd)
	if err != nil { return nil, err }
	if n.raft.State() == raft.Leader { return n.applyRaft(data) }
	return n.forwardExecute(data)
}

func (n *Node) applyRaft(data []byte) (*ApplyResult, error) {
	future := n.raft.Apply(data, n.config.ApplyTimeout)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("colmena: raft apply: %w", err)
	}
	resp, ok := future.Response().(*ApplyResult)
	if !ok { return nil, fmt.Errorf("colmena: unexpected apply response type") }
	if resp.Error != "" { return nil, errors.New(resp.Error) }
	return resp, nil
}

func (n *Node) verifyLeader() error { return n.raft.VerifyLeader().Error() }

func (n *Node) forwardExecute(data []byte) (*ApplyResult, error) {
	leaderAddr, _ := n.raft.LeaderWithID()
	if leaderAddr == "" { return nil, ErrNotLeader }
	client, err := n.getRPCClient(string(leaderAddr))
	if err != nil { return nil, fmt.Errorf("colmena: connect to leader: %w", err) }
	req := &RPCExecuteRequest{Command: data}
	var resp RPCExecuteResponse
	if err := client.Call("Colmena.Execute", req, &resp); err != nil {
		return nil, fmt.Errorf("colmena: forward execute: %w", err)
	}
	if resp.Error != "" { return nil, errors.New(resp.Error) }
	return &ApplyResult{Results: resp.Results}, nil
}

func (n *Node) forwardQuery(dbName, sqlStr string, args []any) (*RPCQueryResponse, error) {
	leaderAddr, _ := n.raft.LeaderWithID()
	if leaderAddr == "" { return nil, ErrNotLeader }
	client, err := n.getRPCClient(string(leaderAddr))
	if err != nil { return nil, fmt.Errorf("colmena: connect to leader: %w", err) }
	iArgs := make([]any, len(args))
	copy(iArgs, args)
	req := &RPCQueryRequest{DB: dbName, SQL: sqlStr, Args: iArgs}
	var resp RPCQueryResponse
	if err := client.Call("Colmena.Query", req, &resp); err != nil {
		return nil, fmt.Errorf("colmena: forward query: %w", err)
	}
	if resp.Error != "" { return nil, errors.New(resp.Error) }
	return &resp, nil
}

func (n *Node) WaitForLeader(timeout time.Duration) error {
	deadline := time.After(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			return fmt.Errorf("colmena: timeout waiting for leader")
		case <-ticker.C:
			if addr, _ := n.raft.LeaderWithID(); addr != "" { return nil }
		}
	}
}

// ExecMulti executes multiple statements atomically on the "default" database.
func (n *Node) ExecMulti(stmts []Statement) ([]ExecResult, error) {
	cmd := &Command{Type: CommandExecuteMulti, DB: "default", Statements: stmts}
	result, err := n.execute(cmd)
	if err != nil { return nil, err }
	return result.Results, nil
}

func (n *Node) Nodes() ([]raft.Server, error) {
	f := n.raft.GetConfiguration()
	if err := f.Error(); err != nil { return nil, err }
	return f.Configuration().Servers, nil
}

func (n *Node) RemoveNode(nodeID string) error {
	return n.raft.RemoveServer(raft.ServerID(nodeID), 0, n.config.ApplyTimeout).Error()
}

// --- RPC ---

type RPCJoinRequest struct{ NodeID, Address string }
type RPCJoinResponse struct{ Error, LeaderAddr string }
type RPCService struct{ node *Node }

func (s *RPCService) Execute(req *RPCExecuteRequest, resp *RPCExecuteResponse) error {
	if s.node.raft.State() != raft.Leader { resp.Error = "not the leader"; return nil }
	result, err := s.node.applyRaft(req.Command)
	if err != nil { resp.Error = err.Error(); return nil }
	resp.Results = result.Results
	return nil
}

func (s *RPCService) Query(req *RPCQueryRequest, resp *RPCQueryResponse) error {
	dbName := req.DB
	if dbName == "" { dbName = "default" }
	st, err := s.node.stores.get(dbName)
	if err != nil { resp.Error = err.Error(); return nil }
	rows, err := st.query(req.SQL, req.Args...)
	if err != nil { resp.Error = err.Error(); return nil }
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil { resp.Error = err.Error(); return nil }
	resp.Columns = cols
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values { ptrs[i] = &values[i] }
		if err := rows.Scan(ptrs...); err != nil { resp.Error = err.Error(); return nil }
		rowData := make([]json.RawMessage, len(cols))
		for i, v := range values { b, _ := json.Marshal(v); rowData[i] = b }
		resp.Rows = append(resp.Rows, rowData)
	}
	if err := rows.Err(); err != nil { resp.Error = err.Error() }
	return nil
}

func (s *RPCService) Join(req *RPCJoinRequest, resp *RPCJoinResponse) error {
	if s.node.raft.State() != raft.Leader {
		leaderAddr, _ := s.node.raft.LeaderWithID()
		resp.Error = "not the leader"
		resp.LeaderAddr = string(leaderAddr)
		return nil
	}
	f := s.node.raft.AddVoter(raft.ServerID(req.NodeID), raft.ServerAddress(req.Address), 0, s.node.config.ApplyTimeout)
	if err := f.Error(); err != nil { resp.Error = err.Error() }
	return nil
}

func (n *Node) join() error {
	for _, addr := range n.config.Join {
		client, err := n.getRPCClient(addr)
		if err != nil { log.Printf("colmena: failed to connect to %s: %v", addr, err); continue }
		req := &RPCJoinRequest{NodeID: n.config.NodeID, Address: n.config.Advertise}
		var resp RPCJoinResponse
		if err := client.Call("Colmena.Join", req, &resp); err != nil { log.Printf("colmena: join RPC to %s failed: %v", addr, err); continue }
		if resp.Error != "" {
			if resp.LeaderAddr != "" {
				if c2, err := n.getRPCClient(resp.LeaderAddr); err == nil {
					var r2 RPCJoinResponse
					if err := c2.Call("Colmena.Join", req, &r2); err == nil && r2.Error == "" { return nil }
				}
			}
			log.Printf("colmena: join via %s: %s", addr, resp.Error); continue
		}
		return nil
	}
	return fmt.Errorf("colmena: failed to join cluster via any address")
}

func (n *Node) getRPCClient(addr string) (*rpc.Client, error) {
	n.rpcClientsMu.Lock()
	defer n.rpcClientsMu.Unlock()
	if c, ok := n.rpcClients[addr]; ok { return c, nil }
	rpcAddr, err := rpcAddrFrom(addr)
	if err != nil { return nil, err }
	c, err := rpc.Dial("tcp", rpcAddr)
	if err != nil { return nil, fmt.Errorf("colmena: dial RPC %s: %w", rpcAddr, err) }
	n.rpcClients[addr] = c
	return c, nil
}

func (n *Node) startRPC() error {
	rpcAddr, err := rpcAddrFrom(n.config.Bind)
	if err != nil { return err }
	n.rpcServer = rpc.NewServer()
	if err := n.rpcServer.RegisterName("Colmena", &RPCService{node: n}); err != nil {
		return fmt.Errorf("colmena: register RPC: %w", err)
	}
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil { return fmt.Errorf("colmena: listen RPC on %s: %w", rpcAddr, err) }
	n.rpcListener = ln
	go func() {
		for { conn, err := ln.Accept(); if err != nil { return }; go n.rpcServer.ServeConn(conn) }
	}()
	return nil
}

func rpcAddrFrom(raftAddr string) (string, error) {
	host, portStr, err := net.SplitHostPort(raftAddr)
	if err != nil { return "", fmt.Errorf("colmena: parse addr %q: %w", raftAddr, err) }
	var port int
	fmt.Sscanf(portStr, "%d", &port)
	return fmt.Sprintf("%s:%d", host, port+1), nil
}
