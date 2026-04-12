package colmena

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// rpcPool manages RPC client connections to other nodes with automatic
// reconnection, health checks, and idle eviction.
type rpcPool struct {
	mu        sync.Mutex
	clients   map[string]*rpcEntry
	tlsConfig *tls.Config
	maxIdle   time.Duration
}

type rpcEntry struct {
	client   *rpc.Client
	lastUsed time.Time
	failures int
}

func newRPCPool(tlsConfig *tls.Config) *rpcPool {
	return &rpcPool{
		clients:   make(map[string]*rpcEntry),
		tlsConfig: tlsConfig,
		maxIdle:   5 * time.Minute,
	}
}

// get returns a healthy RPC client for the given Raft address.
// It reconnects if the cached client has failed or is stale.
func (p *rpcPool) get(raftAddr string) (*rpc.Client, error) {
	rpcAddr, err := rpcAddrFrom(raftAddr)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if entry, ok := p.clients[rpcAddr]; ok {
		if entry.failures == 0 && time.Since(entry.lastUsed) < p.maxIdle {
			entry.lastUsed = time.Now()
			return entry.client, nil
		}
		// Stale or failed connection — close and reconnect.
		entry.client.Close()
		delete(p.clients, rpcAddr)
	}

	client, err := p.dial(rpcAddr)
	if err != nil {
		return nil, err
	}

	p.clients[rpcAddr] = &rpcEntry{
		client:   client,
		lastUsed: time.Now(),
	}
	return client, nil
}

// markFailed records that an RPC call to this address failed,
// so the next get() will reconnect.
func (p *rpcPool) markFailed(raftAddr string) {
	rpcAddr, err := rpcAddrFrom(raftAddr)
	if err != nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if entry, ok := p.clients[rpcAddr]; ok {
		entry.failures++
	}
}

func (p *rpcPool) dial(addr string) (*rpc.Client, error) {
	if p.tlsConfig != nil {
		conn, err := tls.DialWithDialer(
			&net.Dialer{Timeout: 5 * time.Second},
			"tcp", addr, p.tlsConfig,
		)
		if err != nil {
			return nil, fmt.Errorf("colmena: TLS dial RPC %s: %w", addr, err)
		}
		return rpc.NewClient(conn), nil
	}
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("colmena: dial RPC %s: %w", addr, err)
	}
	return rpc.NewClient(conn), nil
}

// close shuts down all cached connections.
func (p *rpcPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, entry := range p.clients {
		entry.client.Close()
		delete(p.clients, addr)
	}
}

// RPCPing is a lightweight health check method. Returns nil on success.
type RPCPingRequest struct{}
type RPCPingResponse struct{}

func (s *RPCService) Ping(req *RPCPingRequest, resp *RPCPingResponse) error {
	return nil
}
