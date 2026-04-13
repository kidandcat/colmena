package colmena

import (
	"encoding/json"
	"fmt"
	"sync"
)

// HandlerKey is a typed key that identifies a leader-forwarded handler.
// The type parameters ensure compile-time safety: Req is the request type,
// Resp is the response type. The underlying name is only used for RPC routing.
type HandlerKey[Req, Resp any] struct {
	name string
}

// NewHandlerKey creates a handler key with the given name.
// The name must be unique per node — registering two handlers with the same
// name will panic.
func NewHandlerKey[Req, Resp any](name string) HandlerKey[Req, Resp] {
	return HandlerKey[Req, Resp]{name: name}
}

// RegisterHandler registers a handler that executes on the leader node.
// When Forward is called on any node, the request is routed to the leader
// and dispatched to this handler.
func RegisterHandler[Req, Resp any](n *Node, key HandlerKey[Req, Resp], handler func(Req) (Resp, error)) {
	n.handlers.register(key.name, func(data []byte) ([]byte, error) {
		var req Req
		if err := json.Unmarshal(data, &req); err != nil {
			return nil, fmt.Errorf("colmena: unmarshal handler request: %w", err)
		}
		resp, err := handler(req)
		if err != nil {
			return nil, err
		}
		return json.Marshal(resp)
	})
}

// Forward sends a request to the leader for processing and returns the typed response.
// If this node is the leader, the handler is called locally.
// If not, the request is forwarded via the existing RPC pool.
func Forward[Req, Resp any](n *Node, key HandlerKey[Req, Resp], req Req) (Resp, error) {
	var zero Resp

	data, err := json.Marshal(req)
	if err != nil {
		return zero, fmt.Errorf("colmena: marshal handler request: %w", err)
	}

	var respData []byte
	if n.IsLeader() {
		respData, err = n.handlers.call(key.name, data)
	} else {
		respData, err = n.forwardHandler(key.name, data)
	}
	if err != nil {
		return zero, err
	}

	var resp Resp
	if err := json.Unmarshal(respData, &resp); err != nil {
		return zero, fmt.Errorf("colmena: unmarshal handler response: %w", err)
	}
	return resp, nil
}

// handlerRegistry holds user-registered handlers keyed by name.
type handlerRegistry struct {
	mu       sync.RWMutex
	handlers map[string]func([]byte) ([]byte, error)
}

func (r *handlerRegistry) register(name string, fn func([]byte) ([]byte, error)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[name]; exists {
		panic(fmt.Sprintf("colmena: handler %q already registered", name))
	}
	r.handlers[name] = fn
}

func (r *handlerRegistry) call(name string, data []byte) ([]byte, error) {
	r.mu.RLock()
	fn, ok := r.handlers[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("colmena: unknown handler %q", name)
	}
	return fn(data)
}

// --- RPC types for handler forwarding ---

// RPCForwardRequest is sent from a follower to the leader to invoke a handler.
type RPCForwardRequest struct {
	Handler string
	Payload []byte
}

// RPCForwardResponse is the leader's response from a forwarded handler call.
type RPCForwardResponse struct {
	Payload []byte
	Error   string
}
