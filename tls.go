package colmena

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// tlsStreamLayer implements raft.StreamLayer over a TLS-wrapped TCP listener.
// Inbound connections are accepted via the TLS listener (which enforces mTLS),
// and outbound connections use tls.DialWithDialer with the client TLS config.
type tlsStreamLayer struct {
	listener  net.Listener
	advertise net.Addr
	tlsConfig *tls.Config
}

func (t *tlsStreamLayer) Accept() (net.Conn, error) {
	return t.listener.Accept()
}

func (t *tlsStreamLayer) Close() error {
	return t.listener.Close()
}

func (t *tlsStreamLayer) Addr() net.Addr {
	if t.advertise != nil {
		return t.advertise
	}
	return t.listener.Addr()
}

func (t *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", string(address), t.tlsConfig)
}
