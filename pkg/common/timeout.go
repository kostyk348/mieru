package common

import (
	"net"
	"sync"
	"time"
)

const DefaultIdleTimeout = 15 * time.Minute

// IdleTimeoutConn wraps a net.Conn and sets a combined read+write deadline
// before each Read or Write call. The deadline resets on every operation,
// so active transfers (asymmetric or symmetric) never time out.
// A connection that is completely idle for the duration of the timeout
// will fail with a timeout error, allowing the owner to detect and
// clean up hung connections.
type IdleTimeoutConn struct {
	net.Conn
	mu      sync.Mutex
	timeout time.Duration
}

func NewIdleTimeoutConn(conn net.Conn, timeout time.Duration) *IdleTimeoutConn {
	return &IdleTimeoutConn{Conn: conn, timeout: timeout}
}

func (c *IdleTimeoutConn) Read(p []byte) (int, error) {
	c.refresh()
	return c.Conn.Read(p)
}

func (c *IdleTimeoutConn) Write(p []byte) (int, error) {
	c.refresh()
	return c.Conn.Write(p)
}

func (c *IdleTimeoutConn) refresh() {
	c.mu.Lock()
	c.Conn.SetDeadline(time.Now().Add(c.timeout))
	c.mu.Unlock()
}
