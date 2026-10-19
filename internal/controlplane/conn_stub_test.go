package controlplane

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"time"
)

// stubAddr is a no-op net.Addr for stubConn.
type stubAddr struct{}

func (stubAddr) Network() string { return "stub" }
func (stubAddr) String() string  { return "stub" }

// stubConn is a scriptable net.Conn used to exercise connection-level error
// paths that net.Pipe cannot easily produce (write errors, deadline errors).
type stubConn struct {
	readBuf bytes.Buffer

	writeErr        error
	readErr         error
	setDeadlineErr  error
	zeroDeadlineErr error
}

func (c *stubConn) Read(p []byte) (int, error) {
	if c.readBuf.Len() > 0 {
		return c.readBuf.Read(p)
	}
	if c.readErr != nil {
		return 0, c.readErr
	}
	return 0, io.EOF
}

func (c *stubConn) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return len(p), nil
}

func (c *stubConn) Close() error                       { return nil }
func (c *stubConn) LocalAddr() net.Addr                { return stubAddr{} }
func (c *stubConn) RemoteAddr() net.Addr               { return stubAddr{} }
func (c *stubConn) SetDeadline(t time.Time) error      { return nil }
func (c *stubConn) SetWriteDeadline(t time.Time) error { return nil }

func (c *stubConn) SetReadDeadline(t time.Time) error {
	if t.IsZero() && c.zeroDeadlineErr != nil {
		return c.zeroDeadlineErr
	}
	return c.setDeadlineErr
}

// frame encodes a length-prefixed frame the same way writeFrame does.
func frame(typ messageType, payload []byte) []byte {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)+1))
	return append(append(header[:], byte(typ)), payload...)
}
