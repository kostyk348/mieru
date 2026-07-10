package common

import (
	"net"
	"sync"
	"testing"
	"time"
)

func drainConn(t *testing.T, conn net.Conn, wg *sync.WaitGroup) {
	t.Helper()
	buf := make([]byte, 1024)
	for {
		_, err := conn.Read(buf)
		if err != nil {
			wg.Done()
			return
		}
	}
}

func TestIdleTimeoutConn_ReadGetsData(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() { server.Close(); client.Close() })

	wrapped := NewIdleTimeoutConn(server, 10*time.Second)

	done := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 10)
		n, _ := wrapped.Read(buf)
		done <- buf[:n]
	}()

	time.Sleep(10 * time.Millisecond)
	client.Write([]byte("hello"))

	select {
	case data := <-done:
		if string(data) != "hello" {
			t.Fatalf("got %q, want %q", string(data), "hello")
		}
	case <-time.After(time.Second):
		t.Fatal("Read didn't complete in time")
	}
}

func TestIdleTimeoutConn_TimesOutOnIdleRead(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() { server.Close(); client.Close() })

	wrapped := NewIdleTimeoutConn(server, 20*time.Millisecond)

	_, err := wrapped.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestIdleTimeoutConn_ReadRefreshesDeadline(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() { server.Close(); client.Close() })

	wrapped := NewIdleTimeoutConn(server, 100*time.Millisecond)

	// Wait 60ms (within the 100ms deadline).
	time.Sleep(60 * time.Millisecond)

	// This read should succeed because the deadline has been refreshed
	// by our sleep being well within the deadline.
	go func() {
		time.Sleep(10 * time.Millisecond)
		client.Write([]byte("data"))
	}()

	buf := make([]byte, 10)
	n, err := wrapped.Read(buf)
	if err != nil {
		t.Fatalf("Read after 60ms failed: %v — deadline should still be valid", err)
	}
	if string(buf[:n]) != "data" {
		t.Fatalf("got %q, want %q", string(buf[:n]), "data")
	}
}

func TestIdleTimeoutConn_DeadlineExpiredThenRefreshOnNextRead(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() { server.Close(); client.Close() })

	wrapped := NewIdleTimeoutConn(server, 20*time.Millisecond)

	// First read: block until deadline expires.
	_, err := wrapped.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("expected timeout on first read")
	}

	// Even though deadline expired, our wrapper refreshes it on each Read.
	// So a second Read should work.
	go func() {
		time.Sleep(10 * time.Millisecond)
		client.Write([]byte("x"))
	}()

	buf := make([]byte, 10)
	n, err := wrapped.Read(buf)
	if err != nil {
		t.Fatalf("second Read after refresh failed: %v", err)
	}
	if string(buf[:n]) != "x" {
		t.Fatalf("got %q, want %q", string(buf[:n]), "x")
	}
}

func TestIdleTimeoutConn_ContinuousReadsKeepAlive(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() { server.Close(); client.Close() })

	wrapped := NewIdleTimeoutConn(server, 60*time.Millisecond)

	result := make(chan error, 1)
	go func() {
		for i := 0; i < 3; i++ {
			buf := make([]byte, 10)
			_, err := wrapped.Read(buf)
			if err != nil {
				result <- err
				return
			}
		}
		result <- nil
	}()

	for i := 0; i < 3; i++ {
		time.Sleep(30 * time.Millisecond)
		client.Write([]byte("x"))
	}

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("continuous reads failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("continuous reads didn't complete")
	}
}
