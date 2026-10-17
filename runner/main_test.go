package runner

import (
	"io"
	"net"
	"testing"
)

func TestNotifyNode(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "::1"} {
		t.Run(host, func(t *testing.T) {
			l, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
			if err != nil {
				t.Skipf("no %s listener: %s", host, err)
			}
			defer l.Close()
			got := make(chan string, 1)
			go func() {
				conn, err := l.Accept()
				if err != nil {
					got <- err.Error()
					return
				}
				defer conn.Close()
				b, _ := io.ReadAll(conn)
				got <- string(b)
			}()

			if err := notifyNode(host, l.Addr().(*net.TCPAddr).Port); err != nil {
				t.Fatal(err)
			}
			if s := <-got; s != "dequeue_actions" {
				t.Fatalf("received %q, want %q", s, "dequeue_actions")
			}
		})
	}
}
