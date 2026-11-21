package smtp_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emersion/go-smtp"
)

type bdatLengthSession struct {
	smtp.Session
	started chan struct{}
}

func (s *bdatLengthSession) Data(r io.Reader) error {
	close(s.started)
	return s.Session.Data(r)
}

type bdatLengthLMTPSession struct{ *bdatLengthSession }

func (s *bdatLengthLMTPSession) LMTPData(r io.Reader, status smtp.StatusCollector) error {
	err := s.Data(r)
	status.SetStatus("recipient@example.com", err)
	return err
}

func TestServerBdatChunkLength(t *testing.T) {
	for _, tc := range []struct {
		name      string
		last      bool
		prior     bool
		size      int
		body      string
		truncated bool
		protocol  string
	}{
		{name: "short-last", last: true, size: 8, body: "hello", truncated: true},
		{name: "empty-short-last", last: true, size: 8, truncated: true},
		{name: "short-middle", size: 8, body: "hello", truncated: true},
		{name: "empty-short-middle", size: 8, truncated: true},
		{name: "short-final-chunk", last: true, prior: true, size: 8, body: "hello", truncated: true},
		{name: "short-later-middle", prior: true, size: 8, body: "hello", truncated: true},
		{name: "complete-last", last: true, size: 8, body: "hello\r\n!"},
		{name: "empty-last", last: true},
		{name: "complete-final-chunk", last: true, prior: true, size: 8, body: "hello\r\n!"},
		{name: "lmtp-complete", last: true, prior: true, size: 8, body: "hello\r\n!", protocol: "fallback"},
		{name: "lmtp-empty-last", last: true, protocol: "fallback"},
		{name: "lmtp-native-complete", last: true, prior: true, size: 8, body: "hello\r\n!", protocol: "native"},
		{name: "lmtp-native-empty-last", last: true, protocol: "native"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started := make(chan struct{})
			be, s, c, scanner := testServer(t, func(s *smtp.Server) {
				s.LMTP = tc.protocol != ""
				be := s.Backend.(*backend)
				be.dataErrors = make(chan error, 1)
				s.Backend = smtp.BackendFunc(func(c *smtp.Conn) (smtp.Session, error) {
					session, err := be.NewSession(c)
					wrapped := &bdatLengthSession{Session: session, started: started}
					if tc.protocol == "native" {
						return &bdatLengthLMTPSession{wrapped}, err
					}
					return wrapped, err
				})
			})
			defer s.Close()
			defer c.Close()
			if err := c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "220 ") {
				t.Fatalf("invalid greeting: %s (%v)", scanner.Text(), scanner.Err())
			}
			send := func(command string) {
				t.Helper()
				if _, err := io.WriteString(c, command); err != nil {
					t.Fatal(err)
				}
			}
			response := func() {
				t.Helper()
				for scanner.Scan() {
					line := scanner.Text()
					if strings.HasPrefix(line, "250 ") {
						return
					}
					if !strings.HasPrefix(line, "250-") {
						t.Fatalf("unexpected response: %s", line)
					}
				}
				t.Fatalf("missing response: %v", scanner.Err())
			}
			if tc.protocol != "" {
				send("LHLO example.com\r\n")
			} else {
				send("EHLO example.com\r\n")
			}
			response()
			send("MAIL FROM:<sender@example.com>\r\n")
			response()
			send("RCPT TO:<recipient@example.com>\r\n")
			response()
			want := tc.body
			if tc.prior {
				send("BDAT 4\r\npart")
				response()
				want = "part" + want
			}
			last := ""
			if tc.last {
				last = " LAST"
			}
			send(fmt.Sprintf("BDAT %d%s\r\n%s", tc.size, last, tc.body))
			if tc.truncated {
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("backend did not start reading")
				}
				if err := c.(*net.TCPConn).CloseWrite(); err != nil {
					t.Fatal(err)
				}
			} else {
				response()
			}
			select {
			case err := <-be.dataErrors:
				if tc.truncated {
					if err == nil {
						t.Fatal("backend reader completed successfully on truncated chunk")
					}
					for scanner.Scan() {
						if strings.HasPrefix(scanner.Text(), "250 ") {
							t.Errorf("successful reply for truncated chunk: %s", scanner.Text())
						}
					}
					if scanner.Err() != nil {
						t.Fatal(scanner.Err())
					}
					if len(be.anonmsgs) != 0 {
						t.Fatal("backend accepted truncated message")
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					if len(be.anonmsgs) != 1 || string(be.anonmsgs[0].Data) != want {
						t.Fatal("backend did not receive complete message")
					}
				}
			case <-time.After(5 * time.Second):
				t.Fatal("backend reader did not finish")
			}
		})
	}
}

type bdatAbortSession struct {
	smtp.Session
	started    chan struct{}
	readResult chan error
	release    chan struct{}
	reset      chan struct{}
	active     int32
	earlyReset int32
}

func (s *bdatAbortSession) Data(r io.Reader) error {
	atomic.StoreInt32(&s.active, 1)
	defer atomic.StoreInt32(&s.active, 0)
	close(s.started)
	_, err := io.Copy(ioutil.Discard, r)
	s.readResult <- err
	<-s.release
	return err
}

func (s *bdatAbortSession) Reset() {
	if atomic.LoadInt32(&s.active) != 0 {
		atomic.StoreInt32(&s.earlyReset, 1)
	}
	select {
	case <-s.started:
		select {
		case s.reset <- struct{}{}:
		default:
		}
	default:
	}
	s.Session.Reset()
}

type bdatAbortLMTPSession struct {
	*bdatAbortSession
}

func (s *bdatAbortLMTPSession) LMTPData(r io.Reader, collector smtp.StatusCollector) error {
	err := s.Data(r)
	collector.SetStatus("recipient@example.com", err)
	return err
}

func TestServerBdatShortLastLMTP(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprintf("native=%t", native), func(t *testing.T) {
			started := make(chan struct{})
			readResult := make(chan error, 1)
			release := make(chan struct{})
			reset := make(chan struct{}, 1)
			var wrapped *bdatAbortSession
			_, s, c, scanner := testServer(t, func(s *smtp.Server) {
				s.LMTP = true
				be := s.Backend
				s.Backend = smtp.BackendFunc(func(c *smtp.Conn) (smtp.Session, error) {
					session, err := be.NewSession(c)
					wrapped = &bdatAbortSession{Session: session, started: started, readResult: readResult, release: release, reset: reset}
					if native {
						return &bdatAbortLMTPSession{wrapped}, err
					}
					return wrapped, err
				})
			})
			defer s.Close()
			defer c.Close()
			released := false
			defer func() {
				if !released {
					close(release)
				}
			}()
			if err := c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if !scanner.Scan() || scanner.Text() != "220 localhost LMTP Service Ready" {
				t.Fatalf("invalid greeting: %s (%v)", scanner.Text(), scanner.Err())
			}
			for _, command := range []string{
				"LHLO example.com\r\n",
				"MAIL FROM:<sender@example.com>\r\n",
				"RCPT TO:<recipient@example.com>\r\n",
			} {
				if _, err := io.WriteString(c, command); err != nil {
					t.Fatal(err)
				}
				for {
					if !scanner.Scan() {
						t.Fatalf("missing response: %v", scanner.Err())
					}
					if strings.HasPrefix(scanner.Text(), "250 ") {
						break
					}
					if !strings.HasPrefix(scanner.Text(), "250-") {
						t.Fatalf("unexpected response: %s", scanner.Text())
					}
				}
			}
			if _, err := io.WriteString(c, "BDAT 8 LAST\r\n"); err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("backend did not start reading")
			}
			if err := c.(*net.TCPConn).CloseWrite(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-readResult:
				if err == nil {
					t.Error("backend reader completed successfully on truncated chunk")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("backend reader did not finish")
			}
			close(release)
			released = true
			for scanner.Scan() {
				if strings.HasPrefix(scanner.Text(), "250 ") {
					t.Errorf("successful LMTP reply for truncated chunk: %s", scanner.Text())
				}
			}
			if scanner.Err() != nil {
				t.Fatal(scanner.Err())
			}
			select {
			case <-reset:
			case <-time.After(5 * time.Second):
				t.Fatal("transaction did not reset after backend returned")
			}
			if atomic.LoadInt32(&wrapped.earlyReset) != 0 {
				t.Error("transaction reset before backend returned")
			}
		})
	}
}

type bdatPanicSession struct {
	*bdatAbortSession
	logout chan struct{}
}

func (s *bdatPanicSession) Data(r io.Reader) error {
	s.bdatAbortSession.Data(r)
	panic("backend failure after reading chunk")
}

func (s *bdatPanicSession) Logout() error {
	err := s.Session.Logout()
	close(s.logout)
	return err
}

func TestServerBdatShortLastPanic(t *testing.T) {
	started := make(chan struct{})
	readResult := make(chan error, 1)
	release := make(chan struct{})
	close(release)
	reset := make(chan struct{}, 1)
	logout := make(chan struct{})
	var conn *smtp.Conn
	_, s, c, scanner, _ := testServerEhlo(t, func(s *smtp.Server) {
		be := s.Backend
		s.Backend = smtp.BackendFunc(func(c *smtp.Conn) (smtp.Session, error) {
			conn = c
			session, err := be.NewSession(c)
			wrapped := &bdatAbortSession{Session: session, started: started, readResult: readResult, release: release, reset: reset}
			return &bdatPanicSession{bdatAbortSession: wrapped, logout: logout}, err
		})
	})
	defer s.Close()
	defer c.Close()
	if err := c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{
		"MAIL FROM:<sender@example.com>\r\n",
		"RCPT TO:<recipient@example.com>\r\n",
	} {
		if _, err := io.WriteString(c, command); err != nil {
			t.Fatal(err)
		}
		if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "250 ") {
			t.Fatalf("unexpected response: %s (%v)", scanner.Text(), scanner.Err())
		}
	}
	if _, err := io.WriteString(c, "BDAT 8 LAST\r\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("backend did not start reading")
	}
	if err := c.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readResult:
		if err == nil {
			t.Error("backend reader completed successfully on truncated chunk")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("backend reader did not finish")
	}
	if !scanner.Scan() || scanner.Text() != "421 4.0.0 Internal server error" {
		t.Errorf("unexpected panic response: %s (%v)", scanner.Text(), scanner.Err())
	}
	if scanner.Scan() || scanner.Err() != nil {
		t.Fatalf("expected connection close after panic: %s (%v)", scanner.Text(), scanner.Err())
	}
	select {
	case <-logout:
	case <-time.After(5 * time.Second):
		t.Fatal("session was not logged out after panic")
	}
	if conn.Session() != nil {
		t.Error("session retained after panic")
	}
	select {
	case <-reset:
		t.Error("transaction reset instead of panic close")
	default:
	}
}
