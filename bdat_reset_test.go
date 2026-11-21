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

type bdatResetSession struct {
	smtp.Session
	conn         *smtp.Conn
	started      chan struct{}
	read         chan error
	release      chan struct{}
	logout       chan struct{}
	returned     chan struct{}
	backendClose bool
	active       int32
	overlap      int32
	calls        int
}

func (s *bdatResetSession) Data(r io.Reader) error {
	s.calls++
	if s.calls != 1 {
		return s.Session.Data(r)
	}
	atomic.StoreInt32(&s.active, 1)
	defer close(s.returned)
	defer atomic.StoreInt32(&s.active, 0)
	close(s.started)
	if s.backendClose {
		if _, err := io.CopyN(ioutil.Discard, r, 4); err != nil {
			return err
		}
		s.conn.Close()
	}
	_, err := io.Copy(ioutil.Discard, r)
	s.read <- err
	<-s.release
	s.conn.Session()
	return err
}

func (s *bdatResetSession) Reset() {
	if atomic.LoadInt32(&s.active) != 0 {
		atomic.StoreInt32(&s.overlap, 1)
	}
	s.Session.Reset()
}

func (s *bdatResetSession) Logout() error {
	defer close(s.logout)
	return s.Session.Logout()
}

type bdatResetLMTPSession struct{ *bdatResetSession }

func (s *bdatResetLMTPSession) LMTPData(r io.Reader, status smtp.StatusCollector) error {
	err := s.Data(r)
	status.SetStatus("recipient@example.com", err)
	return err
}

func TestServerBdatResetLifecycle(t *testing.T) {
	for _, abort := range []string{"RSET", "EHLO example.com", "close", "server-close", "backend-close", "lmtp-fallback", "lmtp-native"} {
		t.Run(abort, func(t *testing.T) {
			var session *bdatResetSession
			lmtp := strings.HasPrefix(abort, "lmtp-")
			be, server, client, scan := testServer(t, func(server *smtp.Server) {
				server.LMTP = lmtp
				be := server.Backend
				server.Backend = smtp.BackendFunc(func(conn *smtp.Conn) (smtp.Session, error) {
					base, err := be.NewSession(conn)
					session = &bdatResetSession{Session: base, conn: conn, started: make(chan struct{}), read: make(chan error, 1), release: make(chan struct{}), logout: make(chan struct{}), returned: make(chan struct{}), backendClose: abort == "backend-close"}
					if abort == "lmtp-native" {
						return &bdatResetLMTPSession{session}, err
					}
					return session, err
				})
			})
			defer server.Close()
			defer client.Close()
			if err := client.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			command := func(text string) {
				t.Helper()
				if _, err := fmt.Fprint(client, text+"\r\n"); err != nil {
					t.Fatal(err)
				}
			}
			response := func() {
				t.Helper()
				for scan.Scan() {
					if strings.HasPrefix(scan.Text(), "250 ") {
						return
					}
					if !strings.HasPrefix(scan.Text(), "250-") {
						t.Fatalf("unexpected response: %s", scan.Text())
					}
				}
				t.Fatalf("missing response: %v", scan.Err())
			}
			if !scan.Scan() || !strings.HasPrefix(scan.Text(), "220 ") {
				t.Fatalf("invalid greeting: %s (%v)", scan.Text(), scan.Err())
			}
			greet := "EHLO example.com"
			if lmtp {
				greet = "LHLO example.com"
			}
			command(greet)
			response()
			defer func() {
				select {
				case <-session.release:
				default:
					close(session.release)
				}
			}()
			for _, text := range []string{"MAIL FROM:<sender@example.com>", "RCPT TO:<recipient@example.com>", "BDAT 4\r\npart"} {
				if strings.HasPrefix(text, "BDAT") {
					if _, err := io.WriteString(client, text); err != nil {
						t.Fatal(err)
					}
				} else {
					command(text)
				}
				if abort != "backend-close" || !strings.HasPrefix(text, "BDAT") {
					response()
				}
			}
			<-session.started
			closed := make(chan error, 1)
			if abort == "server-close" {
				go func() { closed <- server.Close() }()
			} else if abort == "close" {
				if err := client.(*net.TCPConn).CloseWrite(); err != nil {
					t.Fatal(err)
				}
			} else if abort == "backend-close" {
			} else if lmtp {
				command("RSET")
			} else {
				command(abort)
			}
			select {
			case err := <-session.read:
				if err != smtp.ErrDataReset {
					t.Errorf("abort reader error = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("callback not aborted")
			}
			if strings.HasSuffix(abort, "close") {
				select {
				case <-session.logout:
				case <-time.After(5 * time.Second):
					t.Fatal("Close waited on its own callback")
				}
			}
			close(session.release)
			select {
			case <-session.returned:
			case <-time.After(5 * time.Second):
				t.Fatal("callback did not return")
			}
			if !strings.HasSuffix(abort, "close") {
				response()
				for _, text := range []string{"MAIL FROM:<sender@example.com>", "RCPT TO:<recipient@example.com>", "BDAT 4 LAST\r\nnext"} {
					if strings.HasPrefix(text, "BDAT") {
						if _, err := io.WriteString(client, text); err != nil {
							t.Fatal(err)
						}
					} else {
						command(text)
					}
					response()
				}
				if len(be.anonmsgs) != 1 || string(be.anonmsgs[0].Data) != "next" {
					t.Error("wrong transaction accepted")
				}
				command("QUIT")
			}
			for scan.Scan() {
			}
			if scan.Err() != nil {
				t.Fatal(scan.Err())
			}
			select {
			case <-session.logout:
			case <-time.After(5 * time.Second):
				t.Fatal("session not logged out")
			}
			if abort == "server-close" {
				select {
				case err := <-closed:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("server close did not finish")
				}
			}
			if atomic.LoadInt32(&session.overlap) != 0 {
				t.Error("session reset before BDAT callback returned")
			}
		})
	}
}
