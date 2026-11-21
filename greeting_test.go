package smtp_test

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"net/textproto"
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-smtp"
)

type greetingObservation struct {
	conn                                  *smtp.Conn
	host                                  string
	extended, secured, supported, aborted bool
}

func observeGreeting(c *smtp.Conn) greetingObservation {
	mode, supported := interface{}(c).(interface{ ESMTP() bool })
	extended := false
	if supported {
		extended = mode.ESMTP()
	}
	state, secured := c.TLSConnectionState()
	return greetingObservation{conn: c, host: c.Hostname(), extended: extended, secured: secured && state.HandshakeComplete, supported: supported}
}

type greetingSession struct {
	smtp.Session
	conn     *smtp.Conn
	observed chan<- greetingObservation
}

func (s *greetingSession) Data(r io.Reader) error {
	_, err := io.Copy(ioutil.Discard, r)
	observation := observeGreeting(s.conn)
	observation.aborted = err != nil
	s.observed <- observation
	return err
}

func TestServerGreetingRejectedAndLMTP(t *testing.T) {
	for _, lmtp := range []bool{false, true} {
		t.Run(fmt.Sprintf("LMTP=%t", lmtp), func(t *testing.T) {
			observed := make(chan greetingObservation, 1)
			calls := 0
			_, server, client, _ := testServer(t, func(server *smtp.Server) {
				server.LMTP = lmtp
				be := server.Backend
				server.Backend = smtp.BackendFunc(func(c *smtp.Conn) (smtp.Session, error) {
					observed <- observeGreeting(c)
					calls++
					if calls == 1 {
						return nil, &smtp.SMTPError{Code: 451, EnhancedCode: smtp.EnhancedCode{4, 0, 0}, Message: "try again"}
					}
					return be.NewSession(c)
				})
			})
			defer server.Close()
			defer client.Close()
			client.SetDeadline(time.Now().Add(5 * time.Second))
			wire := textproto.NewConn(client)
			if _, _, err := wire.ReadResponse(220); err != nil {
				t.Fatal(err)
			}
			exchange := func(command string, code int) {
				t.Helper()
				if err := wire.PrintfLine("%s", command); err != nil {
					t.Fatal(err)
				}
				if _, _, err := wire.ReadResponse(code); err != nil {
					t.Fatal(err)
				}
			}
			greet := "EHLO esmtp.example"
			if lmtp {
				greet = "LHLO esmtp.example"
			}
			exchange(greet, 451)
			got := <-observed
			if !got.supported || !got.extended || got.host != "esmtp.example" {
				t.Errorf("NewSession did not observe extended greeting: %+v", got)
			}
			mode, ok := interface{}(got.conn).(interface{ ESMTP() bool })
			if !ok {
				t.Fatal("connection has no supported greeting-mode API")
			}
			if mode.ESMTP() {
				t.Error("rejected initial greeting retained ESMTP mode")
			}
			exchange("MAIL FROM:<sender@example.com>", 502)
			exchange(greet, 250)
			<-observed
			exchange("RSET", 250)
			if !mode.ESMTP() {
				t.Error("RSET cleared extended mode")
			}
			if lmtp {
				exchange("HELO smtp.example", 500)
				if !mode.ESMTP() {
					t.Error("rejected HELO changed LMTP greeting mode")
				}
			}
			exchange("QUIT", 221)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := server.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestServerGreetingMode(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	observed := make(chan greetingObservation, 8)
	_, server, client, scan := testServer(t, func(server *smtp.Server) {
		server.TLSConfig = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
		be := server.Backend
		server.Backend = smtp.BackendFunc(func(c *smtp.Conn) (smtp.Session, error) {
			observed <- observeGreeting(c)
			session, err := be.NewSession(c)
			return &greetingSession{Session: session, conn: c, observed: observed}, err
		})
	})
	defer server.Close()
	defer func() { client.Close() }()
	if err := client.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	response := func(code string) {
		t.Helper()
		for scan.Scan() {
			if strings.HasPrefix(scan.Text(), code+" ") {
				return
			}
			if !strings.HasPrefix(scan.Text(), code+"-") {
				t.Fatalf("response: %q, want %s", scan.Text(), code)
			}
		}
		t.Fatalf("missing %s response: %v", code, scan.Err())
	}
	send := func(text string) {
		t.Helper()
		if _, err := io.WriteString(client, text); err != nil {
			t.Fatal(err)
		}
	}
	command := func(text, code string) { t.Helper(); send(text + "\r\n"); response(code) }
	check := func(host string, extended, secured, aborted bool) *smtp.Conn {
		t.Helper()
		select {
		case got := <-observed:
			if got.host != host || got.extended != extended || got.secured != secured || got.aborted != aborted {
				t.Errorf("greeting = host %q, extended %t, TLS %t, aborted %t (public mode supported %t); want %q, %t, %t, %t", got.host, got.extended, got.secured, got.aborted, got.supported, host, extended, secured, aborted)
			}
			return got.conn
		case <-time.After(5 * time.Second):
			t.Fatal("missing backend greeting observation")
			return nil
		}
	}
	envelope := func() {
		t.Helper()
		command("MAIL FROM:<sender@example.com>", "250")
		command("RCPT TO:<recipient@example.com>", "250")
	}
	data := func(host string, extended, secured bool) {
		t.Helper()
		envelope()
		command("DATA", "354")
		send("synthetic body\r\n.\r\n")
		response("250")
		check(host, extended, secured, false)
	}
	response("220")
	command("HELO esmtp.example", "250")
	conn := check("esmtp.example", false, false, false)
	data("esmtp.example", false, false)
	command("EHLO esmtp.example", "250")
	data("esmtp.example", true, false)
	command("RSET", "250")
	data("esmtp.example", true, false)
	command("HELO esmtp.example", "250")
	command("EHLO", "501")
	data("esmtp.example", false, false)
	command("EHLO plain.example", "250")
	for _, next := range []struct {
		command, oldHost, newHost string
		oldMode, newMode          bool
	}{
		{"HELO esmtp.example", "plain.example", "esmtp.example", true, false},
		{"EHLO esmtp.example", "esmtp.example", "esmtp.example", false, true},
	} {
		envelope()
		send("BDAT 4\r\npart")
		response("250")
		command(next.command, "250")
		check(next.oldHost, next.oldMode, false, true)
		data(next.newHost, next.newMode, false)
	}
	command("STARTTLS", "220")
	secured := tls.Client(client, &tls.Config{RootCAs: roots, ServerName: "localhost"})
	if err := secured.Handshake(); err != nil {
		t.Fatal(err)
	}
	if len(secured.ConnectionState().VerifiedChains) == 0 {
		t.Fatal("TLS peer not verified")
	}
	client = secured
	scan = bufio.NewScanner(client)
	command("NOOP", "250")
	if mode, ok := interface{}(conn).(interface{ ESMTP() bool }); !ok || mode.ESMTP() {
		t.Error("successful STARTTLS did not clear the public greeting mode")
	}
	command("MAIL FROM:<sender@example.com>", "502")
	command("HELO esmtp.example", "250")
	check("esmtp.example", false, true, false)
	data("esmtp.example", false, true)
	command("EHLO esmtp.example", "250")
	command("RSET", "250")
	data("esmtp.example", true, true)
	command("HELO plain.example", "250")
	data("plain.example", false, true)
	command("QUIT", "221")
	if scan.Scan() || scan.Err() != nil {
		t.Fatalf("connection did not close: %v", scan.Err())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}
