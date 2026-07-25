// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tls

import (
	"bytes"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

var rsaCertPEM = `-----BEGIN CERTIFICATE-----
MIIB0zCCAX2gAwIBAgIJAI/M7BYjwB+uMA0GCSqGSIb3DQEBBQUAMEUxCzAJBgNV
BAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQwHhcNMTIwOTEyMjE1MjAyWhcNMTUwOTEyMjE1MjAyWjBF
MQswCQYDVQQGEwJBVTETMBEGA1UECAwKU29tZS1TdGF0ZTEhMB8GA1UECgwYSW50
ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBANLJ
hPHhITqQbPklG3ibCVxwGMRfp/v4XqhfdQHdcVfHap6NQ5Wok/4xIA+ui35/MmNa
rtNuC+BdZ1tMuVCPFZcCAwEAAaNQME4wHQYDVR0OBBYEFJvKs8RfJaXTH08W+SGv
zQyKn0H8MB8GA1UdIwQYMBaAFJvKs8RfJaXTH08W+SGvzQyKn0H8MAwGA1UdEwQF
MAMBAf8wDQYJKoZIhvcNAQEFBQADQQBJlffJHybjDGxRMqaRmDhX0+6v02TUKZsW
r5QuVbpQhH6u+0UgcW0jp9QwpxoPTLTWGXEWBBBurxFwiCBhkQ+V
-----END CERTIFICATE-----
`

var rsaKeyPEM = testingKey(`-----BEGIN RSA TESTING KEY-----
MIIBOwIBAAJBANLJhPHhITqQbPklG3ibCVxwGMRfp/v4XqhfdQHdcVfHap6NQ5Wo
k/4xIA+ui35/MmNartNuC+BdZ1tMuVCPFZcCAwEAAQJAEJ2N+zsR0Xn8/Q6twa4G
6OB1M1WO+k+ztnX/1SvNeWu8D6GImtupLTYgjZcHufykj09jiHmjHx8u8ZZB/o1N
MQIhAPW+eyZo7ay3lMz1V01WVjNKK9QSn1MJlb06h/LuYv9FAiEA25WPedKgVyCW
SmUwbPw8fnTcpqDWE3yTO3vKcebqMSsCIBF3UmVue8YU3jybC3NxuXq3wNm34R8T
xVLHwDXh/6NJAiEAl2oHGGLz64BuAfjKrqwz7qMYr9HCLIe/YsoWq/olzScCIQDi
D2lWusoe2/nEqfDVVWGWlyJ7yOmqaVm/iNUN9B2N2g==
-----END RSA TESTING KEY-----
`)

// keyPEM is the same as rsaKeyPEM, but declares itself as just
// "PRIVATE KEY", not "RSA PRIVATE KEY".  https://golang.org/issue/4477
var keyPEM = testingKey(`-----BEGIN TESTING KEY-----
MIIBOwIBAAJBANLJhPHhITqQbPklG3ibCVxwGMRfp/v4XqhfdQHdcVfHap6NQ5Wo
k/4xIA+ui35/MmNartNuC+BdZ1tMuVCPFZcCAwEAAQJAEJ2N+zsR0Xn8/Q6twa4G
6OB1M1WO+k+ztnX/1SvNeWu8D6GImtupLTYgjZcHufykj09jiHmjHx8u8ZZB/o1N
MQIhAPW+eyZo7ay3lMz1V01WVjNKK9QSn1MJlb06h/LuYv9FAiEA25WPedKgVyCW
SmUwbPw8fnTcpqDWE3yTO3vKcebqMSsCIBF3UmVue8YU3jybC3NxuXq3wNm34R8T
xVLHwDXh/6NJAiEAl2oHGGLz64BuAfjKrqwz7qMYr9HCLIe/YsoWq/olzScCIQDi
D2lWusoe2/nEqfDVVWGWlyJ7yOmqaVm/iNUN9B2N2g==
-----END TESTING KEY-----
`)

var ecdsaCertPEM = `-----BEGIN CERTIFICATE-----
MIIB/jCCAWICCQDscdUxw16XFDAJBgcqhkjOPQQBMEUxCzAJBgNVBAYTAkFVMRMw
EQYDVQQIEwpTb21lLVN0YXRlMSEwHwYDVQQKExhJbnRlcm5ldCBXaWRnaXRzIFB0
eSBMdGQwHhcNMTIxMTE0MTI0MDQ4WhcNMTUxMTE0MTI0MDQ4WjBFMQswCQYDVQQG
EwJBVTETMBEGA1UECBMKU29tZS1TdGF0ZTEhMB8GA1UEChMYSW50ZXJuZXQgV2lk
Z2l0cyBQdHkgTHRkMIGbMBAGByqGSM49AgEGBSuBBAAjA4GGAAQBY9+my9OoeSUR
lDQdV/x8LsOuLilthhiS1Tz4aGDHIPwC1mlvnf7fg5lecYpMCrLLhauAc1UJXcgl
01xoLuzgtAEAgv2P/jgytzRSpUYvgLBt1UA0leLYBy6mQQbrNEuqT3INapKIcUv8
XxYP0xMEUksLPq6Ca+CRSqTtrd/23uTnapkwCQYHKoZIzj0EAQOBigAwgYYCQXJo
A7Sl2nLVf+4Iu/tAX/IF4MavARKC4PPHK3zfuGfPR3oCCcsAoz3kAzOeijvd0iXb
H5jBImIxPL4WxQNiBTexAkF8D1EtpYuWdlVQ80/h/f4pBcGiXPqX5h2PQSQY7hP1
+jwM1FGS4fREIOvlBYr/SzzQRtwrvrzGYxDEDbsC0ZGRnA==
-----END CERTIFICATE-----
`

var ecdsaKeyPEM = testingKey(`-----BEGIN EC PARAMETERS-----
BgUrgQQAIw==
-----END EC PARAMETERS-----
-----BEGIN EC TESTING KEY-----
MIHcAgEBBEIBrsoKp0oqcv6/JovJJDoDVSGWdirrkgCWxrprGlzB9o0X8fV675X0
NwuBenXFfeZvVcwluO7/Q9wkYoPd/t3jGImgBwYFK4EEACOhgYkDgYYABAFj36bL
06h5JRGUNB1X/Hwuw64uKW2GGJLVPPhoYMcg/ALWaW+d/t+DmV5xikwKssuFq4Bz
VQldyCXTXGgu7OC0AQCC/Y/+ODK3NFKlRi+AsG3VQDSV4tgHLqZBBus0S6pPcg1q
kohxS/xfFg/TEwRSSws+roJr4JFKpO2t3/be5OdqmQ==
-----END EC TESTING KEY-----
`)

var keyPairTests = []struct {
	algo string
	cert string
	key  string
}{
	{"ECDSA", ecdsaCertPEM, ecdsaKeyPEM},
	{"RSA", rsaCertPEM, rsaKeyPEM},
	{"RSA-untyped", rsaCertPEM, keyPEM}, // golang.org/issue/4477
}

func TestX509KeyPair(t *testing.T) {
	t.Parallel()
	var pem []byte
	for _, test := range keyPairTests {
		pem = []byte(test.cert + test.key)
		if _, err := X509KeyPair(pem, pem); err != nil {
			t.Errorf("Failed to load %s cert followed by %s key: %s", test.algo, test.algo, err)
		}
		pem = []byte(test.key + test.cert)
		if _, err := X509KeyPair(pem, pem); err != nil {
			t.Errorf("Failed to load %s key followed by %s cert: %s", test.algo, test.algo, err)
		}
	}
}

func TestX509KeyPairErrors(t *testing.T) {
	_, err := X509KeyPair([]byte(rsaKeyPEM), []byte(rsaCertPEM))
	if err == nil {
		t.Fatalf("X509KeyPair didn't return an error when arguments were switched")
	}
	if subStr := "been switched"; !strings.Contains(err.Error(), subStr) {
		t.Fatalf("Expected %q in the error when switching arguments to X509KeyPair, but the error was %q", subStr, err)
	}

	_, err = X509KeyPair([]byte(rsaCertPEM), []byte(rsaCertPEM))
	if err == nil {
		t.Fatalf("X509KeyPair didn't return an error when both arguments were certificates")
	}
	if subStr := "certificate"; !strings.Contains(err.Error(), subStr) {
		t.Fatalf("Expected %q in the error when both arguments to X509KeyPair were certificates, but the error was %q", subStr, err)
	}

	const nonsensePEM = `
-----BEGIN NONSENSE-----
Zm9vZm9vZm9v
-----END NONSENSE-----
`

	_, err = X509KeyPair([]byte(nonsensePEM), []byte(nonsensePEM))
	if err == nil {
		t.Fatalf("X509KeyPair didn't return an error when both arguments were nonsense")
	}
	if subStr := "NONSENSE"; !strings.Contains(err.Error(), subStr) {
		t.Fatalf("Expected %q in the error when both arguments to X509KeyPair were nonsense, but the error was %q", subStr, err)
	}
}

func TestX509MixedKeyPair(t *testing.T) {
	if _, err := X509KeyPair([]byte(rsaCertPEM), []byte(ecdsaKeyPEM)); err == nil {
		t.Error("Load of RSA certificate succeeded with ECDSA private key")
	}
	if _, err := X509KeyPair([]byte(ecdsaCertPEM), []byte(rsaKeyPEM)); err == nil {
		t.Error("Load of ECDSA certificate succeeded with RSA private key")
	}
}

func newLocalListener(t testing.TB) net.Listener {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		ln, err = net.Listen("tcp6", "[::1]:0")
	}
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

// tests that Conn.Read returns (non-zero, io.EOF) instead of
// (non-zero, nil) when a Close (alertCloseNotify) is sitting right
// behind the application data in the buffer.
func TestConnCloseBreakingWrite(t *testing.T) {
	ln := newLocalListener(t)
	defer ln.Close()

	srvCh := make(chan *Conn, 1)
	var serr error
	var sconn net.Conn
	go func() {
		var err error
		sconn, err = ln.Accept()
		if err != nil {
			serr = err
			srvCh <- nil
			return
		}
		serverConfig := testConfig.Clone()
		srv := Server(sconn, serverConfig)
		if err := srv.Handshake(); err != nil {
			serr = fmt.Errorf("handshake: %v", err)
			srvCh <- nil
			return
		}
		srvCh <- srv
	}()

	cconn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer cconn.Close()

	conn := &changeImplConn{
		Conn: cconn,
	}

	clientConfig := testConfig.Clone()
	tconn := Client(conn, clientConfig)
	if err := tconn.Handshake(); err != nil {
		t.Fatal(err)
	}

	srv := <-srvCh
	if srv == nil {
		t.Fatal(serr)
	}
	defer sconn.Close()

	connClosed := make(chan struct{})
	conn.closeFunc = func() error {
		close(connClosed)
		return nil
	}

	inWrite := make(chan bool, 1)
	var errConnClosed = errors.New("conn closed for test")
	conn.writeFunc = func(p []byte) (n int, err error) {
		inWrite <- true
		<-connClosed
		return 0, errConnClosed
	}

	closeReturned := make(chan bool, 1)
	go func() {
		<-inWrite
		tconn.Close() // test that this doesn't block forever.
		closeReturned <- true
	}()

	_, err = tconn.Write([]byte("foo"))
	if err != errConnClosed {
		t.Errorf("Write error = %v; want errConnClosed", err)
	}

	<-closeReturned
	if err := tconn.Close(); err != net.ErrClosed {
		t.Errorf("Close error = %v; want net.ErrClosed", err)
	}
}

func TestCloneFuncFields(t *testing.T) {
	const expectedCount = 8
	called := 0

	c1 := Config{
		Time: func() time.Time {
			called |= 1 << 0
			return time.Time{}
		},
		GetCertificate: func(*ClientHelloInfo) (*Certificate, error) {
			called |= 1 << 1
			return nil, nil
		},
		GetClientCertificate: func(*CertificateRequestInfo) (*Certificate, error) {
			called |= 1 << 2
			return nil, nil
		},
		GetConfigForClient: func(*ClientHelloInfo) (*Config, error) {
			called |= 1 << 3
			return nil, nil
		},
		VerifyPeerCertificate: func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			called |= 1 << 4
			return nil
		},
		VerifyConnection: func(ConnectionState) error {
			called |= 1 << 5
			return nil
		},
		UnwrapSession: func(identity []byte, cs ConnectionState) (*SessionState, error) {
			called |= 1 << 6
			return nil, nil
		},
		WrapSession: func(cs ConnectionState, ss *SessionState) ([]byte, error) {
			called |= 1 << 7
			return nil, nil
		},
	}

	c2 := c1.Clone()

	c2.Time()
	c2.GetCertificate(nil)
	c2.GetClientCertificate(nil)
	c2.GetConfigForClient(nil)
	c2.VerifyPeerCertificate(nil, nil)
	c2.VerifyConnection(ConnectionState{})
	c2.UnwrapSession(nil, ConnectionState{})
	c2.WrapSession(ConnectionState{}, nil)

	if called != (1<<expectedCount)-1 {
		t.Fatalf("expected %d calls but saw calls %b", expectedCount, called)
	}
}

func TestCloneNonFuncFields(t *testing.T) {
	var c1 Config
	v := reflect.ValueOf(&c1).Elem()

	typ := v.Type()
	for i := 0; i < typ.NumField(); i++ {
		f := v.Field(i)
		// testing/quick can't handle functions or interfaces and so
		// isn't used here.
		switch fn := typ.Field(i).Name; fn {
		case "Rand":
			f.Set(reflect.ValueOf(io.Reader(os.Stdin)))
		case "Time", "GetCertificate", "GetConfigForClient", "VerifyPeerCertificate", "VerifyConnection", "GetClientCertificate", "WrapSession", "UnwrapSession":
			// DeepEqual can't compare functions. If you add a
			// function field to this list, you must also change
			// TestCloneFuncFields to ensure that the func field is
			// cloned.
		case "Certificates":
			f.Set(reflect.ValueOf([]Certificate{
				{Certificate: [][]byte{{'b'}}},
			}))
		case "NameToCertificate":
			f.Set(reflect.ValueOf(map[string]*Certificate{"a": nil}))
		case "RootCAs", "ClientCAs":
			f.Set(reflect.ValueOf(x509.NewCertPool()))
		case "ClientSessionCache":
			f.Set(reflect.ValueOf(NewLRUClientSessionCache(10)))
		case "KeyLogWriter":
			f.Set(reflect.ValueOf(io.Writer(os.Stdout)))
		case "NextProtos":
			f.Set(reflect.ValueOf([]string{"a", "b"}))
		case "ServerName":
			f.Set(reflect.ValueOf("b"))
		case "ClientAuth":
			f.Set(reflect.ValueOf(VerifyClientCertIfGiven))
		case "InsecureSkipVerify", "SessionTicketsDisabled", "DynamicRecordSizingDisabled", "PreferServerCipherSuites":
			f.Set(reflect.ValueOf(true))
		case "MinVersion", "MaxVersion":
			f.Set(reflect.ValueOf(uint16(VersionTLS12)))
		case "SessionTicketKey":
			f.Set(reflect.ValueOf([32]byte{}))
		case "CipherSuites":
			f.Set(reflect.ValueOf([]uint16{1, 2}))
		case "CurvePreferences":
			f.Set(reflect.ValueOf([]CurveID{CurveP256}))
		case "Renegotiation":
			f.Set(reflect.ValueOf(RenegotiateOnceAsClient))
		case "mutex", "autoSessionTicketKeys", "sessionTicketKeys":
			continue // these are unexported fields that are handled separately
		default:
			t.Errorf("all fields must be accounted for, but saw unknown field %q", fn)
		}
	}
	// Set the unexported fields related to session ticket keys, which are copied with Clone().
	c1.autoSessionTicketKeys = []ticketKey{c1.ticketKeyFromBytes(c1.SessionTicketKey)}
	c1.sessionTicketKeys = []ticketKey{c1.ticketKeyFromBytes(c1.SessionTicketKey)}

	c2 := c1.Clone()
	if !reflect.DeepEqual(&c1, c2) {
		t.Errorf("clone failed to copy a field")
	}
}

func TestCloneNilConfig(t *testing.T) {
	var config *Config
	if cc := config.Clone(); cc != nil {
		t.Fatalf("Clone with nil should return nil, got: %+v", cc)
	}
}

// changeImplConn is a net.Conn which can change its Write and Close
// methods.
type changeImplConn struct {
	net.Conn
	writeFunc func([]byte) (int, error)
	closeFunc func() error
}

func (w *changeImplConn) Write(p []byte) (n int, err error) {
	if w.writeFunc != nil {
		return w.writeFunc(p)
	}
	return w.Conn.Write(p)
}

func (w *changeImplConn) Close() error {
	if w.closeFunc != nil {
		return w.closeFunc()
	}
	return w.Conn.Close()
}

func TestConnectionStateMarshal(t *testing.T) {
	cs := &ConnectionState{}
	_, err := json.Marshal(cs)
	if err != nil {
		t.Errorf("json.Marshal failed on ConnectionState: %v", err)
	}
}

func TestConnectionState(t *testing.T) {
	issuer, err := x509.ParseCertificate(testRSACertificateIssuer)
	if err != nil {
		panic(err)
	}
	rootCAs := x509.NewCertPool()
	rootCAs.AddCert(issuer)

	now := func() time.Time { return time.Unix(1476984729, 0) }

	const alpnProtocol = "golang"
	const serverName = "example.golang"
	var scts = [][]byte{[]byte("dummy sct 1"), []byte("dummy sct 2")}
	var ocsp = []byte("dummy ocsp")

	for _, v := range []uint16{VersionTLS13} {
		var name string
		switch v {
		case VersionTLS13:
			name = "TLSv13"
		}
		t.Run(name, func(t *testing.T) {
			config := &Config{
				Time:         now,
				Rand:         zeroSource{},
				Certificates: make([]Certificate, 1),
				MaxVersion:   v,
				RootCAs:      rootCAs,
				ClientCAs:    rootCAs,
				ClientAuth:   RequireAndVerifyClientCert,
				NextProtos:   []string{alpnProtocol},
				ServerName:   serverName,
			}
			config.Certificates[0].Certificate = [][]byte{testRSACertificate}
			config.Certificates[0].PrivateKey = testRSAPrivateKey
			config.Certificates[0].SignedCertificateTimestamps = scts
			config.Certificates[0].OCSPStaple = ocsp

			ss, cs, err := testHandshake(t, config, config)
			if err != nil {
				t.Fatalf("Handshake failed: %v", err)
			}

			if ss.Version != v || cs.Version != v {
				t.Errorf("Got versions %x (server) and %x (client), expected %x", ss.Version, cs.Version, v)
			}

			if !ss.HandshakeComplete || !cs.HandshakeComplete {
				t.Errorf("Got HandshakeComplete %v (server) and %v (client), expected true", ss.HandshakeComplete, cs.HandshakeComplete)
			}

			if ss.DidResume || cs.DidResume {
				t.Errorf("Got DidResume %v (server) and %v (client), expected false", ss.DidResume, cs.DidResume)
			}

			if ss.CipherSuite == 0 || cs.CipherSuite == 0 {
				t.Errorf("Got invalid cipher suite: %v (server) and %v (client)", ss.CipherSuite, cs.CipherSuite)
			}

			if ss.NegotiatedProtocol != alpnProtocol || cs.NegotiatedProtocol != alpnProtocol {
				t.Errorf("Got negotiated protocol %q (server) and %q (client), expected %q", ss.NegotiatedProtocol, cs.NegotiatedProtocol, alpnProtocol)
			}

			if !cs.NegotiatedProtocolIsMutual {
				t.Errorf("Got false NegotiatedProtocolIsMutual on the client side")
			}
			// NegotiatedProtocolIsMutual on the server side is unspecified.

			if ss.ServerName != serverName {
				t.Errorf("Got server name %q, expected %q", ss.ServerName, serverName)
			}
			if cs.ServerName != serverName {
				t.Errorf("Got server name on client connection %q, expected %q", cs.ServerName, serverName)
			}

			if len(ss.PeerCertificates) != 1 || len(cs.PeerCertificates) != 1 {
				t.Errorf("Got %d (server) and %d (client) peer certificates, expected %d", len(ss.PeerCertificates), len(cs.PeerCertificates), 1)
			}

			if len(ss.VerifiedChains) != 1 || len(cs.VerifiedChains) != 1 {
				t.Errorf("Got %d (server) and %d (client) verified chains, expected %d", len(ss.VerifiedChains), len(cs.VerifiedChains), 1)
			} else if len(ss.VerifiedChains[0]) != 2 || len(cs.VerifiedChains[0]) != 2 {
				t.Errorf("Got %d (server) and %d (client) long verified chain, expected %d", len(ss.VerifiedChains[0]), len(cs.VerifiedChains[0]), 2)
			}

			if len(cs.SignedCertificateTimestamps) != 2 {
				t.Errorf("Got %d SCTs, expected %d", len(cs.SignedCertificateTimestamps), 2)
			}
			if !bytes.Equal(cs.OCSPResponse, ocsp) {
				t.Errorf("Got OCSPs %x, expected %x", cs.OCSPResponse, ocsp)
			}
			// Only TLS 1.3 supports OCSP and SCTs on client certs.
			if v == VersionTLS13 {
				if len(ss.SignedCertificateTimestamps) != 2 {
					t.Errorf("Got %d client SCTs, expected %d", len(ss.SignedCertificateTimestamps), 2)
				}
				if !bytes.Equal(ss.OCSPResponse, ocsp) {
					t.Errorf("Got client OCSPs %x, expected %x", ss.OCSPResponse, ocsp)
				}
			}

			if v == VersionTLS13 {
				if ss.TLSUnique != nil || cs.TLSUnique != nil {
					t.Errorf("Got TLSUnique %x (server) and %x (client), expected nil in TLS 1.3", ss.TLSUnique, cs.TLSUnique)
				}
			} else {
				if ss.TLSUnique == nil || cs.TLSUnique == nil {
					t.Errorf("Got TLSUnique %x (server) and %x (client), expected non-nil", ss.TLSUnique, cs.TLSUnique)
				}
			}
		})
	}
}

// Issue 28744: Ensure that we don't modify memory
// that Config doesn't own such as Certificates.
func TestBuildNameToCertificate_doesntModifyCertificates(t *testing.T) {
	c0 := Certificate{
		Certificate: [][]byte{testRSACertificate},
		PrivateKey:  testRSAPrivateKey,
	}
	c1 := Certificate{
		Certificate: [][]byte{testSNICertificate},
		PrivateKey:  testRSAPrivateKey,
	}
	config := testConfig.Clone()
	config.Certificates = []Certificate{c0, c1}

	config.BuildNameToCertificate()
	got := config.Certificates
	want := []Certificate{c0, c1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Certificates were mutated by BuildNameToCertificate\nGot: %#v\nWant: %#v\n", got, want)
	}
}

func testingKey(s string) string { return strings.ReplaceAll(s, "TESTING KEY", "PRIVATE KEY") }

func TestClientHelloInfo_SupportsCertificate(t *testing.T) {
	rsaCert := &Certificate{
		Certificate: [][]byte{testRSACertificate},
		PrivateKey:  testRSAPrivateKey,
	}
	pkcs1Cert := &Certificate{
		Certificate:                  [][]byte{testRSACertificate},
		PrivateKey:                   testRSAPrivateKey,
		SupportedSignatureAlgorithms: []SignatureScheme{PKCS1WithSHA1, PKCS1WithSHA256},
	}
	ecdsaCert := &Certificate{
		// ECDSA P-256 certificate
		Certificate: [][]byte{testP256Certificate},
		PrivateKey:  testP256PrivateKey,
	}
	ed25519Cert := &Certificate{
		Certificate: [][]byte{testEd25519Certificate},
		PrivateKey:  testEd25519PrivateKey,
	}

	tests := []struct {
		c       *Certificate
		chi     *ClientHelloInfo
		wantErr string
	}{
		{rsaCert, &ClientHelloInfo{
			ServerName:        "example.golang",
			SignatureSchemes:  []SignatureScheme{PSSWithSHA256},
			SupportedVersions: []uint16{VersionTLS13},
		}, ""},
		{ecdsaCert, &ClientHelloInfo{
			SignatureSchemes:  []SignatureScheme{PSSWithSHA256, ECDSAWithP256AndSHA256},
			SupportedVersions: []uint16{VersionTLS13},
		}, ""},
		{rsaCert, &ClientHelloInfo{
			ServerName:        "example.com",
			SignatureSchemes:  []SignatureScheme{PSSWithSHA256},
			SupportedVersions: []uint16{VersionTLS13},
		}, "not valid for requested server name"},
		{ecdsaCert, &ClientHelloInfo{
			SignatureSchemes:  []SignatureScheme{ECDSAWithP384AndSHA384},
			SupportedVersions: []uint16{VersionTLS13},
		}, "signature algorithms"},
		{pkcs1Cert, &ClientHelloInfo{
			SignatureSchemes:  []SignatureScheme{PSSWithSHA256, ECDSAWithP256AndSHA256},
			SupportedVersions: []uint16{VersionTLS13},
		}, "signature algorithms"},
		{ed25519Cert, &ClientHelloInfo{
			SignatureSchemes:  []SignatureScheme{Ed25519},
			SupportedVersions: []uint16{VersionTLS13},
		}, ""},
	}
	for i, tt := range tests {
		err := tt.chi.SupportsCertificate(tt.c)
		switch {
		case tt.wantErr == "" && err != nil:
			t.Errorf("%d: unexpected error: %v", i, err)
		case tt.wantErr != "" && err == nil:
			t.Errorf("%d: unexpected success", i)
		case tt.wantErr != "" && !strings.Contains(err.Error(), tt.wantErr):
			t.Errorf("%d: got error %q, expected %q", i, err, tt.wantErr)
		}
	}
}

func TestCipherSuites(t *testing.T) {
	var lastID uint16
	for _, c := range CipherSuites() {
		if lastID > c.ID {
			t.Errorf("CipherSuites are not ordered by ID: got %#04x after %#04x", c.ID, lastID)
		} else {
			lastID = c.ID
		}

		if c.Insecure {
			t.Errorf("%#04x: Insecure CipherSuite returned by CipherSuites()", c.ID)
		}
	}
	lastID = 0
	for _, c := range InsecureCipherSuites() {
		if lastID > c.ID {
			t.Errorf("InsecureCipherSuites are not ordered by ID: got %#04x after %#04x", c.ID, lastID)
		} else {
			lastID = c.ID
		}

		if !c.Insecure {
			t.Errorf("%#04x: not Insecure CipherSuite returned by InsecureCipherSuites()", c.ID)
		}
	}

	CipherSuiteByID := func(id uint16) *CipherSuite {
		for _, c := range CipherSuites() {
			if c.ID == id {
				return c
			}
		}
		for _, c := range InsecureCipherSuites() {
			if c.ID == id {
				return c
			}
		}
		return nil
	}

	for _, c := range cipherSuitesTLS13 {
		cc := CipherSuiteByID(c.id)
		if cc == nil {
			t.Errorf("%#04x: no CipherSuite entry", c.id)
			continue
		}

		if cc.Insecure {
			t.Errorf("%#04x: Insecure %v, expected false", c.id, cc.Insecure)
		}
		if len(cc.SupportedVersions) != 1 || cc.SupportedVersions[0] != VersionTLS13 {
			t.Errorf("%#04x: suite is TLS 1.3 only, but SupportedVersions is %v", c.id, cc.SupportedVersions)
		}

		if got := CipherSuiteName(c.id); got != cc.Name {
			t.Errorf("%#04x: unexpected CipherSuiteName: got %q, expected %q", c.id, got, cc.Name)
		}
	}

	if got := CipherSuiteName(0xabc); got != "0x0ABC" {
		t.Errorf("unexpected fallback CipherSuiteName: got %q, expected 0x0ABC", got)
	}
}

func TestVersionName(t *testing.T) {
	if got, exp := VersionName(VersionTLS13), "TLS 1.3"; got != exp {
		t.Errorf("unexpected VersionName: got %q, expected %q", got, exp)
	}
	if got, exp := VersionName(0x12a), "0x012A"; got != exp {
		t.Errorf("unexpected fallback VersionName: got %q, expected %q", got, exp)
	}
}

// TestPKCS1OnlyCert uses a client certificate with a broken crypto.Signer that
// always makes PKCS #1 v1.5 signatures, so can't be used with RSA-PSS.
func TestPKCS1OnlyCert(t *testing.T) {
	// PKCS #1 v1.5 signature algorithms are not supported in TLS 1.3.
	t.Skip("PKCS #1 v1.5 signatures are not used in TLS 1.3")
}

func TestVerifyCertificates(t *testing.T) {
	// See https://go.dev/issue/31641.
	t.Run("TLSv13", func(t *testing.T) { testVerifyCertificates(t, VersionTLS13) })
}

func testVerifyCertificates(t *testing.T, version uint16) {
	tests := []struct {
		name string

		InsecureSkipVerify bool
		ClientAuth         ClientAuthType
		ClientCertificates bool
	}{
		{
			name: "defaults",
		},
		{
			name:               "InsecureSkipVerify",
			InsecureSkipVerify: true,
		},
		{
			name:       "RequestClientCert with no certs",
			ClientAuth: RequestClientCert,
		},
		{
			name:               "RequestClientCert with certs",
			ClientAuth:         RequestClientCert,
			ClientCertificates: true,
		},
		{
			name:               "RequireAnyClientCert",
			ClientAuth:         RequireAnyClientCert,
			ClientCertificates: true,
		},
		{
			name:       "VerifyClientCertIfGiven with no certs",
			ClientAuth: VerifyClientCertIfGiven,
		},
		{
			name:               "VerifyClientCertIfGiven with certs",
			ClientAuth:         VerifyClientCertIfGiven,
			ClientCertificates: true,
		},
		{
			name:               "RequireAndVerifyClientCert",
			ClientAuth:         RequireAndVerifyClientCert,
			ClientCertificates: true,
		},
	}

	issuer, err := x509.ParseCertificate(testRSACertificateIssuer)
	if err != nil {
		t.Fatal(err)
	}
	rootCAs := x509.NewCertPool()
	rootCAs.AddCert(issuer)

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var serverVerifyConnection, clientVerifyConnection bool
			var serverVerifyPeerCertificates, clientVerifyPeerCertificates bool

			clientConfig := testConfig.Clone()
			clientConfig.Time = func() time.Time { return time.Unix(1476984729, 0) }
			clientConfig.MaxVersion = version
			clientConfig.MinVersion = version
			clientConfig.RootCAs = rootCAs
			clientConfig.ServerName = "example.golang"
			clientConfig.ClientSessionCache = NewLRUClientSessionCache(1)
			serverConfig := clientConfig.Clone()
			serverConfig.ClientCAs = rootCAs

			clientConfig.VerifyConnection = func(cs ConnectionState) error {
				clientVerifyConnection = true
				return nil
			}
			clientConfig.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
				clientVerifyPeerCertificates = true
				return nil
			}
			serverConfig.VerifyConnection = func(cs ConnectionState) error {
				serverVerifyConnection = true
				return nil
			}
			serverConfig.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
				serverVerifyPeerCertificates = true
				return nil
			}

			clientConfig.InsecureSkipVerify = test.InsecureSkipVerify
			serverConfig.ClientAuth = test.ClientAuth
			if !test.ClientCertificates {
				clientConfig.Certificates = nil
			}

			if _, _, err := testHandshake(t, clientConfig, serverConfig); err != nil {
				t.Fatal(err)
			}

			want := serverConfig.ClientAuth != NoClientCert
			if serverVerifyPeerCertificates != want {
				t.Errorf("VerifyPeerCertificates on the server: got %v, want %v",
					serverVerifyPeerCertificates, want)
			}
			if !clientVerifyPeerCertificates {
				t.Errorf("VerifyPeerCertificates not called on the client")
			}
			if !serverVerifyConnection {
				t.Error("VerifyConnection did not get called on the server")
			}
			if !clientVerifyConnection {
				t.Error("VerifyConnection did not get called on the client")
			}

			serverVerifyPeerCertificates, clientVerifyPeerCertificates = false, false
			serverVerifyConnection, clientVerifyConnection = false, false
			cs, _, err := testHandshake(t, clientConfig, serverConfig)
			if err != nil {
				t.Fatal(err)
			}
			if !cs.DidResume {
				t.Error("expected resumption")
			}

			if serverVerifyPeerCertificates {
				t.Error("VerifyPeerCertificates got called on the server on resumption")
			}
			if clientVerifyPeerCertificates {
				t.Error("VerifyPeerCertificates got called on the client on resumption")
			}
			if !serverVerifyConnection {
				t.Error("VerifyConnection did not get called on the server on resumption")
			}
			if !clientVerifyConnection {
				t.Error("VerifyConnection did not get called on the client on resumption")
			}
		})
	}
}
