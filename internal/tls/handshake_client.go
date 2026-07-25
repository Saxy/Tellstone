// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tls

import (
	"context"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

var testingOnlyForceClientHelloSignatureAlgorithms []SignatureScheme

func (c *Conn) makeClientHello() (*clientHelloMsg, *ecdh.PrivateKey, error) {
	config := c.config
	if len(config.ServerName) == 0 && !config.InsecureSkipVerify {
		return nil, nil, errors.New("tls: either ServerName or InsecureSkipVerify must be specified in the tls.Config")
	}

	nextProtosLength := 0
	for _, proto := range config.NextProtos {
		if l := len(proto); l == 0 || l > 255 {
			return nil, nil, errors.New("tls: invalid NextProtos value")
		} else {
			nextProtosLength += 1 + l
		}
	}
	if nextProtosLength > 0xffff {
		return nil, nil, errors.New("tls: NextProtos values too large")
	}

	supportedVersions := config.supportedVersions(roleClient)
	if len(supportedVersions) == 0 {
		return nil, nil, errors.New("tls: no supported versions satisfy MinVersion and MaxVersion")
	}

	clientHelloVersion := config.maxSupportedVersion(roleClient)
	// The version at the beginning of the ClientHello was capped at TLS 1.2
	// for compatibility reasons. The supported_versions extension is used
	// to negotiate versions now. See RFC 8446, Section 4.2.1.
	if clientHelloVersion > VersionTLS12 {
		clientHelloVersion = VersionTLS12
	}

	hello := &clientHelloMsg{
		vers:                         clientHelloVersion,
		compressionMethods:           []uint8{compressionNone},
		random:                       make([]byte, 32),
		extendedMasterSecret:         true,
		ocspStapling:                 true,
		scts:                         true,
		serverName:                   hostnameInSNI(config.ServerName),
		supportedCurves:              config.curvePreferences(),
		supportedPoints:              []uint8{pointFormatUncompressed},
		secureRenegotiationSupported: true,
		alpnProtocols:                config.NextProtos,
		supportedVersions:            supportedVersions,
	}

	hello.cipherSuites = make([]uint16, 0)

	_, err := io.ReadFull(config.rand(), hello.random)
	if err != nil {
		return nil, nil, errors.New("tls: short read from Rand: " + err.Error())
	}

	// A random session ID is used to detect when the server accepted a ticket
	// and is resuming a session (see RFC 5077). In TLS 1.3, it's always set as
	// a compatibility measure (see RFC 8446, Section 4.1.2).
	hello.sessionId = make([]byte, 32)
	if _, err := io.ReadFull(config.rand(), hello.sessionId); err != nil {
		return nil, nil, errors.New("tls: short read from Rand: " + err.Error())
	}

	if hello.vers >= VersionTLS12 {
		hello.supportedSignatureAlgorithms = supportedSignatureAlgorithms()
	}
	if testingOnlyForceClientHelloSignatureAlgorithms != nil {
		hello.supportedSignatureAlgorithms = testingOnlyForceClientHelloSignatureAlgorithms
	}

	var key *ecdh.PrivateKey
	if hello.supportedVersions[0] == VersionTLS13 {
		// Reset the list of ciphers when the client only supports TLS 1.3.
		if len(hello.supportedVersions) == 1 {
			hello.cipherSuites = nil
		}
		if hasAESGCMHardwareSupport {
			hello.cipherSuites = append(hello.cipherSuites, defaultCipherSuitesTLS13...)
		} else {
			hello.cipherSuites = append(hello.cipherSuites, defaultCipherSuitesTLS13NoAES...)
		}

		curveID := config.curvePreferences()[0]
		if _, ok := curveForCurveID(curveID); !ok {
			return nil, nil, errors.New("tls: CurvePreferences includes unsupported curve")
		}
		key, err = generateECDHEKey(config.rand(), curveID)
		if err != nil {
			return nil, nil, err
		}
		hello.keyShares = []keyShare{{group: curveID, data: key.PublicKey().Bytes()}}
	}

	return hello, key, nil
}

func (c *Conn) clientHandshake(ctx context.Context) error {
	if c.config == nil {
		c.config = defaultConfig()
	}

	c.didResume = false

	hello, ecdheKey, err := c.makeClientHello()
	if err != nil {
		return err
	}
	c.serverName = hello.serverName

	session, earlySecret, binderKey, err := c.loadSession(hello)
	if err != nil {
		return err
	}
	if session != nil {
		defer func() {
			if err != nil {
				if cacheKey := c.clientSessionCacheKey(); cacheKey != "" {
					c.config.ClientSessionCache.Put(cacheKey, nil)
				}
			}
		}()
	}

	if _, err := c.writeHandshakeRecord(hello, nil); err != nil {
		return err
	}

	msg, err := c.readHandshake(nil)
	if err != nil {
		return err
	}

	serverHello, ok := msg.(*serverHelloMsg)
	if !ok {
		c.sendAlert(alertUnexpectedMessage)
		return unexpectedMessageError(serverHello, msg)
	}

	if err := c.pickTLSVersion(serverHello); err != nil {
		return err
	}

	hs := &clientHandshakeStateTLS13{
		c:           c,
		ctx:         ctx,
		serverHello: serverHello,
		hello:       hello,
		ecdheKey:    ecdheKey,
		session:     session,
		earlySecret: earlySecret,
		binderKey:   binderKey,
	}

	return hs.handshake()
}

func (c *Conn) loadSession(hello *clientHelloMsg) (
	session *SessionState, earlySecret, binderKey []byte, err error) {
	if c.config.SessionTicketsDisabled || c.config.ClientSessionCache == nil {
		return nil, nil, nil, nil
	}

	hello.ticketSupported = true

	if hello.supportedVersions[0] == VersionTLS13 {
		// Require DHE on resumption as it guarantees forward secrecy against
		// compromise of the session ticket key. See RFC 8446, Section 4.2.9.
		hello.pskModes = []uint8{pskModeDHE}
	}

	// Session resumption is not allowed if renegotiating because
	// renegotiation is primarily used to allow a client to send a client
	// certificate, which would be skipped if session resumption occurred.
	if c.handshakes != 0 {
		return nil, nil, nil, nil
	}

	// Try to resume a previously negotiated TLS session, if available.
	cacheKey := c.clientSessionCacheKey()
	if cacheKey == "" {
		return nil, nil, nil, nil
	}
	cs, ok := c.config.ClientSessionCache.Get(cacheKey)
	if !ok || cs == nil {
		return nil, nil, nil, nil
	}
	session = cs.session

	// Check that version used for the previous session is still valid.
	versOk := false
	for _, v := range hello.supportedVersions {
		if v == session.version {
			versOk = true
			break
		}
	}
	if !versOk {
		return nil, nil, nil, nil
	}

	// Check that the cached server certificate is not expired, and that it's
	// valid for the ServerName. This should be ensured by the cache key, but
	// protect the application from a faulty ClientSessionCache implementation.
	if c.config.time().After(session.peerCertificates[0].NotAfter) {
		// Expired certificate, delete the entry.
		c.config.ClientSessionCache.Put(cacheKey, nil)
		return nil, nil, nil, nil
	}
	if !c.config.InsecureSkipVerify {
		if len(session.verifiedChains) == 0 {
			// The original connection had InsecureSkipVerify, while this doesn't.
			return nil, nil, nil, nil
		}
		if err := session.peerCertificates[0].VerifyHostname(c.config.ServerName); err != nil {
			return nil, nil, nil, nil
		}
	}

	if session.version != VersionTLS13 {
		return nil, nil, nil, nil
	}

	// Check that the session ticket is not expired.
	if c.config.time().After(time.Unix(int64(session.useBy), 0)) {
		c.config.ClientSessionCache.Put(cacheKey, nil)
		return nil, nil, nil, nil
	}

	// In TLS 1.3 the KDF hash must match the resumed session. Ensure we
	// offer at least one cipher suite with that hash.
	cipherSuite := cipherSuiteTLS13ByID(session.cipherSuite)
	if cipherSuite == nil {
		return nil, nil, nil, nil
	}
	cipherSuiteOk := false
	for _, offeredID := range hello.cipherSuites {
		offeredSuite := cipherSuiteTLS13ByID(offeredID)
		if offeredSuite != nil && offeredSuite.hash == cipherSuite.hash {
			cipherSuiteOk = true
			break
		}
	}
	if !cipherSuiteOk {
		return nil, nil, nil, nil
	}

	// Set the pre_shared_key extension. See RFC 8446, Section 4.2.11.1.
	ticketAge := c.config.time().Sub(time.Unix(int64(session.createdAt), 0))
	identity := pskIdentity{
		label:               cs.ticket,
		obfuscatedTicketAge: uint32(ticketAge/time.Millisecond) + session.ageAdd,
	}
	hello.pskIdentities = []pskIdentity{identity}
	hello.pskBinders = [][]byte{make([]byte, cipherSuite.hash.Size())}

	// Compute the PSK binders. See RFC 8446, Section 4.2.11.2.
	earlySecret = cipherSuite.extract(session.secret, nil)
	binderKey = cipherSuite.deriveSecret(earlySecret, resumptionBinderLabel, nil)
	transcript := cipherSuite.hash.New()
	helloBytes, err := hello.marshalWithoutBinders()
	if err != nil {
		return nil, nil, nil, err
	}
	transcript.Write(helloBytes)
	pskBinders := [][]byte{cipherSuite.finishedHash(binderKey, transcript)}
	if err := hello.updateBinders(pskBinders); err != nil {
		return nil, nil, nil, err
	}

	return
}

func (c *Conn) pickTLSVersion(serverHello *serverHelloMsg) error {
	peerVersion := serverHello.vers
	if serverHello.supportedVersion != 0 {
		peerVersion = serverHello.supportedVersion
	}

	vers, ok := c.config.mutualVersion(roleClient, []uint16{peerVersion})
	if !ok {
		c.sendAlert(alertProtocolVersion)
		return fmt.Errorf("tls: server selected unsupported protocol version %x", peerVersion)
	}

	c.vers = vers
	c.haveVers = true
	c.in.version = vers
	c.out.version = vers

	return nil
}

// checkALPN ensure that the server's choice of ALPN protocol is compatible with
// the protocols that we advertised in the Client Hello.
func checkALPN(clientProtos []string, serverProto string) error {
	if serverProto == "" {
		return nil
	}
	if len(clientProtos) == 0 {
		return errors.New("tls: server advertised unrequested ALPN extension")
	}
	for _, proto := range clientProtos {
		if proto == serverProto {
			return nil
		}
	}
	return errors.New("tls: server selected unadvertised ALPN protocol")
}

// defaultMaxRSAKeySize is the maximum RSA key size in bits that we are willing
// to verify the signatures of during a TLS handshake.
const defaultMaxRSAKeySize = 8192

func checkKeySize(n int) (max int, ok bool) {
	return defaultMaxRSAKeySize, n <= defaultMaxRSAKeySize
}

// verifyServerCertificate parses and verifies the provided chain, setting
// c.verifiedChains and c.peerCertificates or sending the appropriate alert.
func (c *Conn) verifyServerCertificate(certificates [][]byte) error {
	activeHandles := make([]*activeCert, len(certificates))
	certs := make([]*x509.Certificate, len(certificates))
	for i, asn1Data := range certificates {
		cert, err := globalCertCache.newCert(asn1Data)
		if err != nil {
			c.sendAlert(alertBadCertificate)
			return errors.New("tls: failed to parse certificate from server: " + err.Error())
		}
		if cert.cert.PublicKeyAlgorithm == x509.RSA {
			n := cert.cert.PublicKey.(*rsa.PublicKey).N.BitLen()
			if max, ok := checkKeySize(n); !ok {
				c.sendAlert(alertBadCertificate)
				return fmt.Errorf("tls: server sent certificate containing RSA key larger than %d bits", max)
			}
		}
		activeHandles[i] = cert
		certs[i] = cert.cert
	}

	if !c.config.InsecureSkipVerify {
		opts := x509.VerifyOptions{
			Roots:         c.config.RootCAs,
			CurrentTime:   c.config.time(),
			DNSName:       c.config.ServerName,
			Intermediates: x509.NewCertPool(),
		}

		for _, cert := range certs[1:] {
			opts.Intermediates.AddCert(cert)
		}
		var err error
		c.verifiedChains, err = certs[0].Verify(opts)
		if err != nil {
			c.sendAlert(alertBadCertificate)
			return &CertificateVerificationError{UnverifiedCertificates: certs, Err: err}
		}
	}

	switch certs[0].PublicKey.(type) {
	case *rsa.PublicKey, *ecdsa.PublicKey, ed25519.PublicKey:
		break
	default:
		c.sendAlert(alertUnsupportedCertificate)
		return fmt.Errorf("tls: server's certificate contains an unsupported type of public key: %T", certs[0].PublicKey)
	}

	c.activeCertHandles = activeHandles
	c.peerCertificates = certs

	if c.config.VerifyPeerCertificate != nil {
		if err := c.config.VerifyPeerCertificate(certificates, c.verifiedChains); err != nil {
			c.sendAlert(alertBadCertificate)
			return err
		}
	}

	if c.config.VerifyConnection != nil {
		if err := c.config.VerifyConnection(c.connectionStateLocked()); err != nil {
			c.sendAlert(alertBadCertificate)
			return err
		}
	}

	return nil
}

// certificateRequestInfoFromMsg generates a CertificateRequestInfo from a TLS
// <= 1.2 CertificateRequest, making an effort to fill in missing information.
func certificateRequestInfoFromMsg(ctx context.Context, vers uint16, certReq *certificateRequestMsg) *CertificateRequestInfo {
	cri := &CertificateRequestInfo{
		AcceptableCAs: certReq.certificateAuthorities,
		Version:       vers,
		ctx:           ctx,
	}

	var rsaAvail, ecAvail bool
	for _, certType := range certReq.certificateTypes {
		switch certType {
		case certTypeRSASign:
			rsaAvail = true
		case certTypeECDSASign:
			ecAvail = true
		}
	}

	if !certReq.hasSignatureAlgorithm {
		// Prior to TLS 1.2, signature schemes did not exist. In this case we
		// make up a list based on the acceptable certificate types, to help
		// GetClientCertificate and SupportsCertificate select the right certificate.
		// The hash part of the SignatureScheme is a lie here, because
		// TLS 1.0 and 1.1 always use MD5+SHA1 for RSA and SHA1 for ECDSA.
		switch {
		case rsaAvail && ecAvail:
			cri.SignatureSchemes = []SignatureScheme{
				ECDSAWithP256AndSHA256, ECDSAWithP384AndSHA384, ECDSAWithP521AndSHA512,
				PKCS1WithSHA256, PKCS1WithSHA384, PKCS1WithSHA512, PKCS1WithSHA1,
			}
		case rsaAvail:
			cri.SignatureSchemes = []SignatureScheme{
				PKCS1WithSHA256, PKCS1WithSHA384, PKCS1WithSHA512, PKCS1WithSHA1,
			}
		case ecAvail:
			cri.SignatureSchemes = []SignatureScheme{
				ECDSAWithP256AndSHA256, ECDSAWithP384AndSHA384, ECDSAWithP521AndSHA512,
			}
		}
		return cri
	}

	// Filter the signature schemes based on the certificate types.
	// See RFC 5246, Section 7.4.4 (where it calls this "somewhat complicated").
	cri.SignatureSchemes = make([]SignatureScheme, 0, len(certReq.supportedSignatureAlgorithms))
	for _, sigScheme := range certReq.supportedSignatureAlgorithms {
		sigType, _, err := typeAndHashFromSignatureScheme(sigScheme)
		if err != nil {
			continue
		}
		switch sigType {
		case signatureECDSA, signatureEd25519:
			if ecAvail {
				cri.SignatureSchemes = append(cri.SignatureSchemes, sigScheme)
			}
		case signatureRSAPSS, signaturePKCS1v15:
			if rsaAvail {
				cri.SignatureSchemes = append(cri.SignatureSchemes, sigScheme)
			}
		}
	}

	return cri
}

func (c *Conn) getClientCertificate(cri *CertificateRequestInfo) (*Certificate, error) {
	if c.config.GetClientCertificate != nil {
		return c.config.GetClientCertificate(cri)
	}

	for _, chain := range c.config.Certificates {
		if err := cri.SupportsCertificate(&chain); err != nil {
			continue
		}
		return &chain, nil
	}

	// No acceptable certificate found. Don't send a certificate.
	return new(Certificate), nil
}

// clientSessionCacheKey returns a key used to cache sessionTickets that could
// be used to resume previously negotiated TLS sessions with a server.
func (c *Conn) clientSessionCacheKey() string {
	if len(c.config.ServerName) > 0 {
		return c.config.ServerName
	}
	if c.conn != nil {
		return c.conn.RemoteAddr().String()
	}
	return ""
}

// hostnameInSNI converts name into an appropriate hostname for SNI.
// Literal IP addresses and absolute FQDNs are not permitted as SNI values.
// See RFC 6066, Section 3.
func hostnameInSNI(name string) string {
	host := name
	if len(host) > 0 && host[0] == '[' && host[len(host)-1] == ']' {
		host = host[1 : len(host)-1]
	}
	if i := strings.LastIndex(host, "%"); i > 0 {
		host = host[:i]
	}
	if net.ParseIP(host) != nil {
		return ""
	}
	for len(name) > 0 && name[len(name)-1] == '.' {
		name = name[:len(name)-1]
	}
	return name
}
