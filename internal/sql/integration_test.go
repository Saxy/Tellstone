/*
Package sql
Tellstone PostgreSQL Wire Frontend Tests
File: integration_test.go
Description: End-to-end PGv3 wire tests driving a real server socket: startup /
auth exchanges, simple-query CRUD, error codes, transactions, the extended
protocol, SSL negotiation, the cleartext-over-TLS gate and RBAC enforcement.
The client side is hand-rolled so the suite exercises the exact frame encoding
without depending on a third-party driver (psql/pgx cover interop separately).
*/
package sql

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/rbac"
	tlslib "github.com/Saxy/Tellstone/internal/tls"
	"github.com/Saxy/Tellstone/logger"
)

// ---- fake store ----

type testStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newTestStore() *testStore { return &testStore{data: map[string][]byte{}} }

func (s *testStore) GetErr(key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), v...), true, nil
}

func (s *testStore) Set(key string, value []byte, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = append([]byte(nil), value...)
	return nil
}

func (s *testStore) Delete(key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data[key]
	delete(s.data, key)
	return ok, nil
}

// ---- minimal PGv3 client ----

type frame struct {
	typ byte
	val []byte
}

type tclient struct {
	t  *testing.T
	cn net.Conn
	r  *bufio.Reader
}

func dialServer(t *testing.T, addr string) *tclient {
	t.Helper()
	cn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = cn.Close() })
	return &tclient{t: t, cn: cn, r: bufio.NewReader(cn)}
}

// sendStartup writes a startup-format frame (length + body, no type byte).
func (cl *tclient) sendStartup(body []byte) {
	cl.t.Helper()
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(4+len(body)))
	if _, err := cl.cn.Write(hdr[:]); err != nil {
		cl.t.Fatalf("write startup: %v", err)
	}
	if _, err := cl.cn.Write(body); err != nil {
		cl.t.Fatalf("write startup: %v", err)
	}
}

func startupBody(code int32, params ...string) []byte {
	body := make([]byte, 4)
	binary.BigEndian.PutUint32(body, uint32(code))
	for _, kv := range params {
		body = append(body, kv...)
		body = append(body, 0)
	}
	body = append(body, 0)
	return body
}

func (cl *tclient) send(typ byte, payload []byte) {
	cl.t.Helper()
	var m msgBuilder
	m.begin(typ)
	m.bytes(payload)
	m.end()
	if _, err := cl.cn.Write(m.buf); err != nil {
		cl.t.Fatalf("write %q: %v", typ, err)
	}
}

func (cl *tclient) recv() frame {
	cl.t.Helper()
	typ, p, err := readFrame(cl.r)
	if err != nil {
		cl.t.Fatalf("read frame: %v", err)
	}
	if os.Getenv("SQL_DEBUG_LOG") != "" {
		fmt.Fprintf(os.Stderr, "recv[%c] payload=%q\n", typ, p)
	}
	return frame{typ: typ, val: p}
}

// recvUntil reads frames until the wanted type appears, returning everything.
func (cl *tclient) recvUntil(want byte) []frame {
	cl.t.Helper()
	var out []frame
	for {
		f := cl.recv()
		out = append(out, f)
		if f.typ == want {
			return out
		}
	}
}

// expectLine reads a single frame and checks its type.
func (cl *tclient) expectLine(want byte) frame {
	cl.t.Helper()
	f := cl.recv()
	if f.typ != want {
		cl.t.Fatalf("expected message %q, got %q (payload %q)", want, f.typ, f.val)
	}
	return f
}

func (cl *tclient) startupTrust(user string) {
	cl.t.Helper()
	if user == "" {
		user = "default"
	}
	cl.sendStartup(startupBody(protocolVersionNumber, "user", user, "database", "test"))
	frames := recvStartupOK(cl, "")
	if len(frames) != 1+5 {
		cl.t.Fatalf("trust startup: expected 6 frames (auth+4 status+keydata+ready...), got %d", len(frames))
	}
	cl.expectLine(msgReadyForQuery)
}

// recvStartupOK reads the post-auth banner. extra may be an expected
// AuthenticationCleartextPassword frame (password mode). Returns the frames read.
func recvStartupOK(cl *tclient, extra string) []frame {
	cl.t.Helper()
	var out []frame
	cl.expectLine(msgAuthentication) // AuthenticationOk
	out = append(out, frame{msgAuthentication, nil})
	for i := 0; i < 4; i++ {
		f := cl.expectLine(msgParameterStatus)
		out = append(out, f)
	}
	k := cl.expectLine(msgBackendKeyData)
	out = append(out, k)
	return out
}

func (cl *tclient) startupPassword(user, pass string) {
	cl.t.Helper()
	cl.startTLS()
	cl.sendStartup(startupBody(protocolVersionNumber, "user", user, "database", "test"))
	f := cl.expectLine(msgAuthentication)
	if code := int32(binary.BigEndian.Uint32(f.val)); code != authCleartextPassword {
		cl.t.Fatalf("expected CleartextPassword (%d), got auth code %d", authCleartextPassword, code)
	}
	cl.send(msgPassword, append([]byte(pass), 0))
	cl.startupReadBanner()
}

func (cl *tclient) startupReadBanner() {
	cl.t.Helper()
	cl.expectLine(msgAuthentication) // AuthenticationOk
	for i := 0; i < 4; i++ {
		cl.expectLine(msgParameterStatus)
	}
	cl.expectLine(msgBackendKeyData)
	cl.expectLine(msgReadyForQuery)
}

// startTLS performs the SSLRequest negotiation and client handshake.
func (cl *tclient) startTLS() {
	cl.t.Helper()
	cl.sendStartup(startupBody(sslRequestCode))
	resp, err := cl.r.ReadByte()
	if err != nil {
		cl.t.Fatalf("read SSLRequest reply: %v", err)
	}
	if resp != 'S' {
		cl.t.Fatalf("expected 'S' from SSLRequest, got %q", resp)
	}
	tc := tlslib.Client(cl.cn, &tlslib.Config{InsecureSkipVerify: true})
	if err := tc.Handshake(); err != nil {
		cl.t.Fatalf("tls handshake: %v", err)
	}
	cl.cn = tc
	cl.r = bufio.NewReader(tc)
}

// query sends a simple-Query and returns all frames through ReadyForQuery.
func (cl *tclient) query(q string) []frame {
	cl.t.Helper()
	cl.send(msgQuery, append([]byte(q), 0))
	return cl.recvUntil(msgReadyForQuery)
}

func findFrame(frames []frame, typ byte) *frame {
	for i := range frames {
		if frames[i].typ == typ {
			return &frames[i]
		}
	}
	return nil
}

func findTag(frames []frame) string {
	f := findFrame(frames, msgCommandComplete)
	if f == nil {
		return ""
	}
	s, _, err := consumeCString(f.val)
	if err != nil {
		return ""
	}
	return s
}

// findError extracts the SQLSTATE and message from an ErrorResponse.
func findError(frames []frame) (code, msg string, ok bool) {
	f := findFrame(frames, msgErrorResponse)
	if f == nil {
		return "", "", false
	}
	b := f.val
	for len(b) > 0 && b[0] != 0 {
		kind := b[0]
		s, rest, err := consumeCString(b[1:])
		if err != nil {
			return "", "", false
		}
		switch kind {
		case 'C':
			code = s
		case 'M':
			msg = s
		}
		b = rest
	}
	return code, msg, true
}

// ---- TLS + RBAC fixtures ----

func writeSelfSignedCert(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "tellstone-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyBytes := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, certBytes, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, keyBytes, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath
}

type srvOpts struct {
	requirePass string
	policy      *rbac.Store
	withTLS     bool
}

func newTestServer(t *testing.T, o srvOpts) (*Server, *testStore) {
	t.Helper()
	store := newTestStore()
	var passHash []byte
	if o.requirePass != "" && o.policy == nil {
		var err error
		passHash, err = bcrypt.GenerateFromPassword([]byte(o.requirePass), bcrypt.DefaultCost)
		if err != nil {
			t.Fatalf("bcrypt: %v", err)
		}
	}
	var tlsConfigs *tlslib.ConfigStore
	if o.withTLS {
		cert, key := writeSelfSignedCert(t)
		cfg, err := tlslib.BuildConfig(cert, key, "")
		if err != nil {
			t.Fatalf("build tls config: %v", err)
		}
		tlsConfigs, err = tlslib.NewConfigStore(cfg)
		if err != nil {
			t.Fatalf("new config store: %v", err)
		}
	}
	srv := NewServer("127.0.0.1:0", store, o.policy, nil, nil, passHash, tlsConfigs, testLogger())
	addr, err := srv.Start()
	if err != nil {
		t.Fatalf("start sql server: %v", err)
	}
	t.Logf("sql server on %s", addr)
	t.Cleanup(srv.Close)
	return srv, store
}

func testLogger() log.Logger {
	if os.Getenv("SQL_DEBUG_LOG") != "" {
		return logger.NewSlogLogger(logger.LevelDebug)
	}
	return log.NewNoOpLogger()
}

func newRBACPolicy(t *testing.T) *rbac.Store {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	yaml := fmt.Sprintf(`
roles:
  - name: full
    rules: ["+@all", "~*"]
  - name: readonly
    rules: ["+@read", "~cache:*"]
users:
  - name: alice
    password: %q
    role: full
  - name: bob
    password: %q
    role: readonly
default_role: readonly
`, string(hash), string(hash))
	policy, err := rbac.Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse policy: %v", err)
	}
	return rbac.NewStore(policy, log.NewNoOpLogger())
}

// ---- tests ----

func TestTrustStartupAndSimpleCRUD(t *testing.T) {
	srv, store := newTestServer(t, srvOpts{})

	cl := dialServer(t, srv.Addr())
	cl.startupTrust("alice")

	ins := cl.query(`INSERT INTO tellstone (key, value) VALUES ('foo', '\x626172')`)
	if tag := findTag(ins); tag != "INSERT 0 1" {
		t.Fatalf("INSERT tag = %q", tag)
	}

	sel := cl.query(`SELECT key, value FROM tellstone WHERE key = 'foo'`)
	rd := findFrame(sel, msgRowDescription)
	data := findFrame(sel, msgDataRow)
	if rd == nil || data == nil {
		t.Fatalf("SELECT: want RowDescription+DataRow, got types %v", frameTypes(sel))
	}
	assertRowDescription(t, rd.val, []string{"key", "value"}, []int32{oidText, oidBytea})
	row, err := parseDataRow(data.val)
	if err != nil {
		t.Fatalf("parseDataRow: %v", err)
	}
	if len(row) != 2 || string(row[0]) != "foo" || string(row[1]) != `\x626172` {
		t.Fatalf("row = %q", row)
	}
	if tag := findTag(sel); tag != "SELECT 1" {
		t.Fatalf("SELECT tag = %q", tag)
	}

	miss := cl.query(`SELECT value FROM tellstone WHERE key = 'nope'`)
	if findFrame(miss, msgDataRow) != nil {
		t.Fatal("missing key must not return a row")
	}
	if tag := findTag(miss); tag != "SELECT 0" {
		t.Fatalf("missing SELECT tag = %q", tag)
	}

	// second INSERT of the same key is a duplicate-key violation.
	dup := cl.query(`INSERT INTO tellstone (key, value) VALUES ('foo', 'x')`)
	code, _, ok := findError(dup)
	if !ok || code != errDuplicateKey {
		t.Fatalf("dup INSERT: want 23505, got code=%q found=%v", code, ok)
	}

	// OnConflict opts into the upsert.
	oc := cl.query(`INSERT INTO tellstone (key, value) VALUES ('foo', '\x7a7a') ON CONFLICT (key) DO UPDATE SET value = excluded.value`)
	if tag := findTag(oc); tag != "INSERT 0 1" {
		t.Fatalf("ON CONFLICT tag = %q", tag)
	}
	after := cl.query(`SELECT value FROM tellstone WHERE key = 'foo'`)
	data = findFrame(after, msgDataRow)
	row, _ = parseDataRow(data.val)
	if string(row[0]) != `\x7a7a` {
		t.Fatalf("after upsert value = %q", row[0])
	}

	// UPDATE existing key updates, missing key reports 0.
	if tag := findTag(cl.query(`UPDATE tellstone SET value = 'bar' WHERE key = 'foo'`)); tag != "UPDATE 1" {
		t.Fatalf("UPDATE tag = %q", tag)
	}
	if tag := findTag(cl.query(`UPDATE tellstone SET value = 'x' WHERE key = 'nope'`)); tag != "UPDATE 0" {
		t.Fatalf("UPDATE-missing tag = %q", tag)
	}

	// DELETE both cases.
	if tag := findTag(cl.query(`DELETE FROM tellstone WHERE key = 'foo'`)); tag != "DELETE 1" {
		t.Fatalf("DELETE tag = %q", tag)
	}
	if tag := findTag(cl.query(`DELETE FROM tellstone WHERE key = 'foo'`)); tag != "DELETE 0" {
		t.Fatalf("DELETE-missing tag = %q", tag)
	}
	store.mu.Lock()
	_, left := store.data["foo"]
	store.mu.Unlock()
	if left {
		t.Fatal("key must be gone after DELETE")
	}
}

func frameTypes(fs []frame) string {
	var b strings.Builder
	for _, f := range fs {
		b.WriteByte(f.typ)
	}
	return b.String()
}

func assertRowDescription(t *testing.T, payload []byte, names []string, oids []int32) {
	t.Helper()
	if len(payload) < 2 {
		t.Fatalf("RowDescription too short: %d bytes", len(payload))
	}
	n := int(binary.BigEndian.Uint16(payload[:2]))
	if n != len(names) {
		t.Fatalf("RowDescription has %d cols, want %d", n, len(names))
	}
	b := payload[2:]
	for i := 0; i < n; i++ {
		name, rest, err := consumeCString(b)
		if err != nil {
			t.Fatalf("col %d name: %v", i, err)
		}
		if name != names[i] {
			t.Fatalf("col %d name = %q, want %q", i, name, names[i])
		}
		if len(rest) < 18 {
			t.Fatalf("col %d metadata truncated", i)
		}
		objOID := int32(binary.BigEndian.Uint32(rest[6:10]))
		if objOID != oids[i] {
			t.Fatalf("col %d type OID = %d, want %d", i, objOID, oids[i])
		}
		b = rest[18:]
	}
}

// parseDataRow decodes one frame into cells (nil = NULL), assuming text format.
func parseDataRow(payload []byte) ([][]byte, error) {
	if len(payload) < 2 {
		return nil, fmt.Errorf("DataRow too short")
	}
	n := int(binary.BigEndian.Uint16(payload[:2]))
	cols := make([][]byte, n)
	b := payload[2:]
	for i := 0; i < n; i++ {
		if len(b) < 4 {
			return nil, fmt.Errorf("col %d length truncated", i)
		}
		l := int(int32(binary.BigEndian.Uint32(b[:4])))
		b = b[4:]
		if l < 0 {
			cols[i] = nil
			continue
		}
		if l > len(b) {
			return nil, fmt.Errorf("col %d payload truncated", i)
		}
		cols[i] = append([]byte(nil), b[:l]...)
		b = b[l:]
	}
	return cols, nil
}

func TestSimpleErrors(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})

	sess := dialServer(t, srv.Addr())
	sess.startupTrust("default")

	cases := []struct {
		q    string
		code string
	}{
		{`DROP TABLE tellstone`, errSyntax},
		{`SELECT * FROM other_table`, errUndefinedTable},
		{`SELECT value FROM tellstone`, errSyntax},
		{`INSERT INTO tellstone (key, value) VALUES ('a')`, errSyntax},
		{`SELECT count(*) FROM tellstone WHERE key = 'a'`, errSyntax},
		{`BEGIN; COMMIT`, errSyntax},
	}
	for _, tc := range cases {
		frames := sess.query(tc.q)
		code, _, ok := findError(frames)
		if !ok {
			t.Fatalf("%q: expected an error frame, got %q (%s)", tc.q, frameTypes(frames), findTag(frames))
		}
		if code != tc.code {
			t.Fatalf("%q: code = %s, want %s", tc.q, code, tc.code)
		}
	}
}

func TestTransactions(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})

	sess := dialServer(t, srv.Addr())
	sess.startupTrust("default")

	frames := sess.query(`BEGIN`)
	if tag := findTag(frames); tag != "BEGIN" {
		t.Fatalf("BEGIN tag = %q", tag)
	}
	if f := findFrame(frames, msgReadyForQuery); f != nil && len(f.val) > 0 && f.val[0] != 'T' {
		t.Fatalf("BEGIN must leave tx state 'T', got %q", f.val[0])
	}
	if tag := findTag(sess.query(`INSERT INTO tellstone (key, value) VALUES ('tx', 'ok')`)); tag != "INSERT 0 1" {
		t.Fatalf("insert in txn tag = %q", tag)
	}
	frames = sess.query(`ROLLBACK`)
	if tag := findTag(frames); tag != "ROLLBACK" {
		t.Fatalf("ROLLBACK tag = %q", tag)
	}
	if f := findFrame(frames, msgReadyForQuery); f != nil && f.val[0] != 'I' {
		t.Fatalf("ROLLBACK must leave tx state 'I', got %q", f.val[0])
	}
	frames = sess.query(`BEGIN`)
	sess.query(`INSERT INTO tellstone (key, value) VALUES ('tx', 'ok')`)
	frames = sess.query(`COMMIT`)
	if tag := findTag(frames); tag != "COMMIT" {
		t.Fatalf("COMMIT tag = %q", tag)
	}
	if tag := findTag(sess.query(`DELETE FROM tellstone WHERE key = 'tx'`)); tag != "DELETE 1" {
		t.Fatalf("committed row missing: tag = %q", tag)
	}
}

func TestExtendedProtocol(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})

	sess := dialServer(t, srv.Addr())
	sess.startupTrust("default")

	// DELETE with a parameter, so a prior run never leaves residue.
	sess.send(msgParse, concat(cstring("del"), cstring(`DELETE FROM tellstone WHERE key = $1`), two(0)))
	sess.expectLine(msgParseComplete)
	sess.send(msgBind, concat(cstring(""), cstring("del"), two(0), two(1), four(6), []byte("extkey"), two(0)))
	sess.expectLine(msgBindComplete)
	sess.send(msgExecute, concat(cstring(""), four(0)))
	sess.expectLine(msgCommandComplete)
	sess.send(msgSync, nil)
	sess.expectLine(msgReadyForQuery)

	// Prepare a parameterized SELECT, describe it, bind and execute it.
	sess.send(msgParse, concat(cstring(""), cstring(`SELECT key, value FROM tellstone WHERE key = $1`), two(0)))
	sess.expectLine(msgParseComplete)
	sess.send(msgDescribe, concat([]byte{'S'}, cstring("")))
	pd := sess.expectLine(msgParameterDesc)
	assertParameterDescription(t, pd.val, []int32{oidText})
	rd := sess.expectLine(msgRowDescription)
	assertRowDescription(t, rd.val, []string{"key", "value"}, []int32{oidText, oidBytea})
	sess.send(msgBind, concat(cstring(""), cstring(""), two(0), two(1), four(6), []byte("extkey"), two(0)))
	sess.expectLine(msgBindComplete)
	sess.send(msgExecute, concat(cstring(""), four(0)))
	// The portal was not described at portal level, so Execute re-sends
	// RowDescription before reporting the empty result.
	er := sess.expectLine(msgRowDescription)
	assertRowDescription(t, er.val, []string{"key", "value"}, []int32{oidText, oidBytea})
	ec := sess.expectLine(msgCommandComplete)
	tag, _, _ := consumeCString(ec.val)
	if tag != "SELECT 0" {
		t.Fatalf("extended SELECT tag = %q", tag)
	}
	sess.send(msgSync, nil)
	sess.expectLine(msgReadyForQuery)

	// Close a statement and prove it is gone.
	sess.send(msgClose, concat([]byte{'S'}, cstring("")))
	sess.expectLine(msgCloseComplete)
	sess.send(msgDescribe, concat([]byte{'S'}, cstring("")))
	code, _, ok := findError([]frame{sess.recv()})
	if !ok || code != errInvalidSQLStatement {
		t.Fatalf("closed statement: code=%q ok=%v", code, ok)
	}
	sess.send(msgSync, nil)
	sess.expectLine(msgReadyForQuery)
}

func assertParameterDescription(t *testing.T, payload []byte, oids []int32) {
	t.Helper()
	if len(payload) < 2 {
		t.Fatalf("ParameterDescription too short: %d", len(payload))
	}
	n := int(binary.BigEndian.Uint16(payload[:2]))
	if n != len(oids) {
		t.Fatalf("ParameterDescription has %d params, want %d", n, len(oids))
	}
	b := payload[2:]
	for i, want := range oids {
		if len(b) < 4 {
			t.Fatalf("param %d OID truncated", i)
		}
		got := int32(binary.BigEndian.Uint32(b[:4]))
		if got != want {
			t.Fatalf("param %d OID = %d, want %d", i, got, want)
		}
		b = b[4:]
	}
}

func two(v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return b
}

func four(v int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return b
}

func cstring(s string) []byte { return append([]byte(s), 0) }

func concat(parts ...[]byte) []byte {
	n := 0
	for _, p := range parts {
		n += len(p)
	}
	out := make([]byte, 0, n)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func TestExtendedInsertAndBinaryParam(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})

	sess := dialServer(t, srv.Addr())
	sess.startupTrust("default")

	// Text-format params: value given in bytea hex form.
	sess.send(msgParse, concat(cstring(""), cstring(`INSERT INTO tellstone (key, value) VALUES ($1, $2)`), two(0)))
	sess.expectLine(msgParseComplete)
	sess.send(msgBind, concat(cstring(""), cstring(""), two(0), two(2),
		four(2), []byte("k1"), four(6), []byte(`\x6869`), two(0))) // "hi"
	sess.expectLine(msgBindComplete)
	sess.send(msgExecute, concat(cstring(""), four(0)))
	sess.expectLine(msgCommandComplete)
	sess.send(msgSync, nil)
	sess.expectLine(msgReadyForQuery)

	// Binary-format param bytes pass through untouched: raw DEAD stored as-is.
	sess.send(msgParse, concat(cstring(""), cstring(`INSERT INTO tellstone (key, value) VALUES ($1, $2)`), two(0)))
	sess.expectLine(msgParseComplete)
	sess.send(msgBind, concat(cstring(""), cstring(""), two(1), two(1), two(2), four(2), []byte("k2"), four(2), []byte{0xde, 0xad}, two(0)))
	sess.expectLine(msgBindComplete)
	sess.send(msgExecute, concat(cstring(""), four(0)))
	sess.expectLine(msgCommandComplete)
	sess.send(msgSync, nil)
	sess.expectLine(msgReadyForQuery)

	// Both round-trip in hex form.
	sel := sess.query(`SELECT key, value FROM tellstone WHERE key = 'k1'`)
	data := findFrame(sel, msgDataRow)
	row, err := parseDataRow(data.val)
	if err != nil {
		t.Fatalf("parseDataRow: %v", err)
	}
	if string(row[1]) != `\x6869` {
		t.Fatalf("k1 value = %q, want \\x6869", row[1])
	}
	sel = sess.query(`SELECT value FROM tellstone WHERE key = 'k2'`)
	data = findFrame(sel, msgDataRow)
	row, _ = parseDataRow(data.val)
	if !strings.EqualFold(string(row[0]), `\xdead`) {
		t.Fatalf("k2 value = %q, want \\xdead (hex.Eq %v)", row[0], hex.EncodeToString([]byte{0xde, 0xad}))
	}
}

func TestExtendedErrorRecovery(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})

	sess := dialServer(t, srv.Addr())
	sess.startupTrust("default")

	// Describe a portal that does not exist: error frame, then Sync restores.
	sess.send(msgDescribe, concat([]byte{'P'}, cstring("missing")))
	code, msg, ok := findError([]frame{sess.recv()})
	if !ok || code != errInvalidCursorName {
		t.Fatalf("unknown portal: code=%q msg=%q ok=%v", code, msg, ok)
	}
	// While inError, further messages are ignored until Sync.
	sess.send(msgParse, concat(cstring(""), cstring(`SELECT key FROM tellstone WHERE key = 'x'`), two(0)))
	sess.send(msgSync, nil)
	f := sess.recvUntil(msgReadyForQuery)
	if len(f) != 1 {
		t.Fatalf("expected only ReadyForQuery after Sync, got %q", frameTypes(f))
	}
	// The ignored Parse must not have taken effect.
	sess.send(msgDescribe, concat([]byte{'S'}, cstring("")))
	code, _, _ = findError([]frame{sess.recv()})
	if code != errInvalidSQLStatement {
		t.Fatalf("after recovery, unnamed statement vanished: code=%q", code)
	}
	sess.send(msgSync, nil)
	sess.recvUntil(msgReadyForQuery)
}

func TestSSLNegotiation(t *testing.T) {
	// No TLS configured: SSLRequest answers 'N'.
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.sendStartup(startupBody(sslRequestCode))
	resp, err := cl.r.ReadByte()
	if err != nil {
		t.Fatalf("read SSLRequest reply: %v", err)
	}
	if resp != 'N' {
		t.Fatalf("expected 'N', got %q", resp)
	}
	// Handshake continues as plaintext; the server is torn down with the conn.
	srv.Close()
}

func TestCleartextRequiresTLS(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{requirePass: "hunter2"})

	cl := dialServer(t, srv.Addr())
	cl.sendStartup(startupBody(protocolVersionNumber, "user", "default"))
	f := cl.recv()
	if f.typ != msgErrorResponse {
		t.Fatalf("expected ErrorResponse, got %q", f.typ)
	}
	code, msg, ok := findError([]frame{f})
	if !ok || code != errProtocolViolation {
		t.Fatalf("gate: code=%q msg=%q ok=%v", code, msg, ok)
	}
	if !strings.Contains(msg, "TLS") {
		t.Fatalf("gate message should mention TLS, got %q", msg)
	}
	if _, err := cl.r.ReadByte(); err == nil {
		t.Fatal("connection must close after FATAL")
	}
}

func TestPasswordModeOverTLS(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{requirePass: "hunter2", withTLS: true})

	cl := dialServer(t, srv.Addr())
	cl.startupPassword("default", "hunter2")

	if tag := findTag(cl.query(`INSERT INTO tellstone (key, value) VALUES ('pw', 'ok')`)); tag != "INSERT 0 1" {
		t.Fatalf("insert tag = %q", tag)
	}
	// Wrong password over TLS: server answers with an unhelpful generic failure.
	cl2 := dialServer(t, srv.Addr())
	cl2.startTLS()
	cl2.sendStartup(startupBody(protocolVersionNumber, "user", "default"))
	cl2.expectLine(msgAuthentication) // CleartextPassword challenge
	cl2.send(msgPassword, concat([]byte("wrong"), []byte{0}))
	code, _, ok := findError([]frame{cl2.recv()})
	if !ok || code != errInvalidPassword {
		t.Fatalf("wrong password: code=%q ok=%v", code, ok)
	}
}

func TestRBACOverTLS(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{policy: newRBACPolicy(t), withTLS: true})

	alice := dialServer(t, srv.Addr())
	alice.startupPassword("alice", "secret")
	if tag := findTag(alice.query(`INSERT INTO tellstone (key, value) VALUES ('cache:uno', 'x')`)); tag != "INSERT 0 1" {
		t.Fatalf("alice insert tag = %q", tag)
	}
	if tag := findTag(alice.query(`SELECT key FROM tellstone WHERE key = 'cache:uno'`)); tag != "SELECT 1" {
		t.Fatalf("alice select tag = %q", tag)
	}

	// bob may read cache:* but not write, and may not access other prefixes.
	bob := dialServer(t, srv.Addr())
	bob.startupPassword("bob", "secret")
	if tag := findTag(bob.query(`SELECT value FROM tellstone WHERE key = 'cache:uno'`)); tag != "SELECT 1" {
		t.Fatalf("bob cache select tag = %q", tag)
	}
	frames := bob.query(`SELECT key FROM tellstone WHERE key = 'other'`)
	code, msg, ok := findError(frames)
	if !ok || code != errInsufficientPrivilege {
		t.Fatalf("bob out-of-prefix select: code=%q msg=%q ok=%v", code, msg, ok)
	}
	frames = bob.query(`INSERT INTO tellstone (key, value) VALUES ('cache:dos', 'x')`)
	code, _, ok = findError(frames)
	if !ok || code != errInsufficientPrivilege {
		t.Fatalf("bob insert: code=%q ok=%v", code, ok)
	}

	// Unknown user: generic per-user failure.
	evil := dialServer(t, srv.Addr())
	evil.startTLS()
	evil.sendStartup(startupBody(protocolVersionNumber, "user", "mallory"))
	_ = evil.expectLine(msgAuthentication)
	evil.send(msgPassword, concat([]byte("secret"), []byte{0}))
	code, _, ok = findError([]frame{evil.recv()})
	if !ok || code != errInvalidPassword {
		t.Fatalf("unknown user: code=%q ok=%v", code, ok)
	}
}
