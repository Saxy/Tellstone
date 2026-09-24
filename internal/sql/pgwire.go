package sql

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Saxy/Tellstone/internal/audit"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/oauth"
	"github.com/Saxy/Tellstone/internal/rbac"
	tlslib "github.com/Saxy/Tellstone/internal/tls"
)

// Protocol constants (frontend/backend protocol 3.0).
const (
	protocolVersionNumber = 196608 // 3.0
	sslRequestCode        = 80877103
	gssEncRequestCode     = 80877104
	cancelRequestCode     = 80877102
)

// Frontend message type codes.
const (
	msgParse        = 'P'
	msgBind         = 'B'
	msgDescribe     = 'D'
	msgExecute      = 'E'
	msgSync         = 'S'
	msgFlush        = 'H'
	msgClose        = 'C'
	msgPassword     = 'p'
	msgTerminate    = 'X'
	msgQuery        = 'Q'
	msgCopyData     = 'd'
	msgCopyDone     = 'c'
	msgCopyFail     = 'f'
	msgFunctionCall = 'F'
)

// Backend message type codes.
const (
	msgAuthentication  = 'R'
	msgBackendKeyData  = 'K'
	msgBindComplete    = '2'
	msgCloseComplete   = '3'
	msgCommandComplete = 'C'
	msgDataRow         = 'D'
	msgEmptyQuery      = 'I'
	msgErrorResponse   = 'E'
	msgNoData          = 'n'
	msgParameterDesc   = 't'
	msgParameterStatus = 'S'
	msgParseComplete   = '1'
	msgReadyForQuery   = 'Z'
	msgRowDescription  = 'T'
	msgNotice          = 'N'
)

// Authentication request codes.
const (
	authOk                = 0
	authCleartextPassword = 3
	authMD5Password       = 5
)

// maxFrameSize caps a single protocol frame. Idle-service reads after it are
// rejected rather than buffered.
const maxFrameSize = 64 << 20

// resultFormatText is the only result/parameter format Phase 8 speaks: column
// data rides as text (bytea hex-encoded), mirroring libpq's default.
const resultFormatText = 0

var errFrameTooLarge = errors.New("sql: frame exceeds size limit")

// paramVal is a bound parameter: raw value plus its wire format code.
type paramVal struct {
	val    []byte
	null   bool
	format int16
}

type pgConn struct {
	srv        *Server
	raw        net.Conn
	tlsConn    net.Conn // non-nil after an SSLRequest handshake
	r          *bufio.Reader
	w          *bufio.Writer
	remoteAddr string
	user       string

	auth *authState

	stmts   map[string]*Plan        // prepared statements by name
	portals map[string]*portalState // bound portals by name
	inTxn   bool
	// inError suspends extended-protocol processing until the client Syncs,
	// which is how the protocol recovers from an error mid-transaction.
	inError bool
}

type portalState struct {
	plan    *Plan
	params  []paramVal
	rowDesc bool // RowDescription already emitted (Describe or a prior Execute)
}

// Server is the PostgreSQL wire listener. One goroutine per connection; every
// data statement executes against the shared Store, so storage failures
// surface as PostgreSQL errors instead of being swallowed.
type Server struct {
	addr            string
	store           Store
	logger          log.Logger
	tlsConfigs      *tlslib.ConfigStore
	policy          *rbac.Store
	oauth           oauth.Provider
	requirePassHash []byte
	audit           *audit.LogEngine

	ln      net.Listener
	closing atomic.Bool
	wg      sync.WaitGroup
	connsMu sync.Mutex
	conns   map[*pgConn]struct{}

	backendSecret atomic.Int32
}

// NewServer wires the PG listener to the shared store and identity stack.
// requirePassHash may be nil; policy/oauth may be nil (trust or single-password
// modes). tlsConfigs may be nil (no TLS advertised).
func NewServer(addr string, store Store, policy *rbac.Store, oauthProvider oauth.Provider, auditEngine *audit.LogEngine, requirePassHash []byte, tlsConfigs *tlslib.ConfigStore, logger log.Logger) *Server {
	return &Server{
		addr:            addr,
		store:           store,
		logger:          logger,
		tlsConfigs:      tlsConfigs,
		policy:          policy,
		oauth:           oauthProvider,
		requirePassHash: requirePassHash,
		audit:           auditEngine,
		conns:           map[*pgConn]struct{}{},
	}
}

// Addr returns the bound listener address ("127.0.0.1:port" when the port was 0).
func (s *Server) Addr() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// Start binds the listener and spawns the accept loop. It returns the resolved
// address, which differs from addr only when the port was 0.
func (s *Server) Start() (string, error) {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return "", err
	}
	s.ln = ln
	s.wg.Add(1)
	go s.acceptLoop()
	return ln.Addr().String(), nil
}

// Close stops accepting new connections and interrupts every active one. It is
// idempotent and safe to call while accept is in flight.
func (s *Server) Close() {
	if s.closing.Swap(true) {
		return
	}
	if s.ln != nil {
		_ = s.ln.Close()
	}
	s.connsMu.Lock()
	for c := range s.conns {
		_ = c.raw.Close()
	}
	s.connsMu.Unlock()
	s.wg.Wait()
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if s.closing.Load() || errors.Is(err, net.ErrClosed) {
				return
			}
			if s.logger.Enabled(log.LevelError) {
				s.logger.Log(log.LevelError, "sql: accept error", log.String("error", err.Error()))
			}
			continue
		}
		s.wg.Add(1)
		go s.serveConn(conn)
	}
}

func (s *Server) serveConn(raw net.Conn) {
	defer s.wg.Done()
	c := &pgConn{
		srv:        s,
		raw:        raw,
		remoteAddr: raw.RemoteAddr().String(),
		r:          bufio.NewReaderSize(raw, 32<<10),
		w:          bufio.NewWriterSize(raw, 32<<10),
		stmts:      map[string]*Plan{},
		portals:    map[string]*portalState{},
	}
	defer raw.Close()
	s.connsMu.Lock()
	s.conns[c] = struct{}{}
	s.connsMu.Unlock()
	defer func() {
		s.connsMu.Lock()
		delete(s.conns, c)
		s.connsMu.Unlock()
	}()

	if err := c.startup(); err != nil {
		if pe, ok := err.(*pgError); ok {
			_ = s.sendError(c, pe, "FATAL")
		}
		return
	}
	if err := c.messageLoop(); err != nil {
		return
	}
}

// startup handles the first frame(s): SSLRequest / GSSENC negotiation, then
// the StartupMessage and the auth exchange. On success it emits the post-auth
// parameter set and BackendKeyData.
func (c *pgConn) startup() error {
	s := c.srv
	for {
		body, err := readStartupFrame(c.r)
		if err != nil {
			return err
		}
		if len(body) < 4 {
			return &pgError{code: errProtocolViolation, msg: "truncated startup frame"}
		}
		code := int32(binary.BigEndian.Uint32(body[:4]))
		switch code {
		case sslRequestCode:
			if cfg := s.loadTLSConfig(); cfg != nil {
				if err := c.writeRaw([]byte{'S'}); err != nil {
					return err
				}
				tc := tlslib.Server(c.raw, cfg)
				if err := tc.Handshake(); err != nil {
					return err
				}
				c.tlsConn = tc
				c.r = bufio.NewReaderSize(c.tlsConn, 32<<10)
				c.w = bufio.NewWriterSize(c.tlsConn, 32<<10)
			} else {
				if err := c.writeRaw([]byte{'N'}); err != nil {
					return err
				}
			}
		case gssEncRequestCode:
			if err := c.writeRaw([]byte{'N'}); err != nil {
				return err
			}
		case cancelRequestCode:
			// The protocol requires no response: the closed socket signals the
			// (possibly unanswered) cancellation.
			return io.EOF
		case protocolVersionNumber:
			params, err := parseStartupParams(body[4:])
			if err != nil {
				return &pgError{code: errProtocolViolation, msg: err.Error()}
			}
			return c.authorize(params)
		default:
			return &pgError{code: errProtocolViolation, msg: fmt.Sprintf("unsupported protocol version %d", code)}
		}
	}
}

func (s *Server) loadTLSConfig() *tlslib.Config {
	if s.tlsConfigs == nil {
		return nil
	}
	return s.tlsConfigs.Load()
}

func (c *pgConn) writeRaw(b []byte) error {
	if _, err := c.w.Write(b); err != nil {
		return err
	}
	return c.w.Flush()
}

// authorize runs the auth exchange and, on success, writes the post-auth
// messages. It returns a protocol error, or nil to proceed to the loop.
func (c *pgConn) authorize(params map[string]string) error {
	s := c.srv
	auth, perr := s.authenticate(c, params)
	if perr != nil {
		return perr
	}
	c.auth = auth
	if c.user == "" {
		c.user = auth.user
	}
	// Each frame is encoded into a reset builder and flushed separately; a
	// shared builder would smear cumulative lengths into the wire.
	var buf msgBuilder
	buf.begin(msgAuthentication)
	buf.int32(authOk)
	buf.end()
	if err := c.writeMsg(&buf); err != nil {
		return &pgError{code: errIoError, msg: err.Error()}
	}
	for _, kv := range [][2]string{
		{"server_version", "14.10"},
		{"client_encoding", "UTF8"},
		{"standard_conforming_strings", "on"},
		{"integer_datetimes", "on"},
	} {
		buf.reset()
		buf.begin(msgParameterStatus)
		buf.cstring(kv[0])
		buf.cstring(kv[1])
		buf.end()
		if err := c.writeMsg(&buf); err != nil {
			return &pgError{code: errIoError, msg: err.Error()}
		}
	}
	buf.reset()
	buf.begin(msgBackendKeyData)
	buf.int32(s.backendPID())
	buf.int32(s.backendSecret.Load())
	buf.end()
	if err := c.writeMsg(&buf); err != nil {
		return &pgError{code: errIoError, msg: err.Error()}
	}
	if s.audit != nil {
		s.audit.Record(audit.EventAuthSuccess, "client authenticated",
			log.String("user", auth.user),
			log.String("remote_addr", c.remoteAddr),
			log.String("protocol", "sql"),
		)
	}
	// The startup sequence ends with ReadyForQuery, exactly like a real backend.
	return c.ready()
}

// backendPID returns the BackendKeyData PID exposed to clients.
func (s *Server) backendPID() int32 {
	return 4242
}

// parseStartupParams decodes the key/value section that follows the protocol
// version int32. Application name and options carry no semantics here.
func parseStartupParams(b []byte) (map[string]string, error) {
	params := make(map[string]string, 8)
	for len(b) > 0 {
		key, rest, err := consumeCString(b)
		if err != nil {
			return nil, fmt.Errorf("malformed startup params: %w", err)
		}
		if key == "" {
			return params, nil // trailing zero on an empty key terminates
		}
		val, rest2, err := consumeCString(rest)
		if err != nil {
			return nil, fmt.Errorf("malformed startup params: %w", err)
		}
		params[key] = val
		b = rest2
	}
	return params, nil
}

// messageLoop dispatches frontend messages. Extended-protocol errors put the
// connection in the Sync-wait state; Sync alone restores normal processing.
func (c *pgConn) messageLoop() error {
	for {
		if c.inError {
			t, _, err := readFrame(c.r)
			if err != nil {
				return normalizeIO(err)
			}
			switch t {
			case msgSync:
				c.inError = false
				c.resetUnnamed()
				if err := c.ready(); err != nil {
					return err
				}
			case msgTerminate:
				return nil
			}
			continue
		}
		t, p, err := readFrame(c.r)
		if err != nil {
			return normalizeIO(err)
		}
		switch t {
		case msgTerminate:
			return nil
		case msgQuery:
			if err = c.handleQuery(p); err != nil {
				return err
			}
		case msgParse:
			if te := c.execStep(func() error { return c.handleParse(p) }); te != nil {
				return te
			}
		case msgBind:
			if te := c.execStep(func() error { return c.handleBind(p) }); te != nil {
				return te
			}
		case msgDescribe:
			if te := c.execStep(func() error { return c.handleDescribe(p) }); te != nil {
				return te
			}
		case msgExecute:
			if te := c.execStep(func() error { return c.handleExecute(p) }); te != nil {
				return te
			}
		case msgClose:
			if te := c.execStep(func() error { return c.handleClose(p) }); te != nil {
				return te
			}
		case msgSync:
			c.resetUnnamed()
			if err = c.ready(); err != nil {
				return err
			}
		case msgFlush:
			if err = c.w.Flush(); err != nil {
				return normalizeIO(err)
			}
		case msgFunctionCall, msgCopyData, msgCopyDone, msgCopyFail:
			// Declared but unsupported: fail loudly rather than stall a client.
			return c.sendExtError(&pgError{code: errFeatureNotSupported, msg: fmt.Sprintf("frontend message %q is not supported", t)})
		default:
			return c.sendExtError(&pgError{code: errProtocolViolation, msg: fmt.Sprintf("unrecognized frontend message %d", t)})
		}
	}
}

// execStep runs one extended-protocol handler. Statement errors become
// ErrorResponse + Sync-wait state (returning nil so the loop continues to the
// inError branch); only transport errors propagate up and close the session.
func (c *pgConn) execStep(f func() error) error {
	err := f()
	if err == nil {
		return nil
	}
	return c.extendedError(err)
}

// extendedError reports an error through the extended protocol's recovery: the
// client must Sync before normal processing resumes.
func (c *pgConn) extendedError(err error) error {
	if c.inError {
		return err
	}
	c.inError = true
	c.resetUnnamed()
	return c.sendExtError(err)
}

func (c *pgConn) sendExtError(err error) error {
	pe := asPGError(err)
	if err = c.srv.sendError(c, pe, "ERROR"); err != nil {
		return err
	}
	return c.w.Flush()
}

func (c *pgConn) resetUnnamed() {
	if _, ok := c.stmts[""]; ok {
		delete(c.stmts, "")
	}
	if _, ok := c.portals[""]; ok {
		delete(c.portals, "")
	}
}

func (c *pgConn) ready() error {
	var buf msgBuilder
	buf.begin(msgReadyForQuery)
	buf.byte(txnStatus(c.inTxn))
	buf.end()
	return c.writeMsg(&buf)
}

func txnStatus(inTxn bool) byte {
	if inTxn {
		return 'T'
	}
	return 'I'
}

// ---- message handlers ----

func (c *pgConn) handleQuery(payload []byte) error {
	query, _, err := consumeCString(payload)
	if err != nil {
		return &pgError{code: errProtocolViolation, msg: "malformed query message"}
	}
	if strings.TrimSpace(query) == "" {
		var buf msgBuilder
		buf.begin(msgEmptyQuery)
		buf.end()
		if err := c.writeMsg(&buf); err != nil {
			return err
		}
		return c.ready()
	}
	plan, err := Translate(query)
	if err != nil {
		if perr := c.srv.sendError(c, errOrSyntax(err), "ERROR"); perr != nil {
			return perr
		}
		return c.ready()
	}
	out, xerr := c.srv.execute(c, plan, nil)
	if xerr != nil {
		if perr := c.srv.sendError(c, asPGError(xerr), "ERROR"); perr != nil {
			return perr
		}
		return c.ready()
	}
	if out.selectRows {
		if err = c.sendRowDescription(plan); err != nil {
			return err
		}
	}
	if err = c.emitOutcome(out); err != nil {
		return err
	}
	return c.ready()
}

func (c *pgConn) handleParse(payload []byte) error {
	stmtName, rest, err := consumeCString(payload)
	if err != nil {
		return &pgError{code: errProtocolViolation, msg: "malformed Parse message"}
	}
	query, rest, err := consumeCString(rest)
	if err != nil {
		return &pgError{code: errProtocolViolation, msg: "malformed Parse message"}
	}
	if len(rest) < 2 {
		return &pgError{code: errProtocolViolation, msg: "malformed Parse message"}
	}
	plan, err := Translate(query)
	if err != nil {
		return errOrSyntax(err)
	}
	c.stmts[stmtName] = plan
	var buf msgBuilder
	buf.begin(msgParseComplete)
	buf.end()
	return c.writeMsg(&buf)
}

func (c *pgConn) handleBind(payload []byte) error {
	portalName, rest, err := consumeCString(payload)
	if err != nil {
		return &pgError{code: errProtocolViolation, msg: "malformed Bind message"}
	}
	stmtName, rest, err := consumeCString(rest)
	if err != nil {
		return &pgError{code: errProtocolViolation, msg: "malformed Bind message"}
	}
	plan, ok := c.stmts[stmtName]
	if !ok {
		return &pgError{code: errInvalidSQLStatement, msg: fmt.Sprintf("prepared statement %q does not exist", stmtName)}
	}
	if len(rest) < 2 {
		return &pgError{code: errProtocolViolation, msg: "malformed Bind message"}
	}
	nf := int(binary.BigEndian.Uint16(rest[:2]))
	rest = rest[2:]
	formats := make([]int16, 0, nf)
	for i := 0; i < nf; i++ {
		if len(rest) < 2 {
			return &pgError{code: errProtocolViolation, msg: "malformed Bind message"}
		}
		formats = append(formats, int16(binary.BigEndian.Uint16(rest[:2])))
		rest = rest[2:]
	}
	paramFormat := func(i int) int16 {
		switch {
		case len(formats) == 0:
			return resultFormatText
		case len(formats) == 1:
			return formats[0]
		case i < len(formats):
			return formats[i]
		default:
			return resultFormatText
		}
	}
	if len(rest) < 2 {
		return &pgError{code: errProtocolViolation, msg: "malformed Bind message"}
	}
	nparams := int(binary.BigEndian.Uint16(rest[:2]))
	rest = rest[2:]
	params := make([]paramVal, nparams)
	for i := 0; i < nparams; i++ {
		if len(rest) < 4 {
			return &pgError{code: errProtocolViolation, msg: "malformed Bind message"}
		}
		n := int(int32(binary.BigEndian.Uint32(rest[:4])))
		rest = rest[4:]
		params[i].format = paramFormat(i)
		if n < 0 {
			params[i].null = true
			continue
		}
		if n > len(rest) {
			return &pgError{code: errProtocolViolation, msg: "malformed Bind message"}
		}
		params[i].val = append([]byte(nil), rest[:n]...)
		rest = rest[n:]
	}
	// Result column formats: accepted, only text is honored.
	if len(rest) < 2 {
		return &pgError{code: errProtocolViolation, msg: "malformed Bind message"}
	}
	c.portals[portalName] = &portalState{plan: plan, params: params}
	var buf msgBuilder
	buf.begin(msgBindComplete)
	buf.end()
	return c.writeMsg(&buf)
}

func (c *pgConn) handleDescribe(payload []byte) error {
	if len(payload) < 1 {
		return &pgError{code: errProtocolViolation, msg: "malformed Describe message"}
	}
	kind := payload[0]
	name, _, err := consumeCString(payload[1:])
	if err != nil {
		return &pgError{code: errProtocolViolation, msg: "malformed Describe message"}
	}
	switch kind {
	case 'S':
		plan, ok := c.stmts[name]
		if !ok {
			return &pgError{code: errInvalidSQLStatement, msg: fmt.Sprintf("prepared statement %q does not exist", name)}
		}
		if perr := c.sendStatementParamDesc(plan); perr != nil {
			return perr
		} else if plan.Kind == StmtSelect {
			return c.sendRowDescription(plan)
		}
		var buf msgBuilder
		buf.begin(msgNoData)
		buf.end()
		return c.writeMsg(&buf)
	case 'P':
		portal, ok := c.portals[name]
		if !ok {
			return &pgError{code: errInvalidCursorName, msg: fmt.Sprintf("portal %q does not exist", name)}
		}
		portal.rowDesc = true
		if portal.plan.Kind == StmtSelect {
			return c.sendRowDescription(portal.plan)
		}
		var buf msgBuilder
		buf.begin(msgNoData)
		buf.end()
		return c.writeMsg(&buf)
	default:
		return &pgError{code: errProtocolViolation, msg: fmt.Sprintf("invalid Describe target %q", kind)}
	}
}

// sendStatementParamDesc emits ParameterDescription (1 per referenced
// parameter, in ascending $n order, text-typed by target column). Postgres
// only sends it when the statement references parameters, so none is emitted
// for literal-only statements.
func (c *pgConn) sendStatementParamDesc(plan *Plan) error {
	if maxParam(plan) == 0 {
		return nil
	}
	types := map[int]int32{}
	if plan.Key.Param > 0 {
		types[plan.Key.Param] = oidText
	}
	if plan.Val.Param > 0 {
		types[plan.Val.Param] = oidBytea
	}
	var buf msgBuilder
	buf.begin(msgParameterDesc)
	buf.int16(int16(len(types)))
	for n := 1; n <= maxParam(plan); n++ {
		if oid, ok := types[n]; ok {
			buf.int32(oid)
		} else {
			buf.int32(oidText)
		}
	}
	buf.end()
	return c.writeMsg(&buf)
}

func maxParam(p *Plan) int {
	m := 0
	for _, v := range []ValRef{p.Key, p.Val} {
		if v.Param > m {
			m = v.Param
		}
	}
	return m
}

func (c *pgConn) sendRowDescription(plan *Plan) error {
	var buf msgBuilder
	buf.begin(msgRowDescription)
	buf.int16(int16(len(plan.Cols)))
	for _, col := range plan.Cols {
		oid := int32(oidText)
		if col == colValue {
			oid = oidBytea
		}
		buf.cstring(col)
		buf.int32(0) // table OID (implicit table, none)
		buf.int16(0) // attribute number
		buf.int32(oid)
		buf.int16(-1) // typlen: variable
		buf.int32(-1) // typmod
		buf.int16(resultFormatText)
	}
	buf.end()
	return c.writeMsg(&buf)
}

func (c *pgConn) handleExecute(payload []byte) error {
	portalName, rest, err := consumeCString(payload)
	if err != nil {
		return &pgError{code: errProtocolViolation, msg: "malformed Execute message"}
	}
	if len(rest) < 4 {
		return &pgError{code: errProtocolViolation, msg: "malformed Execute message"}
	}
	// maxRows is honored (row-at-a-time portals suspend), but a suspended
	// portal needs the row cursor retained across Executes. Phase 8 returns the
	// full result set on the first Execute and ignores maxRows.
	portal, ok := c.portals[portalName]
	if !ok {
		return &pgError{code: errInvalidCursorName, msg: fmt.Sprintf("portal %q does not exist", portalName)}
	}
	out, xerr := c.srv.execute(c, portal.plan, portal.params)
	if xerr != nil {
		// On error the portal remains bound; only Sync destroys it.
		return xerr
	}
	if out.selectRows {
		if !portal.rowDesc {
			if err := c.sendRowDescription(portal.plan); err != nil {
				return err
			}
		}
		portal.rowDesc = true
	}
	return c.emitOutcome(out)
}

func (c *pgConn) handleClose(payload []byte) error {
	if len(payload) < 1 {
		return &pgError{code: errProtocolViolation, msg: "malformed Close message"}
	}
	kind := payload[0]
	name, _, err := consumeCString(payload[1:])
	if err != nil {
		return &pgError{code: errProtocolViolation, msg: "malformed Close message"}
	}
	switch kind {
	case 'S':
		delete(c.stmts, name)
	case 'P':
		delete(c.portals, name)
	default:
		return &pgError{code: errProtocolViolation, msg: fmt.Sprintf("invalid Close target %q", kind)}
	}
	var buf msgBuilder
	buf.begin(msgCloseComplete)
	buf.end()
	return c.writeMsg(&buf)
}

// emitOutcome writes a single DataRow (SELECT) then CommandComplete.
func (c *pgConn) emitOutcome(out *execOutcome) error {
	var buf msgBuilder
	if out.selectRows && out.row != nil {
		buf.begin(msgDataRow)
		buf.int16(int16(len(out.row)))
		for _, col := range out.row {
			if col == nil {
				buf.int32(-1)
				continue
			}
			buf.int32(int32(len(col)))
			buf.bytes(col)
		}
		buf.end()
	}
	var tbuf msgBuilder
	tbuf.begin(msgCommandComplete)
	tbuf.cstring(out.tag)
	tbuf.end()
	if err := buf.flush(c.w); err != nil {
		return err
	}
	if err := tbuf.flush(c.w); err != nil {
		return err
	}
	return c.w.Flush()
}

// ---- frame helpers ----

func readStartupFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, normalizeIO(err)
	}
	n := int(binary.BigEndian.Uint32(hdr[:]))
	// The length counts the whole frame including the length field itself, so
	// the body is n-4 bytes. A startup/cancel/SSL frame always carries its code.
	if n < 8 || n > maxFrameSize {
		return nil, errFrameTooLarge
	}
	body := make([]byte, n-4)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, normalizeIO(err)
	}
	return body, nil
}

// readFrame reads one typed backend-format frame. The returned payload
// excludes the type byte and the 4-byte length.
func readFrame(r io.Reader) (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, normalizeIO(err)
	}
	typ := hdr[0]
	n := int(binary.BigEndian.Uint32(hdr[1:])) // includes the length field
	if n < 4 || n > maxFrameSize {
		return 0, nil, errFrameTooLarge
	}
	body := make([]byte, n-4)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, normalizeIO(err)
	}
	return typ, body, nil
}

func normalizeIO(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return err
	}
	if errors.Is(err, errFrameTooLarge) {
		return err
	}
	return &pgError{code: errIoError, msg: err.Error()}
}

func asPGError(err error) *pgError {
	var pe *pgError
	if errors.As(err, &pe) {
		return pe
	}
	return &pgError{code: errIoError, msg: err.Error()}
}

// errOrSyntax maps a plain error (SQL that parses but is rejected by the
// semantics at hand) to a syntax error, preserving any explicit SQLSTATE that a
// translator already attached.
func errOrSyntax(err error) *pgError {
	var pe *pgError
	if errors.As(err, &pe) {
		return pe
	}
	return &pgError{code: errSyntax, msg: err.Error()}
}

// sendError writes an ErrorResponse with the given severity and flushes.
func (s *Server) sendError(c *pgConn, pe *pgError, severity string) error {
	var buf msgBuilder
	buf.begin(msgErrorResponse)
	buf.byte('S')
	buf.cstring(severity)
	buf.byte('C')
	buf.cstring(pe.code)
	buf.byte('M')
	buf.cstring(pe.msg)
	buf.byte(0) // message terminator: a trailing NUL after the last field
	buf.end()
	return c.writeMsg(&buf)
}

// sendMessage writes one complete frame out and flushes the write buffer.
func sendMessage(c *pgConn, typ byte, payload []byte) error {
	var b msgBuilder
	b.begin(typ)
	b.bytes(payload)
	b.end()
	return c.writeMsg(&b)
}

// writeMsg flushes a builder into the connection's write buffer and drains the
// buffer to the wire. Every logical response ends here so clients never block
// on a full bufio.Writer.
func (c *pgConn) writeMsg(m *msgBuilder) error {
	if err := m.flush(c.w); err != nil {
		return err
	}
	return c.w.Flush()
}

func frameAuthCleartext() []byte {
	var b msgBuilder
	b.int32(authCleartextPassword)
	return b.buf
}

// msgBuilder assembles wire frames into a reusable buffer.
type msgBuilder struct {
	buf []byte
}

func (m *msgBuilder) reset()         { m.buf = m.buf[:0] }
func (m *msgBuilder) begin(typ byte) { m.buf = append(m.buf, typ, 0, 0, 0, 0) }
func (m *msgBuilder) byte(v byte)    { m.buf = append(m.buf, v) }
func (m *msgBuilder) int16(v int16) {
	m.buf = append(m.buf, 0, 0)
	binary.BigEndian.PutUint16(m.buf[len(m.buf)-2:], uint16(v))
}
func (m *msgBuilder) int32(v int32) {
	m.buf = append(m.buf, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(m.buf[len(m.buf)-4:], uint32(v))
}
func (m *msgBuilder) cstring(s string) {
	m.buf = append(m.buf, s...)
	m.buf = append(m.buf, 0)
}
func (m *msgBuilder) bytes(b []byte) {
	m.buf = append(m.buf, b...)
}
func (m *msgBuilder) end() {
	n := len(m.buf)
	l := n - 1 // length counts everything after the type byte, incl. itself
	if l < 4 || l > maxFrameSize {
		panic("sql: msgBuilder frame overflows size limit")
	}
	binary.BigEndian.PutUint32(m.buf[1:5], uint32(l))
}
func (m *msgBuilder) flush(w io.Writer) error {
	if _, err := w.Write(m.buf); err != nil {
		return err
	}
	return nil
}

// consumeCString reads a NUL-terminated field, returning it and the remainder.
func consumeCString(b []byte) (string, []byte, error) {
	for i, c := range b {
		if c == 0 {
			return string(b[:i]), b[i+1:], nil
		}
	}
	return "", nil, io.ErrUnexpectedEOF
}

// encodeByteaText renders raw bytes in PostgreSQL's text bytea hex form.
func encodeByteaText(v []byte) []byte {
	out := make([]byte, 2+2*len(v))
	out[0], out[1] = '\\', 'x'
	hex.Encode(out[2:], v)
	return out
}

// decodeByteaText parses "\x...."; any other value is passed through untouched
// so text-format keys and non-hex payloads survive uninterpreted.
func decodeByteaText(v []byte) ([]byte, error) {
	if len(v) >= 2 && v[0] == '\\' && v[1] == 'x' {
		out := make([]byte, (len(v)-2)/2)
		if _, err := hex.Decode(out, v[2:]); err != nil {
			return nil, &pgError{code: errSyntax, msg: "invalid bytea hex encoding"}
		}
		return out, nil
	}
	return v, nil
}
