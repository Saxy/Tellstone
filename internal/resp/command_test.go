package resp

import (
	"bytes"
	"net"
	"strconv"
	"testing"
	"time"
)

type respValue interface{}
type respArray []respValue
type respBulk []byte
type respStatus string
type respInt int64
type respNull struct{}

func readReply(t *testing.T, conn net.Conn) []byte {
	t.Helper()
	buf := make([]byte, 0, 1024)
	tmp := make([]byte, 4096)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		n, err := conn.Read(tmp)
		if err != nil {
			t.Fatalf("read reply: %v", err)
		}
		buf = append(buf, tmp[:n]...)
		if _, err := parseRESP(buf); err == nil {
			return buf
		}
	}
}

func sendCommand(t *testing.T, conn net.Conn, send string) respValue {
	t.Helper()
	if _, err := conn.Write([]byte(send)); err != nil {
		t.Fatalf("write: %v", err)
	}
	v, err := parseRESP(readReply(t, conn))
	if err != nil {
		t.Fatalf("parse reply: %v", err)
	}
	return v
}

func parseRESP(b []byte) (respValue, error) {
	v, _, err := parseRESPAt(b, 0)
	return v, err
}

func parseRESPAt(b []byte, i int) (respValue, int, error) {
	if i >= len(b) {
		return nil, 0, errIncomplete
	}
	switch b[i] {
	case '+':
		return parseRESPStatus(b, i)
	case '-':
		return parseRESPStatus(b, i)
	case ':':
		line, n, err := parseRESPStatus(b, i)
		if err != nil {
			return nil, 0, err
		}
		v, perr := strconv.ParseInt(string(line.(respStatus)), 10, 64)
		if perr != nil {
			return nil, 0, errProtocol
		}
		return respInt(v), n, nil
	case '$':
		return parseRESPBulk(b, i)
	case '*':
		return parseRESPArray(b, i)
	default:
		return nil, 0, errProtocol
	}
}

func parseRESPStatus(b []byte, i int) (respValue, int, error) {
	end := bytes.IndexByte(b[i:], '\r')
	if end < 0 {
		return nil, 0, errIncomplete
	}
	if i+end+2 > len(b) || b[i+end+1] != '\n' {
		return nil, 0, errIncomplete
	}
	return respStatus(b[i+1 : i+end]), i + end + 2, nil
}

func parseRESPBulk(b []byte, i int) (respValue, int, error) {
	end := bytes.IndexByte(b[i:], '\r')
	if end < 0 {
		return nil, 0, errIncomplete
	}
	if i+end+2 > len(b) || b[i+end+1] != '\n' {
		return nil, 0, errIncomplete
	}
	n, err := strconv.ParseInt(string(b[i+1:i+end]), 10, 64)
	if err != nil {
		return nil, 0, errProtocol
	}
	if n < 0 {
		return respNull{}, i + end + 2, nil
	}
	dataStart := i + end + 2
	dataEnd := dataStart + int(n)
	if dataEnd+2 > len(b) || b[dataEnd] != '\r' || b[dataEnd+1] != '\n' {
		return nil, 0, errIncomplete
	}
	return append(respBulk(nil), b[dataStart:dataEnd]...), dataEnd + 2, nil
}

func parseRESPArray(b []byte, i int) (respValue, int, error) {
	end := bytes.IndexByte(b[i:], '\r')
	if end < 0 {
		return nil, 0, errIncomplete
	}
	if i+end+2 > len(b) || b[i+end+1] != '\n' {
		return nil, 0, errIncomplete
	}
	n, err := strconv.ParseInt(string(b[i+1:i+end]), 10, 64)
	if err != nil {
		return nil, 0, errProtocol
	}
	if n < 0 {
		return respNull{}, i + end + 2, nil
	}
	arr := make(respArray, 0, n)
	pos := i + end + 2
	for k := int64(0); k < n; k++ {
		var v respValue
		v, pos, err = parseRESPAt(b, pos)
		if err != nil {
			return nil, 0, err
		}
		arr = append(arr, v)
	}
	return arr, pos, nil
}

func bulk(v respValue) string {
	switch t := v.(type) {
	case respBulk:
		return string(t)
	case respStatus:
		return string(t)
	}
	return ""
}

func TestCommandCount(t *testing.T) {
	addr := startServer(t, "")
	conn := dialWithRetry(t, addr)
	defer conn.Close()

	if got := sendCommand(t, conn, "*2\r\n$7\r\nCOMMAND\r\n$5\r\nCOUNT\r\n"); got.(respInt) != respInt(len(respCommands)) {
		t.Fatalf("COMMAND COUNT = %v, want %d", got, len(respCommands))
	}
	if got := bulk(sendCommand(t, conn, "*3\r\n$7\r\nCOMMAND\r\n$5\r\nCOUNT\r\n$1\r\nx\r\n")); !hasPrefix(got, "ERR wrong number of arguments for 'command|count'") {
		t.Fatalf("COUNT arity error = %q", got)
	}
}

func TestCommandBare(t *testing.T) {
	addr := startServer(t, "")
	conn := dialWithRetry(t, addr)
	defer conn.Close()

	arr := sendCommand(t, conn, "*1\r\n$7\r\nCOMMAND\r\n").(respArray)
	if len(arr) != len(respCommands) {
		t.Fatalf("COMMAND entries = %d, want %d", len(arr), len(respCommands))
	}
	for _, entry := range arr {
		det := entry.(respArray)
		if len(det) != 10 {
			t.Fatalf("detail for %q has %d elements, want 10", bulk(det[0]), len(det))
		}
	}
	get := arr[4].(respArray)
	if bulk(get[0]) != "get" || get[1].(respInt) != 2 {
		t.Fatalf("GET name/arity = %q/%v, want get/2", bulk(get[0]), get[1])
	}
	if get[3].(respInt) != 1 || get[4].(respInt) != 1 || get[5].(respInt) != 1 {
		t.Fatalf("GET key positions = %v/%v/%v, want 1/1/1", get[3], get[4], get[5])
	}
	flags := get[2].(respArray)
	if len(flags) != 2 || bulk(flags[0]) != "readonly" || bulk(flags[1]) != "fast" {
		t.Fatalf("GET flags = %v", flags)
	}
	// ACL categories are sourced from the rbac catalog, rendered with the
	// Valkey '@' prefix; GET must be granted by the read category.
	cats := get[6].(respArray)
	var hasRead bool
	for _, c := range cats {
		if bulk(c) == "@read" {
			hasRead = true
		}
	}
	if !hasRead {
		t.Fatalf("GET ACL cats = %v, want @read from rbac", cats)
	}
	specs := get[8].(respArray)
	if len(specs) != 1 {
		t.Fatalf("GET key specs = %d, want 1", len(specs))
	}
	ping := arr[5].(respArray)
	if len(ping[8].(respArray)) != 0 {
		t.Fatalf("PING key specs = %d, want 0", len(ping[8].(respArray)))
	}
}

func TestCommandInfo(t *testing.T) {
	addr := startServer(t, "")
	conn := dialWithRetry(t, addr)
	defer conn.Close()

	arr := sendCommand(t, conn, "*6\r\n$7\r\nCOMMAND\r\n$4\r\nINFO\r\n$3\r\nGET\r\n$3\r\nset\r\n$4\r\nnope\r\n$11\r\nrole|create\r\n").(respArray)
	if len(arr) != 4 {
		t.Fatalf("INFO entries = %d, want 4", len(arr))
	}
	if bulk(arr[0].(respArray)[0]) != "get" {
		t.Fatalf("first = %q", bulk(arr[0].(respArray)[0]))
	}
	if bulk(arr[1].(respArray)[0]) != "set" {
		t.Fatalf("second = %q", bulk(arr[1].(respArray)[0]))
	}
	if _, ok := arr[2].(respNull); !ok {
		t.Fatalf("unknown name = %v, want null", arr[2])
	}
	if bulk(arr[3].(respArray)[0]) != "role|create" {
		t.Fatalf("subcommand name = %q, want role|create", bulk(arr[3].(respArray)[0]))
	}
}

func TestCommandDocs(t *testing.T) {
	addr := startServer(t, "")
	conn := dialWithRetry(t, addr)
	defer conn.Close()
	all := sendCommand(t, conn, "*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n").(respArray)
	var n int
	for i := range respCommands {
		n += 1 + len(respCommands[i].subs)
	}
	if len(all) != 2*n {
		t.Fatalf("DOCS entries = %d, want %d", len(all), 2*n)
	}
	get := sendCommand(t, conn, "*3\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n$3\r\nget\r\n").(respArray)
	if len(get) != 2 {
		t.Fatalf("DOCS get entries = %d, want 2 (name + map)", len(get))
	}
	if bulk(get[0]) != "get" {
		t.Fatalf("DOCS get name = %q", bulk(get[0]))
	}
	doc := get[1].(respArray)
	if len(doc) != 8 {
		t.Fatalf("DOCS get map = %d elements, want 8 (4 pairs)", len(doc))
	}
	if bulk(doc[0]) != "summary" || bulk(doc[1]) == "" {
		t.Fatalf("DOCS get map = %v", doc)
	}
	if got := sendCommand(t, conn, "*3\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n$4\r\nnope\r\n").(respArray); len(got) != 0 {
		t.Fatalf("DOCS unknown entries = %d, want 0", len(got))
	}
}

func TestCommandList(t *testing.T) {
	addr := startServer(t, "")
	conn := dialWithRetry(t, addr)
	defer conn.Close()

	names := sendCommand(t, conn, "*2\r\n$7\r\nCOMMAND\r\n$4\r\nLIST\r\n").(respArray)
	if len(names) != len(respCommands) {
		t.Fatalf("LIST entries = %d, want %d", len(names), len(respCommands))
	}
	for i := 1; i < len(names); i++ {
		if bulk(names[i-1]) >= bulk(names[i]) {
			t.Fatalf("LIST not sorted at %d: %q >= %q", i, bulk(names[i-1]), bulk(names[i]))
		}
	}

	read := sendCommand(t, conn,
		"*5\r\n$7\r\nCOMMAND\r\n$4\r\nLIST\r\n$8\r\nFILTERBY\r\n$6\r\nACLCAT\r\n$5\r\n@read\r\n").(respArray)
	if len(read) != 1 || bulk(read[0]) != "get" {
		t.Fatalf("LIST FILTERBY ACLCAT @read = %v, want [get]", read)
	}

	pat := sendCommand(t, conn,
		"*5\r\n$7\r\nCOMMAND\r\n$4\r\nLIST\r\n$8\r\nFILTERBY\r\n$7\r\nPATTERN\r\n$2\r\nc*\r\n").(respArray)
	if len(pat) != 1 || bulk(pat[0]) != "command" {
		t.Fatalf("LIST FILTERBY PATTERN c* = %v, want [command]", pat)
	}

	mod := sendCommand(t, conn,
		"*5\r\n$7\r\nCOMMAND\r\n$4\r\nLIST\r\n$8\r\nFILTERBY\r\n$6\r\nMODULE\r\n$4\r\njson\r\n").(respArray)
	if len(mod) != 0 {
		t.Fatalf("LIST FILTERBY MODULE json = %v, want empty", mod)
	}

	if got := bulk(sendCommand(t, conn, "*5\r\n$7\r\nCOMMAND\r\n$4\r\nLIST\r\n$8\r\nFILTERBY\r\n$5\r\nBOGUS\r\n$2\r\nxx\r\n")); !hasPrefix(got, "ERR unknown command list filter type") {
		t.Fatalf("LIST bad filter = %q", got)
	}
}

// TestCommandHelp verifies COMMAND HELP returns the subcommand reference.
func TestCommandHelp(t *testing.T) {
	addr := startServer(t, "")
	conn := dialWithRetry(t, addr)
	defer conn.Close()

	lines := sendCommand(t, conn, "*2\r\n$7\r\nCOMMAND\r\n$4\r\nHELP\r\n").(respArray)
	if len(lines) < 4 {
		t.Fatalf("HELP lines = %d, want >= 4", len(lines))
	}
	if !hasPrefix(bulk(lines[0]), "COMMAND <subcommand>") {
		t.Fatalf("HELP first = %q", bulk(lines[0]))
	}
}

// TestCommandUnknownSubcommand verifies an unknown subcommand is rejected.
func TestCommandUnknownSubcommand(t *testing.T) {
	addr := startServer(t, "")
	conn := dialWithRetry(t, addr)
	defer conn.Close()

	if got := bulk(sendCommand(t, conn, "*2\r\n$7\r\nCOMMAND\r\n$5\r\nBOGUS\r\n")); !hasPrefix(got, "ERR unknown subcommand 'BOGUS'") {
		t.Fatalf("unknown subcommand = %q", got)
	}
}

// TestCommandRBAC verifies the COMMAND gate: a limited role without the
// COMMAND bit is denied, the admin (default) role is served.
func TestCommandRBAC(t *testing.T) {
	addr := startRBACServer(t)
	conn := dialWithRetry(t, addr)
	defer conn.Close()

	// The limited role grants only +get, so COMMAND is denied.
	sendCommand(t, conn, "*3\r\n$4\r\nAUTH\r\n$7\r\nlimited\r\n$8\r\nwhatever\r\n")
	if got := bulk(sendCommand(t, conn, "*2\r\n$7\r\nCOMMAND\r\n$5\r\nCOUNT\r\n")); !hasPrefix(got, "NOPERM no permission for 'command' command") {
		t.Fatalf("limited COMMAND = %q, want NOPERM", got)
	}
	// nopass is bound to the admin role (+@all) and may introspect.
	sendCommand(t, conn, "*3\r\n$4\r\nAUTH\r\n$6\r\nnopass\r\n$8\r\nwhatever\r\n")
	if got := sendCommand(t, conn, "*2\r\n$7\r\nCOMMAND\r\n$5\r\nCOUNT\r\n").(respInt); got != respInt(len(respCommands)) {
		t.Fatalf("admin COMMAND COUNT = %v", got)
	}
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
