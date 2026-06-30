package resp

import (
	"bytes"
	"errors"
	"testing"
)

func TestParseCompleteCommand(t *testing.T) {
	in := []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nhello\r\n")
	args, consumed, err := Parse(in, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if consumed != len(in) {
		t.Fatalf("consumed=%d want %d", consumed, len(in))
	}
	if len(args) != 3 {
		t.Fatalf("got %d args, want 3", len(args))
	}
	if !EqualFold(args[0], "SET") || string(args[1]) != "key" || string(args[2]) != "hello" {
		t.Fatalf("bad args: %q %q %q", args[0], args[1], args[2])
	}
}

func TestParseIncomplete(t *testing.T) {
	full := []byte("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
	// Every strict prefix must report errIncomplete, never a false success or protocol error.
	for i := 1; i < len(full); i++ {
		_, _, err := Parse(full[:i], nil)
		if !errors.Is(err, errIncomplete) {
			t.Fatalf("prefix len %d: got %v, want errIncomplete", i, err)
		}
	}
	args, consumed, err := Parse(full, nil)
	if err != nil || consumed != len(full) || len(args) != 2 {
		t.Fatalf("full parse failed: args=%d consumed=%d err=%v", len(args), consumed, err)
	}
}

func TestParsePipelined(t *testing.T) {
	in := []byte("*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
	dst := make([][]byte, 0, 4)

	args, consumed, err := Parse(in, dst)
	if err != nil || !EqualFold(args[0], "PING") {
		t.Fatalf("first parse: err=%v args=%v", err, args)
	}
	rest := in[consumed:]
	args, consumed2, err := Parse(rest, dst)
	if err != nil || !EqualFold(args[0], "GET") || string(args[1]) != "k" {
		t.Fatalf("second parse: err=%v args=%v", err, args)
	}
	if consumed+consumed2 != len(in) {
		t.Fatalf("consumed %d+%d != %d", consumed, consumed2, len(in))
	}
}

func TestParseMalformed(t *testing.T) {
	cases := map[string][]byte{
		"not a multibulk":      []byte("+OK\r\n"),
		"arg not a bulk":       []byte("*1\r\n+notbulk\r\n"),
		"wrong body terminator": []byte("*1\r\n$2\r\nabXX"),
	}
	for name, c := range cases {
		if _, _, err := Parse(c, nil); !errors.Is(err, errProtocol) {
			t.Fatalf("%s (%q): got %v, want errProtocol", name, c, err)
		}
	}
}

func TestEncoders(t *testing.T) {
	check := func(got, want []byte, name string) {
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: got %q want %q", name, got, want)
		}
	}
	check(AppendSimpleString(nil, "OK"), []byte("+OK\r\n"), "simple")
	check(AppendError(nil, "ERR boom"), []byte("-ERR boom\r\n"), "error")
	check(AppendBulk(nil, []byte("hello")), []byte("$5\r\nhello\r\n"), "bulk")
	check(AppendBulk(nil, []byte{}), []byte("$0\r\n\r\n"), "empty bulk")
	check(AppendNullBulk(nil), []byte("$-1\r\n"), "null bulk")
	check(AppendInt(nil, 1), []byte(":1\r\n"), "int")
}

func TestEqualFold(t *testing.T) {
	if !EqualFold([]byte("set"), "SET") || !EqualFold([]byte("SeT"), "SET") {
		t.Fatal("case-insensitive match failed")
	}
	if EqualFold([]byte("getx"), "GET") || EqualFold([]byte("ge"), "GET") {
		t.Fatal("length mismatch should not match")
	}
}
