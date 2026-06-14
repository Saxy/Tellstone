package protocol

import "testing"

func TestParseQuery_Valid(t *testing.T) {
	tests := []struct {
		name   string
		query  []byte
		wantTy CommandType
	}{
		{"select", []byte("SELECT * FROM kv WHERE key='k'"), CmdGet},
		{"insert", []byte("INSERT INTO kv (key, value) VALUES ('k','v')"), CmdSet},
		{"delete", []byte("DELETE FROM kv WHERE key='k'"), CmdDelete},
	}

	for _, tt := range tests {
		q, err := ParseQuery(tt.query)
		if err != nil {
			t.Fatalf("%s: unexpected error %v", tt.name, err)
		}
		if q.Type != tt.wantTy {
			t.Fatalf("%s: expected type %v, got %v", tt.name, tt.wantTy, q.Type)
		}
	}
}

func TestParseQuery_Invalid(t *testing.T) {
	invalid := [][]byte{
		{},                                         // empty
		[]byte("   "),                               // whitespace only
		[]byte("DROP TABLE kv"),                   // unknown command
		[]byte("SELECT * FROM kv"),               // missing WHERE
		[]byte("INSERT INTO kv VALUES ('k')"),    // malformed values
		[]byte("DELETE FROM kv"),                 // missing WHERE
	}

	for i, q := range invalid {
		_, err := ParseQuery(q)
		if err == nil {
			t.Fatalf("invalid case %d: expected error, got nil", i)
		}
		if err != ErrParse {
			t.Fatalf("invalid case %d: expected ErrParse, got %v", i, err)
		}
	}
}

func TestParseQuery_TTLOverflow(t *testing.T) {
	overflowQuery := []byte("INSERT INTO kv (key, value, ttl_ms) VALUES ('k','v', 9999999999999)")
	_, err := ParseQuery(overflowQuery)
	if err != ErrTTLOverflow {
		t.Fatalf("expected ErrTTLOverflow, got %v", err)
	}
}
