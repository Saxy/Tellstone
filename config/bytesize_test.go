package config

import "testing"

func TestByteSizeParsing(t *testing.T) {
    cases := map[string]uint32{
        "16MiB": 16 * 1024 * 1024,
        "1GiB":  1 * 1024 * 1024 * 1024,
        "256KB": 256 * 1000,
        "0":     0,
        "123":   123,
    }
    for input, want := range cases {
        var b ByteSize
        if err := b.Set(input); err != nil {
            t.Fatalf("Set(%q) returned error: %v", input, err)
        }
        if uint32(b) != want {
            t.Fatalf("Set(%q) = %d, want %d", input, b, want)
        }
    }
}
