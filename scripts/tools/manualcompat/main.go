// Command manualcompat drives the legacy plaintext<->encrypted WAL migration
// test (scripts/test-backward-compat.sh) over the native binary protocol. It
// exists because the RESP/redis-cli frontend was removed in v2 (ADR-012).
//
// Usage:
//
//	manualcompat ping <addr>
//	manualcompat set <addr> <key> <value>
//	manualcompat get <addr> <key>
//
// set exits 0 on success. get prints the value and exits 0; it exits non-zero
// if the key is missing or the operation fails.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/Saxy/Tellstone/internal/network"
)

func usage() {
	fmt.Fprintln(os.Stderr, "usage: manualcompat <ping|set|get> <addr> [key] [value]")
	os.Exit(2)
}

func dial(addr string) *network.Client {
	c, err := network.Dial(addr, 3*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", addr, err)
		os.Exit(3)
	}
	return c
}

func main() {
	if len(os.Args) < 3 {
		usage()
	}
	cmd := os.Args[1]
	addr := os.Args[2]

	var buf []byte
	c := dial(addr)
	defer c.Close()

	switch cmd {
	case "ping":
		var out network.Message
		buf = make([]byte, 4096)
		if err := c.Call(network.MsgPing, nil, buf, &out); err != nil {
			fmt.Fprintf(os.Stderr, "ping %s: %v\n", addr, err)
			os.Exit(4)
		}
		if out.Type != network.MsgPong {
			fmt.Fprintf(os.Stderr, "ping %s: unexpected reply type %d\n", addr, out.Type)
			os.Exit(4)
		}
		fmt.Println("PONG")
	case "set":
		if len(os.Args) != 5 {
			usage()
		}
		buf = make([]byte, 4096)
		if _, err := c.Set([]byte(os.Args[3]), []byte(os.Args[4]), 0, buf); err != nil {
			fmt.Fprintf(os.Stderr, "set %s=%s: %v\n", os.Args[3], os.Args[4], err)
			os.Exit(4)
		}
	case "get":
		if len(os.Args) != 4 {
			usage()
		}
		buf = make([]byte, 4096)
		val, err := c.Get([]byte(os.Args[3]), buf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "get %s: %v\n", os.Args[3], err)
			os.Exit(4)
		}
		if len(val) == 0 {
			fmt.Fprintf(os.Stderr, "get %s: key not found\n", os.Args[3])
			os.Exit(5)
		}
		fmt.Println(string(val))
	default:
		usage()
	}
}
