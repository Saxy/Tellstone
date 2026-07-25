/*
Package main
Tellstone Cloud-Native In-Memory Database
File: main.go
Description: Example TLS/mTLS client that connects to a Tellstone server with transport encryption.
Demonstrates three modes: plaintext, one-way TLS, and mutual TLS (mTLS).

Usage:
	# 1. Start server with TLS:
	go run ./cmd/tellstone --tls-cert cmd/example/tls/certs/server.crt --tls-key cmd/example/tls/certs/server.key

	# 2. Start server with mTLS:
	go run ./cmd/tellstone --tls-cert cmd/example/tls/certs/server.crt --tls-key cmd/example/tls/certs/server.key --tls-ca cmd/example/tls/certs/ca.crt

	# 3. Run this client (pass the mode as argument):
	go run ./cmd/example/tls --mode tls
	go run ./cmd/example/tls --mode mtls
	go run ./cmd/example/tls --mode plaintext
*/
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Saxy/Tellstone/internal/network"
)

func main() {
	mode := flag.String("mode", "tls", "Connection mode: plaintext, tls, or mtls")
	addr := flag.String("addr", "127.0.0.1:9988", "Tellstone server address")
	certDir := flag.String("certs", "cmd/example/tls/certs", "Directory containing TLS certificates")
	flag.Parse()

	var (
		client *network.Client
		err    error
	)

	switch *mode {
	case "plaintext":
		fmt.Println("[*] Connecting in PLAINTEXT mode...")
		client, err = network.Dial(*addr, 5*time.Second)

	case "tls":
		fmt.Println("[*] Connecting in TLS mode (one-way)...")
		client, err = network.DialTLS(*addr,
			"", "",
			*certDir+"/ca.crt",
			5*time.Second,
		)

	case "mtls":
		fmt.Println("[*] Connecting in mTLS mode (mutual TLS)...")
		client, err = network.DialTLS(*addr,
			*certDir+"/client.crt",
			*certDir+"/client.key",
			*certDir+"/ca.crt",
			5*time.Second,
		)

	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s (use plaintext, tls, or mtls)\n", *mode)
		os.Exit(1)
	}

	if err != nil {
		log.Fatalf("[-] Connection failed: %v", err)
	}
	defer client.Close()
	fmt.Printf("[+] Connected to %s via %s\n", *addr, *mode)

	buf := make([]byte, 4*1024)

	// SET
	fmt.Println("\n--- SET ---")
	res, err := client.Set([]byte("tls-demo-key"), []byte("hello from TLS client"), 0, buf)
	if err != nil {
		log.Fatalf("SET failed: %v", err)
	}
	fmt.Printf("SET => %s\n", string(res))

	// GET
	fmt.Println("\n--- GET ---")
	res, err = client.Get([]byte("tls-demo-key"), buf)
	if err != nil {
		log.Fatalf("GET failed: %v", err)
	}
	fmt.Printf("GET => %s\n", string(res))

	// DELETE
	fmt.Println("\n--- DELETE ---")
	res, err = client.Delete([]byte("tls-demo-key"), buf)
	if err != nil {
		log.Fatalf("DELETE failed: %v", err)
	}
	fmt.Printf("DEL => %s\n", string(res))

	// GET after DELETE (should return NOT_FOUND)
	fmt.Println("\n--- GET (after delete) ---")
	res, err = client.Get([]byte("tls-demo-key"), buf)
	if err != nil {
		log.Fatalf("GET failed: %v", err)
	}
	fmt.Printf("GET => %s\n", string(res))

	fmt.Println("\n[+] All operations completed successfully over", *mode)
}
