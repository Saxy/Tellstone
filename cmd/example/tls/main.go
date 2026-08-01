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
	"os"
	"time"

	"github.com/Saxy/Tellstone/client"
	"github.com/Saxy/Tellstone/logger"
)

func main() {
	mode := flag.String("mode", "tls", "Connection mode: plaintext, tls, or mtls")
	addr := flag.String("addr", "127.0.0.1:9988", "Tellstone server address")
	certDir := flag.String("certs", "cmd/example/tls/certs", "Directory containing TLS certificates")
	flag.Parse()

	slog := logger.NewSlogLogger(logger.LevelInfo)

	var (
		c   *client.Client
		err error
	)

	switch *mode {
	case "plaintext":
		slog.Log(logger.LevelInfo, "connecting in plaintext mode")
		c, err = client.DialWithLogger(*addr, 5*time.Second, slog)

	case "tls":
		slog.Log(logger.LevelInfo, "connecting in tls mode")
		c, err = client.DialTLSWithLogger(*addr,
			"", "",
			*certDir+"/ca.crt",
			5*time.Second,
			slog,
		)

	case "mtls":
		slog.Log(logger.LevelInfo, "connecting in mtls mode")
		c, err = client.DialTLSWithLogger(*addr,
			*certDir+"/client.crt",
			*certDir+"/client.key",
			*certDir+"/ca.crt",
			5*time.Second,
			slog,
		)

	default:
		slog.Log(logger.LevelFatal, "unknown mode", logger.String("mode", *mode))
		os.Exit(1)
	}

	if err != nil {
		fatal(slog, "connection failed", err)
	}
	defer c.Close()
	slog.Log(logger.LevelInfo, "connected", logger.String("addr", *addr), logger.String("mode", *mode))

	buf := make([]byte, 4*1024)

	// SET
	res, err := c.Set([]byte("tls-demo-key"), []byte("hello from TLS client"), 0, buf)
	if err != nil {
		fatal(slog, "SET failed", err)
	}
	slog.Log(logger.LevelInfo, "SET", logger.String("result", string(res)))

	// GET
	res, err = c.Get([]byte("tls-demo-key"), buf)
	if err != nil {
		fatal(slog, "GET failed", err)
	}
	slog.Log(logger.LevelInfo, "GET", logger.String("result", string(res)))

	// DELETE
	res, err = c.Delete([]byte("tls-demo-key"), buf)
	if err != nil {
		fatal(slog, "DELETE failed", err)
	}
	slog.Log(logger.LevelInfo, "DELETE", logger.String("result", string(res)))

	// GET after DELETE (should return NOT_FOUND)
	res, err = c.Get([]byte("tls-demo-key"), buf)
	if err != nil {
		fatal(slog, "GET after delete failed", err)
	}
	slog.Log(logger.LevelInfo, "GET after delete", logger.String("result", string(res)))

	slog.Log(logger.LevelInfo, "all operations completed successfully", logger.String("mode", *mode))
}

// fatal logs the error and terminates the example process.
func fatal(l logger.Logger, msg string, err error) {
	if err != nil {
		l.Log(logger.LevelFatal, msg, logger.String("error", err.Error()))
	} else {
		l.Log(logger.LevelFatal, msg)
	}
	os.Exit(1)
}
