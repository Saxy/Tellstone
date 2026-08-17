/*
Package main
Tellstone Cloud-Native In-Memory Database
File: audit_decrypt.go
Description: CLI subcommand for decrypting encrypted audit log files offline.
"tellstone audit decrypt" reads a single audit file, parses the self-describing
header, resolves the correct decryption key, and writes every decrypted JSON
record to stdout or a file. Not on the server hot path; allocations are
acceptable.

Authors:

	Maximilian Hagen
*/
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Saxy/Tellstone/internal/audit"
	"github.com/Saxy/Tellstone/internal/crypto"
	"github.com/Saxy/Tellstone/internal/log"
)

// runAudit dispatches "tellstone audit <subcommand>" to the appropriate handler.
func runAudit(args []string) {
	if len(args) == 0 {
		printAuditUsage()
		os.Exit(1)
	}
	switch args[0] {
	case "decrypt":
		runAuditDecrypt(args[1:])
	case "-h", "--help", "help":
		printAuditUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown audit subcommand %q\n\n", args[0])
		printAuditUsage()
		os.Exit(1)
	}
}

func printAuditUsage() {
	fmt.Fprintln(os.Stdout, "tellstone audit — audit log utilities")
	fmt.Fprintln(os.Stdout)
	fmt.Fprintln(os.Stdout, "Usage: tellstone audit <command> [arguments]")
	fmt.Fprintln(os.Stdout)
	fmt.Fprintln(os.Stdout, "Commands:")
	fmt.Fprintln(os.Stdout, "  decrypt   Decrypt an encrypted audit log file to JSON lines")
	fmt.Fprintln(os.Stdout)
	fmt.Fprintln(os.Stdout, "Run 'tellstone audit decrypt -h' for details on the decrypt command.")
}

// runAuditDecrypt parses flags and decrypts a single audit log file.
func runAuditDecrypt(args []string) {
	fs := flag.NewFlagSet("tellstone audit decrypt", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	encryptionKey := fs.String("encryption-key",
		getDecryptEnv("TSD_ENCRYPTION_KEY", ""),
		"Base64-encoded 32-byte encryption key (env: TSD_ENCRYPTION_KEY)")
	encryptionKeyFile := fs.String("encryption-key-file",
		getDecryptEnv("TSD_ENCRYPTION_KEY_FILE", ""),
		"Path to a file holding the raw 32-byte encryption key (env: TSD_ENCRYPTION_KEY_FILE)")
	output := fs.String("output", "",
		"Write decrypted output to this file instead of stdout")

	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Decrypt an encrypted audit log file to plaintext JSON lines.")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Usage: tellstone audit decrypt <file|-|> [flags]")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Arguments:")
		fmt.Fprintln(os.Stderr, "  <file>   Path to the audit log file to decrypt")
		fmt.Fprintln(os.Stderr, "  -        Read from stdin (envelope-encrypted files require a file path)")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Flags:")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		os.Exit(1)
	}

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "error: missing required argument <file>")
		fs.Usage()
		os.Exit(1)
	}
	if fs.NArg() > 1 {
		fmt.Fprintf(os.Stderr, "error: unexpected extra arguments after %q\n", fs.Arg(0))
		os.Exit(1)
	}

	fileArg := fs.Arg(0)

	// Validate key source — mutually exclusive.
	if *encryptionKey != "" && *encryptionKeyFile != "" {
		fmt.Fprintln(os.Stderr, "error: --encryption-key and --encryption-key-file are mutually exclusive")
		os.Exit(1)
	}
	if *encryptionKey == "" && *encryptionKeyFile == "" {
		fmt.Fprintln(os.Stderr, "error: a key is required; supply --encryption-key or --encryption-key-file")
		os.Exit(1)
	}

	// Resolve the raw key bytes via the same provider path the server uses.
	key, err := resolveDecryptKey(*encryptionKey, *encryptionKeyFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	// Build the crypto engine from the resolved key.
	engine, err := crypto.NewEngine(key, log.NewNoOpLogger())
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: crypto engine: %v\n", err)
		os.Exit(1)
	}

	// Open the input source.
	var r *os.File
	var dir string
	if fileArg == "-" {
		r = os.Stdin
		// Envelope mode requires audit.env on disk — cannot resolve from stdin.
		// The header will be parsed before this matters; if the file turns out
		// to be envelope-encrypted, DecryptFile returns a clear error about the
		// missing envelope file.
		dir = "."
	} else {
		absPath, err := filepath.Abs(fileArg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: resolve path: %v\n", err)
			os.Exit(1)
		}
		r, err = os.Open(absPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		defer func() { _ = r.Close() }()
		dir = filepath.Dir(absPath)
	}

	// Decrypt.
	out, err := audit.DecryptFile(r, dir, engine)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	// Write output.
	if *output != "" {
		if err := os.WriteFile(*output, out, 0o600); err != nil {
			fmt.Fprintf(os.Stderr, "error: write output: %v\n", err)
			os.Exit(1)
		}
	} else {
		if _, err := os.Stdout.Write(out); err != nil {
			fmt.Fprintf(os.Stderr, "error: write stdout: %v\n", err)
			os.Exit(1)
		}
	}
}

// resolveDecryptKey resolves the raw encryption key from either a base64 string
// or a file path, mirroring the server's key resolution in initCrypto.
func resolveDecryptKey(encoded string, keyFile string) ([]byte, error) {
	if keyFile != "" {
		p := crypto.NewFileKeyProvider(keyFile)
		return p.Key()
	}
	p := crypto.NewBase64KeyProvider(encoded, log.NewNoOpLogger())
	return p.Key()
}

// getDecryptEnv returns the value of the environment variable key, or fallback.
// Duplicated from config.getEnv to avoid importing the config package's
// internal helper (which is unexported).
func getDecryptEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}
