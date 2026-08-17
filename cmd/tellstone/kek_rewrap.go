/*
Package main
Tellstone Cloud-Native In-Memory Database
File: kek_rewrap.go
Description: CLI subcommand for rotating the Key Encryption Key (KEK) used by
envelope encryption. "tellstone kek rewrap" reads all shard-*.env and audit.env
files from a data directory, verifies every envelope matches the current KEK,
and re-wraps each DEK with a new KEK. The operation is offline (server must be
stopped) and idempotent (crash-safe retry). Not on the server hot path;
allocations are acceptable.

Authors:

	Maximilian Hagen
*/
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/Saxy/Tellstone/internal/crypto"
)

// runKEK dispatches "tellstone kek <subcommand>" to the appropriate handler.
func runKEK(args []string) {
	if len(args) == 0 {
		printKEKUsage()
		os.Exit(1)
	}
	switch args[0] {
	case "rewrap":
		runKEKRewrap(args[1:])
	case "-h", "--help", "help":
		printKEKUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown kek subcommand %q\n\n", args[0])
		printKEKUsage()
		os.Exit(1)
	}
}

func printKEKUsage() {
	fmt.Fprintln(os.Stdout, "tellstone kek — key encryption key utilities")
	fmt.Fprintln(os.Stdout)
	fmt.Fprintln(os.Stdout, "Usage: tellstone kek <command> [arguments]")
	fmt.Fprintln(os.Stdout)
	fmt.Fprintln(os.Stdout, "Commands:")
	fmt.Fprintln(os.Stdout, "  rewrap   Re-wrap all envelope DEKs with a new KEK (offline rotation)")
	fmt.Fprintln(os.Stdout)
	fmt.Fprintln(os.Stdout, "Run 'tellstone kek rewrap -h' for details on the rewrap command.")
}

// runKEKRewrap parses flags and rewraps all envelope files with a new KEK.
func runKEKRewrap(args []string) {
	fs := flag.NewFlagSet("tellstone kek rewrap", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	dataDir := fs.String("data-dir",
		getKEKEnv("TSD_DATA_DIR", ""),
		"Directory holding shard-*.env, audit.env, and data files (env: TSD_DATA_DIR)")
	keyFile := fs.String("key-file",
		getKEKEnv("TSD_KEY_FILE", ""),
		"Path to the current KEK file (env: TSD_KEY_FILE)")
	newKeyFile := fs.String("new-key-file",
		getKEKEnv("TSD_NEW_KEY_FILE", ""),
		"Path to the new KEK file (env: TSD_NEW_KEY_FILE)")
	retainOld := fs.Bool("retain-old-keys", false,
		"Keep a .bak copy of each original envelope before rewriting")

	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Re-wrap all envelope DEKs with a new KEK.")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "The server must be stopped before running this command.")
		fmt.Fprintln(os.Stderr, "All envelope files are verified against the current KEK before")
		fmt.Fprintln(os.Stderr, "any file is rewritten. The operation is idempotent: a crashed")
		fmt.Fprintln(os.Stderr, "rewrap can be recovered by running the same command again.")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Usage: tellstone kek rewrap [flags]")
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
	if fs.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "error: unexpected argument %q\n", fs.Arg(0))
		fs.Usage()
		os.Exit(1)
	}

	// Validate required flags.
	if *dataDir == "" {
		fmt.Fprintln(os.Stderr, "error: --data-dir is required")
		fs.Usage()
		os.Exit(1)
	}
	if *keyFile == "" {
		fmt.Fprintln(os.Stderr, "error: --key-file is required")
		fs.Usage()
		os.Exit(1)
	}
	if *newKeyFile == "" {
		fmt.Fprintln(os.Stderr, "error: --new-key-file is required")
		fs.Usage()
		os.Exit(1)
	}

	// Resolve both keys via the same provider path the server uses.
	oldKEK, err := resolveDecryptKey("", *keyFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: read current KEK: %v\n", err)
		os.Exit(1)
	}

	newKEK, err := resolveDecryptKey("", *newKeyFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: read new KEK: %v\n", err)
		os.Exit(1)
	}

	result, err := crypto.RewrapEnvelopes(*dataDir, oldKEK, newKEK, *retainOld)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "rewrap complete: %d rewrapped, %d skipped, %d total\n",
		result.Rewrapped, result.Skipped, result.Total)
}

// getKEKEnv returns the value of the environment variable key, or fallback.
func getKEKEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}
