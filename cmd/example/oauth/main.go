/*
Package main
Tellstone Cloud-Native In-Memory Database
File: main.go
Description: Example that authenticates to the binary protocol with an OIDC id_token as
its AUTH credential. The server maps the token's claims to a role via the policy's oauth.rules
(first match wins) and pins that role to the connection; the example then proves the role by
running SET and GET, and shows that a forged token is rejected (fail-closed). Run a server
with --rbac-config and --oauth-provider pointing at the policy.yaml next to this file before
starting the example.

Authors:

	Maximilian Hagen
*/
package main

import (
	"bufio"
	"flag"
	"os"
	"strings"
	"time"

	"github.com/Saxy/Tellstone/client"
	"github.com/Saxy/Tellstone/logger"
)

func main() {
	tokenFlag := flag.String("token", "", "OIDC id_token to present to AUTH")
	tokenFile := flag.String("token-file", "", "path to a file containing the id_token")
	addr := flag.String("addr", "127.0.0.1:9988", "server address")
	flag.Parse()

	slog := logger.NewSlogLogger(logger.LevelInfo)

	token, err := resolveToken(*tokenFlag, *tokenFile)
	if err != nil {
		fatal(slog, "no id_token supplied", err)
	}

	// Authenticate with the id_token as the credential. The server routes
	// JWT-shaped secrets to the OAuth provider; a valid token's claims resolve
	// to a role through oauth.rules and the connection's identity becomes the
	// token's sub claim.
	c, err := client.DialWithLogger(*addr, 5*time.Second, slog)
	if err != nil {
		fatal(slog, "failed to dial server", err)
	}
	defer c.Close()
	buf := make([]byte, 8*1024)

	if err = c.Auth(token, buf); err != nil {
		fatal(slog, "AUTH with id_token failed", err)
	}
	slog.Log(logger.LevelInfo, "AUTH with id_token", logger.String("result", "OK"))

	// The pinned role is admin for *@saxy.dev tokens, so SET and GET pass.
	if _, err = c.Set([]byte("oauth:demo"), []byte("authenticated-by-id-token"), 0, buf); err != nil {
		fatal(slog, "SET failed", err)
	}
	slog.Log(logger.LevelInfo, "SET oauth:demo", logger.String("result", "OK"))

	res, err := c.Get([]byte("oauth:demo"), buf)
	if err != nil {
		fatal(slog, "GET failed", err)
	}
	slog.Log(logger.LevelInfo, "GET oauth:demo", logger.String("value", string(res)))

	// Fail-closed: a well-formed-looking but invalid token must be rejected
	// exactly like a wrong password.
	bad, err := client.DialWithLogger(*addr, 5*time.Second, slog)
	if err != nil {
		fatal(slog, "failed to dial server", err)
	}
	defer bad.Close()
	if err = bad.Auth("forged.header.payload.sig", buf); err == nil {
		fatal(slog, "forged token unexpectedly accepted", nil)
	}
	slog.Log(logger.LevelInfo, "AUTH with forged token denied", logger.String("error", err.Error()))
}

// resolveToken returns the id_token from the -token flag, a -token-file, the
// TSD_ID_TOKEN environment variable, or a single line read from stdin, in that
// order. Supplying the token via stdin avoids putting credentials in shell
// history or process listings.
func resolveToken(tokenFlag, tokenFile string) (string, error) {
	switch {
	case tokenFlag != "":
		return tokenFlag, nil
	case tokenFile != "":
		b, err := os.ReadFile(tokenFile)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(b)), nil
	}
	if env := os.Getenv("TSD_ID_TOKEN"); env != "" {
		return env, nil
	}
	scanner := bufio.NewScanner(os.Stdin)
	if scanner.Scan() && strings.TrimSpace(scanner.Text()) != "" {
		return strings.TrimSpace(scanner.Text()), nil
	}
	return "", os.ErrNotExist
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
