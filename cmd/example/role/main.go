/*
Package main
Tellstone Cloud-Native In-Memory Database
File: main.go
Description: Example that drives the ROLE command family over the binary protocol: authenticate
as an admin user, create a role and a user bound to it, then verify that the new user can only
run the commands its role grants. Run a server with --rbac-config pointing at the policy.yaml
next to this file before starting the example.

Authors:

	Maximilian Hagen
*/
package main

import (
	"os"
	"strings"
	"time"

	"github.com/Saxy/Tellstone/client"
	"github.com/Saxy/Tellstone/logger"
)

func main() {
	slog := logger.NewSlogLogger(logger.LevelInfo)
	c, err := client.DialWithLogger("127.0.0.1:9988", 5*time.Second, slog)
	if err != nil {
		fatal(slog, "failed to dial server", err)
	}
	defer func() {
		if cerr := c.Close(); cerr != nil {
			slog.Log(logger.LevelWarn, "failed to close admin connection", logger.String("error", cerr.Error()))
		}
	}()

	// 4KB reusable scratch buffer for both building requests and receiving replies
	buf := make([]byte, 4*1024)

	// The server's policy file must already define "admin" with the "admin" role.
	if err = c.AuthUser("admin", "adminsecret", buf); err != nil {
		fatal(slog, "AUTH admin failed", err)
	}
	slog.Log(logger.LevelInfo, "AUTH admin", logger.String("result", "OK"))

	// Seed a value under users:1 so alice's GET below returns it instead of a
	// storage-level miss, which would be indistinguishable from an RBAC denial.
	if _, err = c.Set([]byte("users:1"), []byte("alice-in-users"), 0, buf); err != nil {
		fatal(slog, "SET users:1 failed", err)
	}
	slog.Log(logger.LevelInfo, "SET users:1", logger.String("result", "OK"))

	// ROLE CREATE defines a role that may only read keys under the "users:" prefix.
	if err = c.RoleCreate("user-reader", []string{"+get", "~users:*"}, buf); err != nil {
		fatal(slog, "ROLE CREATE failed", err)
	}
	slog.Log(logger.LevelInfo, "ROLE CREATE user-reader", logger.String("result", "OK"))

	// ROLE SETUSER binds a password-protected user to that role.
	if err = c.RoleSetUser("alice", "user-reader", [][]byte{[]byte(">alicepw")}, buf); err != nil {
		fatal(slog, "ROLE SETUSER failed", err)
	}
	slog.Log(logger.LevelInfo, "ROLE SETUSER alice", logger.String("result", "OK"))

	// ROLE GETUSER confirms the assignment.
	u, err := c.RoleGetUser("alice", buf)
	if err != nil {
		fatal(slog, "ROLE GETUSER failed", err)
	}
	slog.Log(logger.LevelInfo, "ROLE GETUSER alice",
		logger.String("role", u.Role),
		logger.Bool("has_password", u.HasPass),
	)

	// ROLE LIST enumerates every role on the server.
	entries, err := c.RoleList(buf)
	if err != nil {
		fatal(slog, "ROLE LIST failed", err)
	}
	for _, e := range entries {
		ns := make([]string, len(e.Namespaces))
		for i, p := range e.Namespaces {
			ns[i] = string(p)
		}
		slog.Log(logger.LevelInfo, "ROLE LIST",
			logger.String("role", e.Name),
			logger.String("commands", strings.Join(e.Commands, ",")),
			logger.String("namespaces", strings.Join(ns, ",")),
		)
	}

	// Open a second connection as alice and prove the role's limits: GET on a
	// matching key passes, SET and keys outside the whitelist are denied. The
	// client surfaces authorization denials as errors carrying the server's
	// NOT_AUTHORIZED error frame.
	alice, err := client.DialWithLogger("127.0.0.1:9988", 5*time.Second, slog)
	if err != nil {
		fatal(slog, "failed to dial server", err)
	}
	defer alice.Close()
	if err = alice.AuthUser("alice", "alicepw", buf); err != nil {
		fatal(slog, "AUTH alice failed", err)
	}
	slog.Log(logger.LevelInfo, "AUTH alice", logger.String("result", "OK"))

	// GET on a matching key must pass the role gate. The key was seeded
	// above, so any error here — NOT_AUTHORIZED, a transport fault, or a
	// storage miss — is a bug, not a valid outcome.
	res, err := alice.Get([]byte("users:1"), buf)
	if err != nil {
		fatal(slog, "GET users:1 as alice failed", err)
	}
	slog.Log(logger.LevelInfo, "GET users:1 as alice", logger.String("result", string(res)))

	// SET is not in alice's role, so it must come back as a NOT_AUTHORIZED
	// denial. Success means the ACL let an op through it should have blocked;
	// any other error means the transport or storage broke, not the role.
	if _, err = alice.Set([]byte("users:1"), []byte("hacked"), 0, buf); err == nil {
		fatal(slog, "SET as alice unexpectedly allowed", nil)
	}
	if !strings.Contains(err.Error(), "NOT_AUTHORIZED") {
		fatal(slog, "SET as alice denied with the wrong error", err)
	}
	slog.Log(logger.LevelInfo, "SET as alice denied", logger.String("error", err.Error()))

	// Same fail-closed check for a key outside the whitelist: the namespace
	// gate must deny it with NOT_AUTHORIZED.
	if _, err = alice.Get([]byte("accounts:1"), buf); err == nil {
		fatal(slog, "GET accounts:1 as alice unexpectedly allowed", nil)
	}
	if !strings.Contains(err.Error(), "NOT_AUTHORIZED") {
		fatal(slog, "GET accounts:1 as alice denied with the wrong error", err)
	}
	slog.Log(logger.LevelInfo, "GET accounts:1 as alice denied", logger.String("error", err.Error()))
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
