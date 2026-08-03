/*
Package main
Tellstone Cloud-Native In-Memory Database
File: main.go
Description: Example that drives the ACL command family over the binary protocol: authenticate
as an admin user, create a password-protected user bound to the readonly role via ACL SETUSER,
prove the new user can only run its role's commands, trigger auth failures, and read them back
with ACL LOG before ACL DELUSER cleans up. Run a server with --rbac-config pointing at the
policy.yaml next to this file before starting the example.

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

	// ACL SETUSER binds a password-protected user to the readonly role that the
	// policy file already defines. The role must exist; the server rejects a
	// dangling role name fail-closed.
	if err = c.AclSetUser("carol", "readonly", [][]byte{[]byte(">carolpw")}, buf); err != nil {
		fatal(slog, "ACL SETUSER failed", err)
	}
	slog.Log(logger.LevelInfo, "ACL SETUSER carol", logger.String("result", "OK"))

	// ACL LIST enumerates every user with its bound role, password presence,
	// and the role's granted commands and namespace whitelist.
	users, err := c.AclList(buf)
	if err != nil {
		fatal(slog, "ACL LIST failed", err)
	}
	for _, u := range users {
		slog.Log(logger.LevelInfo, "ACL LIST",
			logger.String("user", u.Username),
			logger.String("role", u.Role),
			logger.Bool("has_password", u.HasPass),
			logger.String("commands", strings.Join(u.Commands, ",")),
		)
	}

	// Seed a value under users:1 so carol's GET below returns it instead of a
	// storage-level miss, which would be indistinguishable from an RBAC denial.
	if _, err = c.Set([]byte("users:1"), []byte("carol-in-users"), 0, buf); err != nil {
		fatal(slog, "SET users:1 failed", err)
	}
	slog.Log(logger.LevelInfo, "SET users:1", logger.String("result", "OK"))

	// Open a second connection as carol and prove the readonly role's limits:
	// GET passes, SET is denied with NOT_AUTHORIZED.
	carol, err := client.DialWithLogger("127.0.0.1:9988", 5*time.Second, slog)
	if err != nil {
		fatal(slog, "failed to dial server", err)
	}
	defer carol.Close()
	if err = carol.AuthUser("carol", "carolpw", buf); err != nil {
		fatal(slog, "AUTH carol failed", err)
	}
	slog.Log(logger.LevelInfo, "AUTH carol", logger.String("result", "OK"))

	res, err := carol.Get([]byte("users:1"), buf)
	if err != nil {
		fatal(slog, "GET users:1 as carol failed", err)
	}
	slog.Log(logger.LevelInfo, "GET users:1 as carol", logger.String("result", string(res)))

	if _, err = carol.Set([]byte("users:1"), []byte("hacked"), 0, buf); err == nil {
		fatal(slog, "SET as carol unexpectedly allowed", nil)
	}
	if !strings.Contains(err.Error(), "NOT_AUTHORIZED") {
		fatal(slog, "SET as carol denied with the wrong error", err)
	}
	slog.Log(logger.LevelInfo, "SET as carol denied", logger.String("error", err.Error()))

	// A bad password and an unknown user must land in the store-wide ACL LOG
	// buffer with the attempted username, remote address, and reason.
	bad, err := client.DialWithLogger("127.0.0.1:9988", 5*time.Second, slog)
	if err != nil {
		fatal(slog, "failed to dial server", err)
	}
	defer bad.Close()
	if err = bad.AuthUser("carol", "wrongpw", buf); err == nil {
		fatal(slog, "AUTH with wrong password unexpectedly succeeded", nil)
	}
	slog.Log(logger.LevelInfo, "AUTH carol wrong password", logger.String("result", "denied"))
	if err = bad.AuthUser("ghost", "x", buf); err == nil {
		fatal(slog, "AUTH as unknown user unexpectedly succeeded", nil)
	}
	slog.Log(logger.LevelInfo, "AUTH ghost unknown user", logger.String("result", "denied"))

	// ACL LOG reads the auth-failure buffer back over the binary protocol,
	// oldest entry first.
	entries, err := c.AclLog(buf)
	if err != nil {
		fatal(slog, "ACL LOG failed", err)
	}
	for _, e := range entries {
		slog.Log(logger.LevelInfo, "ACL LOG",
			logger.String("timestamp", e.Timestamp),
			logger.String("username", e.Username),
			logger.String("remote_addr", e.RemoteAddr),
			logger.String("reason", e.Reason),
		)
	}

	// ACL DELUSER revokes carol: her credentials stop working.
	if err = c.AclDelUser("carol", buf); err != nil {
		fatal(slog, "ACL DELUSER failed", err)
	}
	slog.Log(logger.LevelInfo, "ACL DELUSER carol", logger.String("result", "OK"))
	if err = carol.AuthUser("carol", "carolpw", buf); err == nil {
		fatal(slog, "AUTH carol after ACL DELUSER unexpectedly succeeded", nil)
	}
	slog.Log(logger.LevelInfo, "AUTH carol after DELUSER", logger.String("result", "denied"))
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
