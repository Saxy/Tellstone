/*
Package main
Tellstone Cloud-Native In-Memory Database
File: main.go
Description: Example that drives the ROLE command family over the binary protocol: authenticate
as an admin user, create a role and a user bound to it, then verify that the new user can only
run the commands its role grants. Run a server with --rbac-config pointing at the policy file
before starting this example.

Authors:

	Maximilian Hagen
*/
package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Saxy/Tellstone/client"
)

func main() {
	c, err := client.Dial("127.0.0.1:9988", 5*time.Second)
	if err != nil {
		log.Fatalf("failed to dial server: %v", err)
	}
	defer c.Close()

	// 4KB reusable scratch buffer for both building requests and receiving replies
	buf := make([]byte, 4*1024)

	// The server's policy file must already define "admin" with the "admin" role.
	if err = c.AuthUser("admin", "adminsecret", buf); err != nil {
		log.Fatalf("AUTH admin failed: %v", err)
	}
	fmt.Println("AUTH admin => OK")

	// Seed a value under users:1 so alice's GET below returns it instead of a
	// storage-level miss, which would be indistinguishable from an RBAC denial.
	if _, err = c.Set([]byte("users:1"), []byte("alice-in-users"), 0, buf); err != nil {
		log.Fatalf("SET users:1 failed: %v", err)
	}
	fmt.Println("SET users:1 => OK")

	// ROLE CREATE defines a role that may only read keys under the "users:" prefix.
	if err = c.RoleCreate("user-reader", []string{"+get", "~users:*"}, buf); err != nil {
		log.Fatalf("ROLE CREATE failed: %v", err)
	}
	fmt.Println("ROLE CREATE user-reader => OK")

	// ROLE SETUSER binds a password-protected user to that role.
	if err = c.RoleSetUser("alice", "user-reader", [][]byte{[]byte(">alicepw")}, buf); err != nil {
		log.Fatalf("ROLE SETUSER failed: %v", err)
	}
	fmt.Println("ROLE SETUSER alice => OK")

	// ROLE GETUSER confirms the assignment.
	u, err := c.RoleGetUser("alice", buf)
	if err != nil {
		log.Fatalf("ROLE GETUSER failed: %v", err)
	}
	fmt.Printf("ROLE GETUSER alice => role=%q has_password=%v\n", u.Role, u.HasPass)

	// ROLE LIST enumerates every role on the server.
	entries, err := c.RoleList(buf)
	if err != nil {
		log.Fatalf("ROLE LIST failed: %v", err)
	}
	for _, e := range entries {
		ns := make([]string, len(e.Namespaces))
		for i, p := range e.Namespaces {
			ns[i] = string(p)
		}
		fmt.Printf("ROLE LIST => %s commands=%v namespaces=%v\n", e.Name, e.Commands, ns)
	}

	// Open a second connection as alice and prove the role's limits: GET on a
	// matching key passes, SET and keys outside the whitelist are denied. The
	// client surfaces authorization denials as errors carrying the server's
	// "ERR NOT_AUTHORIZED" payload.
	alice, err := client.Dial("127.0.0.1:9988", 5*time.Second)
	if err != nil {
		log.Fatalf("failed to dial server: %v", err)
	}
	defer alice.Close()
	if err := alice.AuthUser("alice", "alicepw", buf); err != nil {
		log.Fatalf("AUTH alice failed: %v", err)
	}
	fmt.Println("AUTH alice => OK")

	// GET on a matching key passes the role gate; the storage layer then
	// reports whether the key exists. A NOT_AUTHORIZED error would mean the
	// role denied an allowed op — fail on that, print anything else.
	if res, err := alice.Get([]byte("users:1"), buf); err != nil && strings.Contains(err.Error(), "NOT_AUTHORIZED") {
		log.Fatalf("GET users:1 as alice denied: %v", err)
	} else {
		fmt.Printf("GET users:1 as alice => %s %v\n", res, err)
	}
	if _, err := alice.Set([]byte("users:1"), []byte("hacked"), 0, buf); err != nil {
		fmt.Printf("SET as alice denied => %v\n", err)
	} else {
		fmt.Println("SET as alice unexpectedly allowed")
	}
	if _, err := alice.Get([]byte("accounts:1"), buf); err != nil {
		fmt.Printf("GET accounts:1 as alice denied => %v\n", err)
	} else {
		fmt.Println("GET accounts:1 as alice unexpectedly allowed")
	}
}
