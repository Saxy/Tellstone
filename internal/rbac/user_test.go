/*
Package rbac
Tellstone Role-Based Access Control Tests
File: user_test.go
Description: Verifies the User model and the ROLE SETUSER password-option parser: nopass,
>password hashing, option ordering, and malformed-option rejection.
*/
package rbac

import (
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func TestPasswordFromOptsNopass(t *testing.T) {
	hash, err := PasswordFromOpts([][]byte{[]byte("nopass")})
	if err != nil {
		t.Fatalf("PasswordFromOpts: %v", err)
	}
	if hash != nil {
		t.Fatal("nopass must clear the hash")
	}
}

func TestPasswordFromOptsHashesPlaintext(t *testing.T) {
	hash, err := PasswordFromOpts([][]byte{[]byte(">sekret")})
	if err != nil {
		t.Fatalf("PasswordFromOpts: %v", err)
	}
	if hash == nil {
		t.Fatal(">password must produce a hash")
	}
	if bcrypt.CompareHashAndPassword(hash, []byte("sekret")) != nil {
		t.Fatal("hash must verify the original plaintext")
	}
}

func TestPasswordFromOptsOrdering(t *testing.T) {
	hash, err := PasswordFromOpts([][]byte{[]byte(">one"), []byte("nopass"), []byte(">two")})
	if err != nil {
		t.Fatalf("PasswordFromOpts: %v", err)
	}
	if bcrypt.CompareHashAndPassword(hash, []byte("two")) != nil {
		t.Fatal("last password option must win")
	}
}

func TestPasswordFromOptsEmptyGivesNil(t *testing.T) {
	hash, err := PasswordFromOpts(nil)
	if err != nil {
		t.Fatalf("PasswordFromOpts: %v", err)
	}
	if hash != nil {
		t.Fatal("no options must yield a nopass user")
	}
}

func TestPasswordFromOptsMalformed(t *testing.T) {
	if _, err := PasswordFromOpts([][]byte{[]byte("bogus")}); err == nil {
		t.Fatal("malformed option must be rejected")
	}
	if _, err := PasswordFromOpts([][]byte{[]byte(">")}); err == nil {
		t.Fatal("bare '>' must be rejected")
	}
}
