package client

import (
	"github.com/Saxy/Tellstone/internal/network"
)

// RoleCreate issues ROLE CREATE <name> <rule>... on the binary protocol.
// Rule tokens follow the RESP conventions: "+cmd", "-cmd", "+@category",
// "-@category", "~prefix", "~*". Fails when the role already exists.
func (c *Client) RoleCreate(role string, rules []string, scratchBuf []byte) error {
	if err := c.valid(); err != nil {
		return err
	}
	return c.c.RoleCreate(role, rules, scratchBuf)
}

// RoleSetUser issues ROLE SETUSER <username> <role> [>password] [nopass].
// The last password option wins; nopass clears the hash (passwordless user).
func (c *Client) RoleSetUser(username, role string, passOptions [][]byte, scratchBuf []byte) error {
	if err := c.valid(); err != nil {
		return err
	}
	return c.c.RoleSetUser(username, role, passOptions, scratchBuf)
}

// RoleDelUser issues ROLE DELUSER <username>.
func (c *Client) RoleDelUser(username string, scratchBuf []byte) error {
	if err := c.valid(); err != nil {
		return err
	}
	return c.c.RoleDelUser(username, scratchBuf)
}

// RoleDelete issues ROLE DELETE <role>.
func (c *Client) RoleDelete(role string, scratchBuf []byte) error {
	if err := c.valid(); err != nil {
		return err
	}
	return c.c.RoleDelete(role, scratchBuf)
}

// RoleList issues ROLE LIST and returns the decoded roles.
func (c *Client) RoleList(scratchBuf []byte) ([]network.RoleListEntry, error) {
	if err := c.valid(); err != nil {
		return nil, err
	}
	return c.c.RoleList(scratchBuf)
}

// RoleGetUser issues ROLE GETUSER <username> and returns the decoded record.
func (c *Client) RoleGetUser(username string, scratchBuf []byte) (network.RoleUser, error) {
	if err := c.valid(); err != nil {
		return network.RoleUser{}, err
	}
	return c.c.RoleGetUser(username, scratchBuf)
}

// AuthUser authenticates with a username/password pair (RBAC mode).
// Must be called after Dial/DialTLS when the server runs with --rbac-config.
func (c *Client) AuthUser(username, password string, scratchBuf []byte) error {
	if err := c.valid(); err != nil {
		return err
	}
	return c.c.AuthUser(username, password, scratchBuf)
}
