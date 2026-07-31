package client

// RoleUser is a decoded ROLE GETUSER record.
type RoleUser struct {
	Role    string // empty when the user has no explicit role
	HasPass bool
}

// RoleListEntry is one role from a ROLE LIST response.
type RoleListEntry struct {
	Name       string
	Commands   []string
	Namespaces [][]byte
}

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
// At least one password option is required: pass []byte("nopass") for a
// passwordless user. The last password option wins; nopass clears the hash.
// A ">password" option transmits the password in cleartext unless the
// connection was made with DialTLS — use DialTLS when passing secrets.
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
func (c *Client) RoleList(scratchBuf []byte) ([]RoleListEntry, error) {
	if err := c.valid(); err != nil {
		return nil, err
	}
	entries, err := c.c.RoleList(scratchBuf)
	if err != nil {
		return nil, err
	}
	out := make([]RoleListEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, RoleListEntry(e))
	}
	return out, nil
}

// RoleGetUser issues ROLE GETUSER <username> and returns the decoded record.
func (c *Client) RoleGetUser(username string, scratchBuf []byte) (RoleUser, error) {
	if err := c.valid(); err != nil {
		return RoleUser{}, err
	}
	u, err := c.c.RoleGetUser(username, scratchBuf)
	if err != nil {
		return RoleUser{}, err
	}
	return RoleUser(u), nil
}

// AuthUser authenticates with a username/password pair (RBAC mode).
// Must be called after Dial/DialTLS when the server runs with --rbac-config.
// The password travels in cleartext unless the connection was made with
// DialTLS — use DialTLS when transmitting secrets.
func (c *Client) AuthUser(username, password string, scratchBuf []byte) error {
	if err := c.valid(); err != nil {
		return err
	}
	return c.c.AuthUser(username, password, scratchBuf)
}
