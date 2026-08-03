package client

// ACLUser is a decoded ACL LIST record.
type ACLUser struct {
	Username   string
	Role       string // empty when the default role applies
	HasPass    bool
	Commands   []string
	Namespaces [][]byte
}

// AuthLogEntry is one decoded ACL LOG record.
type AuthLogEntry struct {
	Timestamp  string // RFC3339, matching the RESP ACL LOG rendering
	Username   string
	RemoteAddr string
	Reason     string
}

// AclSetUser issues ACL SETUSER <username> <role> [>password] [nopass] on the
// binary protocol. The role must already exist. At least one password option is
// required: pass []byte("nopass") for a passwordless user. A ">password"
// option transmits the password in cleartext unless the connection was made
// with DialTLS — use DialTLS when passing secrets.
func (c *Client) AclSetUser(username, role string, passOptions [][]byte, scratchBuf []byte) error {
	if err := c.valid(); err != nil {
		return err
	}
	return c.c.AclSetUser(username, role, passOptions, scratchBuf)
}

// AclDelUser issues ACL DELUSER <username>.
func (c *Client) AclDelUser(username string, scratchBuf []byte) error {
	if err := c.valid(); err != nil {
		return err
	}
	return c.c.AclDelUser(username, scratchBuf)
}

// AclList issues ACL LIST and returns the decoded users.
func (c *Client) AclList(scratchBuf []byte) ([]ACLUser, error) {
	if err := c.valid(); err != nil {
		return nil, err
	}
	users, err := c.c.AclList(scratchBuf)
	if err != nil {
		return nil, err
	}
	out := make([]ACLUser, 0, len(users))
	for _, u := range users {
		out = append(out, ACLUser(u))
	}
	return out, nil
}

// AclLog issues ACL LOG and returns the recent auth-failure buffer in
// chronological order.
func (c *Client) AclLog(scratchBuf []byte) ([]AuthLogEntry, error) {
	if err := c.valid(); err != nil {
		return nil, err
	}
	entries, err := c.c.AclLog(scratchBuf)
	if err != nil {
		return nil, err
	}
	out := make([]AuthLogEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, AuthLogEntry(e))
	}
	return out, nil
}
