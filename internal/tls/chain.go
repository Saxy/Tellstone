package tls

import "errors"

type chain struct {
	calls []func() error
	err   error
}

func (c *chain) then(fn func() error) *chain {
	c.calls = append(c.calls, fn)
	return c
}

func (c *chain) exec() error {
	if c.err != nil {
		return c.err
	}

	if len(c.calls) == 0 {
		return errors.New("chain calls length is zero")
	}
	err := c.calls[0]()

	// data not enough should read next round, else remove current call function
	if errors.Is(err, ErrNotEnough) {
		return err
	}

	c.err = err
	c.calls = c.calls[1:]

	return err
}
