package db09

type Client struct {
	addr string
}

func (c *Client) SendGossip(s *State) error {
	panic("not implemented")
}

func (c *Client) RecvGossip() *State {
	panic("not implemented")
}

func (c *Client) Get(key []byte, replicas int) (*Value, error) {
	panic("not implemented")
}

func (c *Client) Set(key []byte, v *Value, replicas int) error {
	panic("not implemented")
}

func (c *Client) Addr() string {
	return c.addr
}

func (c *Client) String() string {
	return c.addr
}
