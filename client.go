package db09

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
)

var errMap map[string]error = map[string]error{
	TooManyReplicas.Error(): TooManyReplicas,
	EmptyKey.Error():        EmptyKey,
	NotFound.Error():        NotFound,
	StaleWrite.Error():      StaleWrite,
	WrongNode.Error():       WrongNode,
}

type Client struct {
	addr string
}

func (c *Client) path(key []byte, r int) string {
	u := url.URL{
		Scheme:   "http",
		Host:     c.addr,
		Path:     "keys/" + url.QueryEscape(string(key)),
		RawQuery: "rl=" + strconv.Itoa(r),
	}
	return u.String()
}

func (c *Client) SendGossip(s *State) error {
	panic("not implemented")
}

func (c *Client) RecvGossip() *State {
	panic("not implemented")
}

func (c *Client) Get(key []byte, replicas int) (*Value, error) {
	resp, err := http.Get(c.path(key, replicas))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 500:
		//TODO ReadAll is basically always a bad idea, but w/e yolo
		buf, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		if err, ok := errMap[string(buf)]; ok {
			return nil, err
		}
		return nil, fmt.Errorf("unknown error: %q", buf)
	case 404:
		return nil, NotFound
	case 200:
		v := &Value{}
		if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
			return nil, err
		}
		return v, nil
	default:
		return nil, fmt.Errorf("unknown status: %d", resp.StatusCode)
	}
}

func (c *Client) Set(key []byte, v *Value, replicas int) error {
	buf, err := json.Marshal(v)
	if err != nil {
		return err
	}
	resp, err := http.Post(c.path(key, replicas), "application/json", bytes.NewReader(buf))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	panic("FIXME")
}

func (c *Client) Addr() string {
	return c.addr
}

func (c *Client) String() string {
	return c.addr
}
