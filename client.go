package db09

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
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

func NewClient(addr string) *Client {
	return &Client{addr: addr}
}

func (c *Client) keypath(key string, r int) string {
	u := url.URL{
		Scheme:   "http",
		Host:     c.addr,
		Path:     "keys/" + url.QueryEscape(key),
		RawQuery: "rl=" + strconv.Itoa(r),
	}
	return u.String()
}

func (c *Client) GossipUpdate(s *State) error {
	buf, err := json.Marshal(s)
	if err != nil {
		return err
	}
	resp, err := http.Post("http://"+c.addr+"/gossip", "application/json", bytes.NewReader(buf))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200:
		return nil
	case 500:
		//TODO ReadAll is basically always a bad idea, but w/e yolo
		buf, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if err, ok := errMap[string(buf)]; ok {
			return err
		}
		return fmt.Errorf("unknown error: %q", buf)
	default:
		return fmt.Errorf("unknown status: %d", resp.StatusCode)
	}
}

func (c *Client) Gossip() *State {
	resp, err := http.Get("http://" + c.addr + "/gossip")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200:
		s := &State{}
		if err := json.NewDecoder(resp.Body).Decode(s); err != nil {
			log.Printf("Error decoding gossip response: %v", err)
			return nil
		}
		return s
	default:
		log.Printf("Unexpected Gossip() response: %d", resp.StatusCode)
		return nil
	}
}

func (c *Client) Get(key string, replicas int) (*Value, error) {
	resp, err := http.Get(c.keypath(key, replicas))
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

func (c *Client) Set(key string, v *Value, replicas int) error {
	buf, err := json.Marshal(v)
	if err != nil {
		return err
	}
	resp, err := http.Post(c.keypath(key, replicas), "application/json", bytes.NewReader(buf))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200:
		return nil
	case 500:
		//TODO ReadAll is basically always a bad idea, but w/e yolo
		buf, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if err, ok := errMap[string(buf)]; ok {
			return err
		}
		return fmt.Errorf("unknown error: %q", buf)
	default:
		return fmt.Errorf("unknown status: %d", resp.StatusCode)
	}
}

func (c *Client) Version() (version uint64) {
	resp, err := http.Get("http://" + c.addr + "/gossip/version")
	if err != nil {
		log.Printf("Error getting version from client %s: %v", c.addr, err)
		return
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200:
		buf, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Error reading version response from client %s: %v", c.addr, err)
			return
		}
		version, err = strconv.ParseUint(string(buf), 10, 64)
		if err != nil {
			log.Printf("Error parsing version from client %s: %v", c.addr, err)
		}
	default:
		log.Printf("Unexpected status code from client %s when retrieving version: %d", c.addr, resp.StatusCode)
	}
	return
}

func (c *Client) Addr() string {
	return c.addr
}

func (c *Client) String() string {
	return c.addr
}
