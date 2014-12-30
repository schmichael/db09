package db09

import "bytes"

// Value represent a single Key's Value as a (timestamp, deleted, value) tuple.
//
// (It's obviously a struct and not a tuple. Go doesn't have tuples. Tuple is
// just a cool databasey word to use.)
type Value struct {
	Timestamp uint64 `json:"ts"`
	Deleted   bool   `json:"deleted,omitempty"`
	V         []byte `json:"value,omitempty"`
}

func (v *Value) Equals(o *Value) bool {
	if v.Timestamp != o.Timestamp {
		return false
	}
	if v.Deleted != o.Deleted {
		return false
	}
	return bytes.Equal(v.V, o.V)
}

// Result is a Value combined with the address it came from.
type Result struct {
	*Value
	Addr string
}
