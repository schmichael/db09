package db09

import "bytes"

var EmptyValue = Value{}

// Value represent a single Key's Value as a (timestamp, deleted, value) tuple.
//
// (It's obviously a struct and not a tuple. Go doesn't have tuples. Tuple is
// just a cool databasey word to use.)
type Value struct {
	Timestamp uint64 `json:"ts,omitempty"`
	Deleted   bool   `json:"deleted,omitempty"`
	V         []byte `json:"value,omitempty"`
}

// Equals returns true if the two values are identical.
func (v Value) Equals(o Value) bool {
	if v.Timestamp != o.Timestamp {
		return false
	}
	if v.Deleted != o.Deleted {
		return false
	}
	return bytes.Equal(v.V, o.V)
}

// Empty returns true if Value is empty.
func (v Value) Empty() bool {
	return v.Timestamp == 0 && !v.Deleted && v.V == nil
}

// Result is a Value combined with the address it came from.
type Result struct {
	Value
	Addr string
}
