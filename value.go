package db09

// Value represent a single Key's Value as a (timestamp, deleted, value) tuple.
//
// (It's obviously a struct and not a tuple. Go doesn't have tuples. Tuple is
// just a cool databasey word to use.)
type Value struct {
	Timestamp uint64 `json:"ts,omitempty"`
	Deleted   bool   `json:"deleted,omitempty"`
	V         string `json:"value,omitempty"`
}

// Equals returns true if the two values are identical.
func (v *Value) Equals(o *Value) bool {
	if v.Timestamp != o.Timestamp {
		return false
	}
	if v.Deleted != o.Deleted {
		return false
	}
	return v.V == o.V
}

// Result is a Value combined with the address it came from.
type Result struct {
	*Value
	Addr string
}
