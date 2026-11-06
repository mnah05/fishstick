package api

import (
	"encoding/json"
	"time"
)

// OptionalTime distinguishes JSON omitted (no change), null (clear), and a timestamp value.
type OptionalTime struct {
	set   bool
	clear bool
	value *time.Time
}

// UnmarshalJSON implements PATCH semantics for scheduled_at on job updates.
func (o *OptionalTime) UnmarshalJSON(data []byte) error {
	o.set = true
	if string(data) == "null" {
		o.clear = true
		o.value = nil
		return nil
	}
	var t time.Time
	if err := json.Unmarshal(data, &t); err != nil {
		return err
	}
	o.clear = false
	o.value = &t
	return nil
}

func (o OptionalTime) IsSet() bool { return o.set }

func (o OptionalTime) Clears() bool { return o.set && o.clear }

func (o OptionalTime) TimePtr() *time.Time {
	if !o.set || o.clear {
		return nil
	}
	return o.value
}
