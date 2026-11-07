package api

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// SQL null wrappers converted to JSON-friendly pointers for responses.

func nullTimePtr(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	t := value.Time
	return &t
}

func nullStringPtr(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	s := value.String
	return &s
}

func nullUUIDPtr(value uuid.NullUUID) *string {
	if !value.Valid {
		return nil
	}
	s := value.UUID.String()
	return &s
}

func nullInt32Ptr(value sql.NullInt32) *int32 {
	if !value.Valid {
		return nil
	}
	n := value.Int32
	return &n
}

func nullRawMessage(value sql.Null[[]byte]) json.RawMessage {
	if !value.Valid {
		return nil
	}
	return json.RawMessage(value.V)
}

// JSON pointers converted to SQL null wrappers for writes.

func nullTime(value *time.Time) sql.NullTime {
	if value == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: *value, Valid: true}
}

func nullString(value *string) sql.NullString {
	if value == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: *value, Valid: true}
}

func nextCursorFromID(id uuid.UUID) *string {
	s := id.String()
	return &s
}
