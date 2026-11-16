package api

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/fishstick/internal/database/sqlc"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

type deadLetterResponse struct {
	RunID            string     `json:"run_id"`
	JobID            string     `json:"job_id"`
	FailureReason    string     `json:"failure_reason"`
	LastError        *string    `json:"last_error"`
	AttemptCount     int32      `json:"attempt_count"`
	FailedAt         time.Time  `json:"failed_at"`
	ResolutionStatus string     `json:"resolution_status"`
	ResolvedAt       *time.Time `json:"resolved_at"`
	RetryRunID       *string    `json:"retry_run_id"`
}
type deadLetterListResponse struct {
	Data       []deadLetterResponse `json:"data"`
	NextCursor *string              `json:"next_cursor,omitempty"`
}

func deadLetterToResponse(row sqlc.DeadLetterJob) deadLetterResponse {
	r := deadLetterResponse{RunID: row.RunID.String(), JobID: row.JobID.String(), FailureReason: row.FailureReason, AttemptCount: row.AttemptCount, FailedAt: row.FailedAt, ResolutionStatus: row.ResolutionStatus}
	if row.LastError.Valid {
		v := row.LastError.String
		r.LastError = &v
	}
	if row.ResolvedAt.Valid {
		v := row.ResolvedAt.Time
		r.ResolvedAt = &v
	}
	if row.RetryRunID.Valid {
		v := row.RetryRunID.UUID.String()
		r.RetryRunID = &v
	}
	return r
}

func encodeDLCursor(row sqlc.DeadLetterJob) string {
	raw := row.FailedAt.UTC().Format(time.RFC3339Nano) + "|" + row.RunID.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}
func parseDLCursor(raw string) (time.Time, uuid.UUID, error) {
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("cursor must be valid")
	}
	parts := strings.SplitN(string(b), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, uuid.Nil, fmt.Errorf("cursor must be valid")
	}
	t, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("cursor must be valid")
	}
	id, err := uuid.Parse(parts[1])
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("cursor must be valid")
	}
	return t, id, nil
}

func (s *server) listDeadLetters(c echo.Context) error {
	// [1] Decode the opaque timestamp/UUID cursor so equal failure times page deterministically.
	limit := defaultListLimit
	if raw := strings.TrimSpace(c.QueryParam("limit")); raw != "" {
		n, e := strconv.ParseInt(raw, 10, 32)
		if e != nil || n < 1 || n > int64(maxListLimit) {
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", "limit must be a positive integer no greater than 100", nil)
		}
		limit = int32(n)
	}
	p := sqlc.ListDeadLetterPageParams{Limit: limit + 1}
	if raw := strings.TrimSpace(c.QueryParam("resolution_status")); raw != "" {
		switch raw {
		case "OPEN", "RETRIED", "RESOLVED":
		default:
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", "resolution_status must be OPEN, RETRIED, or RESOLVED", nil)
		}
		p.ResolutionStatus = sql.NullString{String: raw, Valid: true}
	}
	if raw := strings.TrimSpace(c.QueryParam("job_id")); raw != "" {
		id, e := uuid.Parse(raw)
		if e != nil {
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", "job_id must be a UUID", nil)
		}
		p.JobID = uuid.NullUUID{UUID: id, Valid: true}
	}
	if raw := strings.TrimSpace(c.QueryParam("run_id")); raw != "" {
		id, e := uuid.Parse(raw)
		if e != nil {
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", "run_id must be a UUID", nil)
		}
		p.RunID = uuid.NullUUID{UUID: id, Valid: true}
	}
	if raw := strings.TrimSpace(c.QueryParam("cursor")); raw != "" {
		t, id, e := parseDLCursor(raw)
		if e != nil {
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", e.Error(), nil)
		}
		p.Cursor = sql.NullTime{Time: t, Valid: true}
		p.CursorRunID = uuid.NullUUID{UUID: id, Valid: true}
	}
	rows, err := s.queries.ListDeadLetterPage(c.Request().Context(), p)
	if err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list dead-letter entries", nil)
	}
	var next *string
	if len(rows) > int(limit) {
		rows = rows[:limit]
		v := encodeDLCursor(rows[len(rows)-1])
		next = &v
	}
	data := make([]deadLetterResponse, 0, len(rows))
	for _, row := range rows {
		data = append(data, deadLetterToResponse(row))
	}
	return c.JSON(http.StatusOK, deadLetterListResponse{Data: data, NextCursor: next})
}

func (s *server) getDeadLetter(c echo.Context) error {
	id, ok := pathUUID(c, "run_id")
	if !ok {
		return nil
	}
	row, err := s.queries.GetDeadLetter(c.Request().Context(), id)
	if err != nil {
		if err == sql.ErrNoRows {
			return writeAPIError(c, http.StatusNotFound, "NOT_FOUND", "dead-letter entry not found", nil)
		}
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load dead-letter entry", nil)
	}
	return c.JSON(http.StatusOK, deadLetterToResponse(row))
}
