package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/fishstick/internal/database/sqlc"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

// ListJobRunsResponse is a page of runs (newest first by run id).
type ListJobRunsResponse struct {
	Data       []RunResponse `json:"data"`
	NextCursor *string       `json:"next_cursor,omitempty"`
}

// CancelRunRequest is an optional JSON body for POST /v1/cancel_run/:run_id.
type CancelRunRequest struct {
	Reason *string `json:"reason"`
}

// RunResponse exposes execution state without internal fencing or idempotency fields.
type RunResponse struct {
	ID                   string          `json:"id"`
	JobID                string          `json:"job_id"`
	RetryOfRunID         *string         `json:"retry_of_run_id"`
	TriggerSource        string          `json:"trigger_source"`
	ScheduledFor         time.Time       `json:"scheduled_for"`
	EventType            string          `json:"event_type"`
	Payload              json.RawMessage `json:"payload"`
	TimeoutSeconds       int32           `json:"timeout_seconds"`
	Status               string          `json:"status"`
	AttemptCount         int32           `json:"attempt_count"`
	MaxAttempts          int32           `json:"max_attempts"`
	WorkerID             *string         `json:"worker_id"`
	LeaseExpiresAt       *time.Time      `json:"lease_expires_at"`
	HeartbeatAt          *time.Time      `json:"heartbeat_at"`
	RetryAt              *time.Time      `json:"retry_at"`
	DeadlineAt           *time.Time      `json:"deadline_at"`
	StartedAt            *time.Time      `json:"started_at"`
	LastAttemptStartedAt *time.Time      `json:"last_attempt_started_at"`
	FinishedAt           *time.Time      `json:"finished_at"`
	CancelRequestedAt    *time.Time      `json:"cancel_requested_at"`
	CancelledAt          *time.Time      `json:"cancelled_at"`
	CancelReason         *string         `json:"cancel_reason"`
	LastError            *string         `json:"last_error"`
	ExitCode             *int32          `json:"exit_code"`
	Result               json.RawMessage `json:"result"`
	CreatedAt            time.Time       `json:"created_at"`
	UpdatedAt            time.Time       `json:"updated_at"`
}

// triggerJob creates a manual run and its dispatch event in one transaction.
func (s *server) triggerJob(c echo.Context) error {
	jobID, ok := pathUUID(c, "job_id")
	if !ok {
		return nil
	}
	key, ok := idempotencyKeyFromHeader(c.Request())
	if !ok {
		return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Idempotency-Key header is required", nil)
	}
	// This endpoint has no body, so do not silently ignore unsupported input.
	if c.Request().Body != nil {
		var b [1]byte
		n, readErr := io.ReadFull(c.Request().Body, b[:])
		if n != 0 || (readErr != nil && !errors.Is(readErr, io.EOF)) {
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", "request body must be empty", nil)
		}
	}
	hash, err := requestHashSHA256(struct{}{})
	if err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to hash request", nil)
	}

	ctx := c.Request().Context()
	tx, qtx, err := s.beginTx(ctx)
	if err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to begin transaction", nil)
	}
	defer tx.Rollback()

	// Serialize triggers for this job and keep its snapshot stable until commit.
	job, err := qtx.GetJobForUpdate(ctx, jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return writeAPIError(c, http.StatusNotFound, "NOT_FOUND", "job not found", nil)
	}
	if err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load job", nil)
	}

	existing, err := qtx.GetManualRunByIdempotencyKey(ctx, sqlc.GetManualRunByIdempotencyKeyParams{
		JobID:                 jobID,
		TriggerIdempotencyKey: sql.NullString{String: key, Valid: true},
	})
	if err == nil {
		// Release the lock before writing the replay response; no writes were made.
		if err := tx.Rollback(); err != nil {
			return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to finish transaction", nil)
		}
		if !existing.TriggerRequestHash.Valid || existing.TriggerRequestHash.String != hash {
			return writeAPIError(c, http.StatusConflict, "IDEMPOTENCY_KEY_REUSED", "idempotency key was already used with a different request body", nil)
		}
		return c.JSON(http.StatusAccepted, runToResponse(existing))
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to lookup idempotency key", nil)
	}

	runID, err := uuid.NewV7()
	if err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to generate run ID", nil)
	}
	outboxID, err := uuid.NewV7()
	if err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to generate outbox ID", nil)
	}
	run, err := qtx.ManualJobRun(ctx, sqlc.ManualJobRunParams{
		ID: runID, JobID: job.ID,
		TriggerIdempotencyKey: sql.NullString{String: key, Valid: true},
		TriggerRequestHash:    sql.NullString{String: hash, Valid: true},
		EventTypeSnapshot:     job.EventType, PayloadSnapshot: job.Payload,
		TimeoutSeconds: job.TimeoutSeconds, MaxAttempts: job.MaxAttempts,
	})
	if err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to create job run", nil)
	}
	message, err := json.Marshal(struct {
		RunID string `json:"run_id"`
	}{RunID: run.ID.String()})
	if err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to encode dispatch event", nil)
	}
	if _, err := qtx.CreateOutboxRecord(ctx, sqlc.CreateOutboxRecordParams{
		ID: outboxID, RunID: run.ID, Message: message,
	}); err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to create dispatch event", nil)
	}
	if err := tx.Commit(); err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to commit triggered run", nil)
	}
	return c.JSON(http.StatusAccepted, runToResponse(run))
}

// getJobRun returns one run by id; it must belong to the given job.
func (s *server) getJobRun(c echo.Context) error {
	jobID, ok := pathUUID(c, "job_id")
	if !ok {
		return nil
	}
	runID, ok := pathUUID(c, "run_id")
	if !ok {
		return nil
	}
	run, err := s.queries.GetJobRun(c.Request().Context(), sqlc.GetJobRunParams{ID: runID, JobID: jobID})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return writeAPIError(c, http.StatusNotFound, "NOT_FOUND", "run not found", nil)
		}
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load run", nil)
	}
	return c.JSON(http.StatusOK, runToResponse(run))
}

// listJobRuns pages runs for a job (newest first), same limit/cursor rules as list jobs.
func (s *server) listJobRuns(c echo.Context) error {
	jobID, ok := pathUUID(c, "job_id")
	if !ok {
		return nil
	}
	limit, cursor, err := ParseListCursor(c.Request())
	if err != nil {
		return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error(), nil)
	}
	params := sqlc.ListJobRunsPageParams{JobID: jobID, Limit: limit + 1}
	if cursor != uuid.Nil {
		params.Cursor = uuid.NullUUID{UUID: cursor, Valid: true}
	}
	rows, err := s.queries.ListJobRunsPage(c.Request().Context(), params)
	if err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list runs", nil)
	}
	rows, nextCursor := paginate(rows, limit, func(row sqlc.JobRun) uuid.UUID { return row.ID })

	data := make([]RunResponse, 0, len(rows))
	for _, row := range rows {
		data = append(data, runToResponse(row))
	}
	return c.JSON(http.StatusOK, ListJobRunsResponse{Data: data, NextCursor: nextCursor})
}

// listRuns returns a filtered page of runs across all jobs (newest run id first).
// Query: limit, cursor (same as /v1/job/list), optional job_id, status, trigger_source, event_type.
func (s *server) listRuns(c echo.Context) error {
	limit, cursor, err := ParseListCursor(c.Request())
	if err != nil {
		return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error(), nil)
	}
	params := sqlc.ListRunsPageParams{Limit: limit + 1}
	if cursor != uuid.Nil {
		params.Cursor = uuid.NullUUID{UUID: cursor, Valid: true}
	}
	if raw := strings.TrimSpace(c.QueryParam("job_id")); raw != "" {
		jid, err := uuid.Parse(raw)
		if err != nil {
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", "job_id must be a UUID", nil)
		}
		params.JobID = uuid.NullUUID{UUID: jid, Valid: true}
	}
	if raw := strings.TrimSpace(c.QueryParam("status")); raw != "" {
		if !isValidRunStatus(raw) {
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", "status must be a valid run status", nil)
		}
		params.Status = sql.NullString{String: raw, Valid: true}
	}
	if raw := strings.TrimSpace(c.QueryParam("trigger_source")); raw != "" {
		if !isValidTriggerSource(raw) {
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", "trigger_source must be MANUAL or SCHEDULED", nil)
		}
		params.TriggerSource = sql.NullString{String: raw, Valid: true}
	}
	if raw := strings.TrimSpace(c.QueryParam("event_type")); raw != "" {
		params.EventType = sql.NullString{String: raw, Valid: true}
	}

	rows, err := s.queries.ListRunsPage(c.Request().Context(), params)
	if err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list runs", nil)
	}
	rows, nextCursor := paginate(rows, limit, func(row sqlc.JobRun) uuid.UUID { return row.ID })

	data := make([]RunResponse, 0, len(rows))
	for _, row := range rows {
		data = append(data, runToResponse(row))
	}
	return c.JSON(http.StatusOK, ListJobRunsResponse{Data: data, NextCursor: nextCursor})
}

// cancelRun requests cancellation of a run by id. Queued-like runs become CANCELLED immediately;
// RUNNING becomes CANCEL_REQUESTED (202). Idempotent for CANCEL_REQUESTED; terminal states → 409.
//
// The run row is locked before the state switch, so the conditional update below
// sees the same status the switch did.
func (s *server) cancelRun(c echo.Context) error {
	runID, ok := pathUUID(c, "run_id")
	if !ok {
		return nil
	}
	var req CancelRunRequest
	if c.Request().Body != nil {
		dec := json.NewDecoder(c.Request().Body)
		if err := dec.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", "request body must be valid JSON", nil)
		}
	}
	var reason sql.NullString
	if req.Reason != nil && strings.TrimSpace(*req.Reason) != "" {
		reason = sql.NullString{String: strings.TrimSpace(*req.Reason), Valid: true}
	}
	now := time.Now().UTC()
	reqTime := sql.NullTime{Time: now, Valid: true}

	ctx := c.Request().Context()
	tx, qtx, err := s.beginTx(ctx)
	if err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to begin transaction", nil)
	}
	defer tx.Rollback()

	run, err := qtx.GetJobRunByIDForUpdate(ctx, runID)
	if errors.Is(err, sql.ErrNoRows) {
		return writeAPIError(c, http.StatusNotFound, "NOT_FOUND", "run not found", nil)
	}
	if err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load run", nil)
	}

	switch {
	case run.Status == RunStatusCancelRequested:
		if err := tx.Commit(); err != nil {
			return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to commit", nil)
		}
		return c.JSON(http.StatusOK, runToResponse(run))
	case isTerminalRunStatus(run.Status):
		return writeAPIError(c, http.StatusConflict, "RUN_ALREADY_TERMINAL", "run is already in a terminal state", map[string]any{
			"status": run.Status,
		})
	case run.Status == RunStatusRunning:
		updated, err := qtx.CancelRun(ctx, sqlc.CancelRunParams{
			ID: runID, CancelRequestedAt: reqTime, CancelReason: reason,
		})
		if err != nil {
			return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to request cancellation", nil)
		}
		if err := tx.Commit(); err != nil {
			return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to commit cancellation request", nil)
		}
		return c.JSON(http.StatusAccepted, runToResponse(updated))
	case isPreRunStatus(run.Status):
		updated, err := qtx.CancelRun(ctx, sqlc.CancelRunParams{
			ID: runID, CancelRequestedAt: reqTime, CancelReason: reason,
		})
		if err != nil {
			return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to cancel run", nil)
		}
		if err := tx.Commit(); err != nil {
			return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to cancel run", nil)
		}
		return c.JSON(http.StatusOK, runToResponse(updated))
	default:
		return writeAPIError(c, http.StatusConflict, "RUN_NOT_CANCELLABLE", "run cannot be cancelled in its current state", map[string]any{
			"status": run.Status,
		})
	}
}

func runToResponse(run sqlc.JobRun) RunResponse {
	return RunResponse{
		ID: run.ID.String(), JobID: run.JobID.String(), RetryOfRunID: nullUUIDPtr(run.RetryOfRunID),
		TriggerSource: run.TriggerSource, ScheduledFor: run.ScheduledFor,
		EventType: run.EventTypeSnapshot, Payload: run.PayloadSnapshot,
		TimeoutSeconds: run.TimeoutSeconds, Status: run.Status, AttemptCount: run.AttemptCount,
		MaxAttempts: run.MaxAttempts, WorkerID: nullStringPtr(run.WorkerID),
		LeaseExpiresAt: nullTimePtr(run.LeaseExpiresAt), HeartbeatAt: nullTimePtr(run.HeartbeatAt),
		RetryAt: nullTimePtr(run.RetryAt), DeadlineAt: nullTimePtr(run.DeadlineAt),
		StartedAt: nullTimePtr(run.StartedAt), LastAttemptStartedAt: nullTimePtr(run.LastAttemptStartedAt),
		FinishedAt: nullTimePtr(run.FinishedAt), CancelRequestedAt: nullTimePtr(run.CancelRequestedAt),
		CancelledAt: nullTimePtr(run.CancelledAt), CancelReason: nullStringPtr(run.CancelReason),
		LastError: nullStringPtr(run.LastError), ExitCode: nullInt32Ptr(run.ExitCode), Result: nullRawMessage(run.Result),
		CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt,
	}
}
