// Job HTTP handlers, request/response DTOs, and mapping for /v1 job routes.
//
// Flow at a glance:
//   - Handlers decode HTTP, call sqlc queries through server, and write JSON.
//   - Schedule owns validation and next_run_at computation for create and update.
//   - buildCreateJobParams / MergeJobUpdate turn API bodies into sqlc rows;
//     jobToResponse maps rows back for JSON.
package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/fishstick/internal/database/sqlc"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

const (
	defaultListLimit = int32(50)
	maxListLimit     = int32(100)

	defaultMaxAttempts       int32 = 3
	defaultRetryDelaySeconds int32 = 30
	defaultTimeoutSeconds    int32 = 120
)

// --- Request DTOs (JSON in) ---------------------------------------------------
// Struct tags drive json decoding; validate tags are checked by Validate() in validate.go.

// CreateJobRequest is the JSON body for POST /v1/job/create.
type CreateJobRequest struct {
	Name              string          `json:"name" validate:"required,notblank"`
	EventType         string          `json:"event_type" validate:"required,notblank"`
	Payload           json.RawMessage `json:"payload"`
	ScheduleType      string          `json:"schedule_type" validate:"required"`
	ScheduledAt       *time.Time      `json:"scheduled_at,omitempty"`
	CronExpression    *string         `json:"cron_expression,omitempty"`
	Timezone          string          `json:"timezone,omitempty"`
	MaxAttempts       *int32          `json:"max_attempts,omitempty" validate:"omitempty,gt=0"`
	RetryDelaySeconds *int32          `json:"retry_delay_seconds,omitempty" validate:"omitempty,gte=0"`
	TimeoutSeconds    *int32          `json:"timeout_seconds,omitempty" validate:"omitempty,gt=0"`
}

// UpdateJobRequest is a partial body for POST /v1/job/update/{job_id}.
// Pointer fields mean "omit field = no change"; present fields replace stored values.
type UpdateJobRequest struct {
	Name              *string         `json:"name,omitempty" validate:"omitempty,notblank"`
	EventType         *string         `json:"event_type,omitempty" validate:"omitempty,notblank"`
	Payload           json.RawMessage `json:"payload,omitempty"`
	ScheduleType      *string         `json:"schedule_type,omitempty"`
	ScheduledAt       OptionalTime    `json:"scheduled_at"`
	CronExpression    *string         `json:"cron_expression,omitempty"`
	Timezone          *string         `json:"timezone,omitempty"`
	MaxAttempts       *int32          `json:"max_attempts,omitempty" validate:"omitempty,gt=0"`
	RetryDelaySeconds *int32          `json:"retry_delay_seconds,omitempty" validate:"omitempty,gte=0"`
	TimeoutSeconds    *int32          `json:"timeout_seconds,omitempty" validate:"omitempty,gt=0"`
}

// --- Response DTOs (JSON out) -------------------------------------------------
// Separate from sqlc.Job so API shape stays stable even if DB columns change.

// JobResponse is the job record returned by create, update, and nested in get/list.
type JobResponse struct {
	ID                string          `json:"id"`
	Name              string          `json:"name"`
	EventType         string          `json:"event_type"`
	Payload           json.RawMessage `json:"payload"`
	ScheduleType      string          `json:"schedule_type"`
	ScheduledAt       *time.Time      `json:"scheduled_at"`
	CronExpression    *string         `json:"cron_expression"`
	Timezone          string          `json:"timezone"`
	NextRunAt         *time.Time      `json:"next_run_at"`
	MaxAttempts       int32           `json:"max_attempts"`
	RetryDelaySeconds int32           `json:"retry_delay_seconds"`
	TimeoutSeconds    int32           `json:"timeout_seconds"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
}

// LatestRunSummary is the full latest-run block on GET /v1/job/get/{job_id}.
type LatestRunSummary struct {
	ID           string     `json:"id"`
	Status       string     `json:"status"`
	AttemptCount int32      `json:"attempt_count"`
	ScheduledFor time.Time  `json:"scheduled_for"`
	StartedAt    *time.Time `json:"started_at"`
	FinishedAt   *time.Time `json:"finished_at"`
	LastError    *string    `json:"last_error"`
}

// LatestRunBrief is a smaller latest-run snippet on list pages to avoid huge payloads.
type LatestRunBrief struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// GetJobResponse adds execution context (latest run + counts by status) on top of the job.
type GetJobResponse struct {
	JobResponse
	LatestRun *LatestRunSummary `json:"latest_run"`
	RunCounts map[string]int64  `json:"run_counts"`
}

// ListJobItem is one row in GET /v1/job/list.
type ListJobItem struct {
	JobResponse
	LatestRun *LatestRunBrief `json:"latest_run,omitempty"`
}

// ListJobsResponse wraps a page of jobs; NextCursor is set when more pages exist.
type ListJobsResponse struct {
	Data       []ListJobItem `json:"data"`
	NextCursor *string       `json:"next_cursor,omitempty"`
}

// createJob handles POST /v1/job/create.
// Order: decode -> struct validation -> schedule rules -> idempotency replay -> insert -> respond.
func (s *server) createJob(c echo.Context) error {
	var req CreateJobRequest
	if err := c.Bind(&req); err != nil {
		return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", "request body must be valid JSON", nil)
	}
	if err := c.Validate(&req); err != nil {
		return writeValidationError(c, err)
	}
	req.Name = strings.TrimSpace(req.Name)
	req.EventType = strings.TrimSpace(req.EventType)

	nextRun, err := ScheduleFromCreate(req).NextRun(time.Now().UTC())
	if err != nil {
		return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error(), nil)
	}
	payload, err := normalizePayload(req.Payload)
	if err != nil {
		return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error(), nil)
	}

	key, hasKey := idempotencyKeyFromHeader(c.Request())
	var hash string
	if hasKey {
		hash, err = requestHashSHA256(req)
		if err != nil {
			return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to hash request", nil)
		}
		if responded, err := s.replayCreatedJob(c, key, hash); responded {
			return err
		}
	}

	params, err := buildCreateJobParams(req, payload, nextRun, key, hash, hasKey)
	if err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to prepare job", nil)
	}

	job, err := s.queries.CreateJob(c.Request().Context(), params)
	if err != nil {
		if hasKey && isUniqueViolation(err) {
			if responded, replayErr := s.replayCreatedJob(c, key, hash); responded {
				return replayErr
			}
		}
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to create job", nil)
	}
	return c.JSON(http.StatusCreated, jobToResponse(job))
}

// updateJob handles POST /v1/job/update/{job_id}.
// Holds the job row lock through validation and update; active executions block edits.
// Worker claims must acquire the same job row lock before transitioning a run.
func (s *server) updateJob(c echo.Context) error {
	jobID, ok := pathUUID(c, "job_id")
	if !ok {
		return nil
	}

	var req UpdateJobRequest
	if err := c.Bind(&req); err != nil {
		return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", "request body must be valid JSON", nil)
	}
	if err := c.Validate(&req); err != nil {
		return writeValidationError(c, err)
	}

	ctx := c.Request().Context()
	tx, qtx, err := s.beginTx(ctx)
	if err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to begin transaction", nil)
	}
	defer tx.Rollback()

	existing, err := qtx.GetJobForUpdate(ctx, jobID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return writeAPIError(c, http.StatusNotFound, "NOT_FOUND", "job not found", nil)
		}
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load job", nil)
	}

	busy, err := qtx.JobHasBusyRun(ctx, jobID)
	if err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to check active runs", nil)
	}
	if busy {
		return writeAPIError(c, http.StatusConflict, "JOB_BUSY", "job has a running execution", nil)
	}

	merged, err := MergeJobUpdate(existing, req)
	if err != nil {
		return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error(), nil)
	}
	merged.NextRunAt, err = ScheduleFromJob(merged).NextRun(time.Now().UTC())
	if err != nil {
		return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error(), nil)
	}

	if err := qtx.UpdateJob(ctx, jobToUpdateParams(merged)); err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to update job", nil)
	}

	updated, err := qtx.GetJob(ctx, jobID)
	if err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load updated job", nil)
	}
	if err := tx.Commit(); err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to commit updated job", nil)
	}
	return c.JSON(http.StatusOK, jobToResponse(updated))
}

// listJobs handles GET /v1/job/list with cursor pagination and optional schedule_type / event_type filters.
func (s *server) listJobs(c echo.Context) error {
	limit, cursor, err := ParseListCursor(c.Request())
	if err != nil {
		return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error(), nil)
	}

	params := sqlc.ListJobsPageParams{Limit: limit + 1}
	if v := c.QueryParam("schedule_type"); v != "" {
		if !isValidScheduleType(v) {
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", "schedule_type must be MANUAL, ONCE, or CRON", nil)
		}
		params.ScheduleType = sql.NullString{String: v, Valid: true}
	}
	if v := c.QueryParam("event_type"); v != "" {
		params.EventType = sql.NullString{String: v, Valid: true}
	}
	if cursor != uuid.Nil {
		params.Cursor = uuid.NullUUID{UUID: cursor, Valid: true}
	}

	rows, err := s.queries.ListJobsPage(c.Request().Context(), params)
	if err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list jobs", nil)
	}
	rows, nextCursor := paginate(rows, limit, func(row sqlc.ListJobsPageRow) uuid.UUID { return row.ID })

	data := make([]ListJobItem, 0, len(rows))
	for _, row := range rows {
		data = append(data, listJobItemFromRow(row))
	}
	return c.JSON(http.StatusOK, ListJobsResponse{Data: data, NextCursor: nextCursor})
}

// getJob handles GET /v1/job/get/{job_id}: job row plus optional latest run and per-status run counts.
func (s *server) getJob(c echo.Context) error {
	jobID, ok := pathUUID(c, "job_id")
	if !ok {
		return nil
	}

	ctx := c.Request().Context()
	job, err := s.queries.GetJob(ctx, jobID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return writeAPIError(c, http.StatusNotFound, "NOT_FOUND", "job not found", nil)
		}
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load job", nil)
	}

	resp := GetJobResponse{
		JobResponse: jobToResponse(job),
		RunCounts:   map[string]int64{},
	}

	summary, err := s.queries.GetLatestJobRunSummary(ctx, jobID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load latest run", nil)
	}
	if err == nil {
		mapped := latestRunSummary(summary)
		resp.LatestRun = &mapped
	}

	counts, err := s.queries.JobRunStatusCounts(ctx, jobID)
	if err != nil {
		return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load run counts", nil)
	}
	for _, row := range counts {
		resp.RunCounts[row.Status] = row.Count
	}

	return c.JSON(http.StatusOK, resp)
}

// ParseListCursor reads limit (default 50, max 100) and optional cursor UUID from query strings.
func ParseListCursor(r *http.Request) (limit int32, cursor uuid.UUID, err error) {
	limit = defaultListLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, parseErr := strconv.ParseInt(raw, 10, 32)
		if parseErr != nil || parsed < 1 {
			return 0, uuid.Nil, errors.New("limit must be a positive integer")
		}
		limit = int32(parsed)
		if limit > maxListLimit {
			return 0, uuid.Nil, errors.New("limit must not exceed 100")
		}
	}
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		cursor, err = uuid.Parse(raw)
		if err != nil {
			return 0, uuid.Nil, errors.New("cursor must be a UUID")
		}
	}
	return limit, cursor, nil
}

// normalizePayload applies the default empty object and rejects invalid JSON.
func normalizePayload(payload json.RawMessage) (json.RawMessage, error) {
	if len(payload) == 0 {
		return json.RawMessage(`{}`), nil
	}
	if !json.Valid(payload) {
		return nil, fmt.Errorf("payload must be valid JSON")
	}
	return payload, nil
}

// buildCreateJobParams assigns server-side fields (UUIDv7 id, defaults, next_run_at)
// and optional idempotency columns before sqlc CreateJob runs.
func buildCreateJobParams(req CreateJobRequest, payload json.RawMessage, nextRun sql.NullTime, idempotencyKey, hash string, hasKey bool) (sqlc.CreateJobParams, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return sqlc.CreateJobParams{}, err
	}

	var idempotencyKeyNull, requestHashNull sql.NullString
	if hasKey {
		idempotencyKeyNull = sql.NullString{String: idempotencyKey, Valid: true}
		requestHashNull = sql.NullString{String: hash, Valid: true}
	}

	return sqlc.CreateJobParams{
		ID:                   id,
		Name:                 req.Name,
		EventType:            req.EventType,
		Payload:              payload,
		ScheduleType:         req.ScheduleType,
		ScheduledAt:          nullTime(req.ScheduledAt),
		CronExpression:       nullString(req.CronExpression),
		Timezone:             TimezoneOrDefault(req.Timezone),
		NextRunAt:            nextRun,
		MaxAttempts:          int32OrDefault(req.MaxAttempts, defaultMaxAttempts),
		RetryDelaySeconds:    int32OrDefault(req.RetryDelaySeconds, defaultRetryDelaySeconds),
		TimeoutSeconds:       int32OrDefault(req.TimeoutSeconds, defaultTimeoutSeconds),
		CreateIdempotencyKey: idempotencyKeyNull,
		CreateRequestHash:    requestHashNull,
	}, nil
}

// int32OrDefault applies server defaults for retry/timeout fields when the client omits them.
func int32OrDefault(value *int32, defaultValue int32) int32 {
	if value == nil {
		return defaultValue
	}
	return *value
}

// MergeJobUpdate overlays a PATCH-style body onto the stored job, including clearing schedule
// columns when switching to MANUAL or away from CRON.
func MergeJobUpdate(existing sqlc.Job, patch UpdateJobRequest) (sqlc.Job, error) {
	merged := existing
	if patch.Name != nil {
		merged.Name = strings.TrimSpace(*patch.Name)
	}
	if patch.EventType != nil {
		merged.EventType = strings.TrimSpace(*patch.EventType)
	}
	if len(patch.Payload) > 0 {
		payload, err := normalizePayload(patch.Payload)
		if err != nil {
			return sqlc.Job{}, err
		}
		merged.Payload = payload
	}
	if patch.ScheduleType != nil {
		merged.ScheduleType = *patch.ScheduleType
		switch merged.ScheduleType {
		case ScheduleManual:
			merged.ScheduledAt = sql.NullTime{}
			merged.CronExpression = sql.NullString{}
		case ScheduleOnce:
			merged.CronExpression = sql.NullString{}
		case ScheduleCron:
			merged.ScheduledAt = sql.NullTime{}
		}
	}
	if patch.ScheduledAt.IsSet() {
		if patch.ScheduledAt.Clears() {
			merged.ScheduledAt = sql.NullTime{}
		} else if t := patch.ScheduledAt.TimePtr(); t != nil {
			merged.ScheduledAt = nullTime(t)
		}
	}
	if patch.CronExpression != nil {
		merged.CronExpression = nullString(patch.CronExpression)
	}
	if patch.Timezone != nil {
		merged.Timezone = TimezoneOrDefault(*patch.Timezone)
	}
	if patch.MaxAttempts != nil {
		merged.MaxAttempts = *patch.MaxAttempts
	}
	if patch.RetryDelaySeconds != nil {
		merged.RetryDelaySeconds = *patch.RetryDelaySeconds
	}
	if patch.TimeoutSeconds != nil {
		merged.TimeoutSeconds = *patch.TimeoutSeconds
	}
	return merged, nil
}

// jobToUpdateParams copies the merged in-memory job into sqlc's UpdateJob argument struct.
// updated_at is owned by the jobs_set_updated_at trigger.
func jobToUpdateParams(job sqlc.Job) sqlc.UpdateJobParams {
	return sqlc.UpdateJobParams{
		ID:                job.ID,
		Name:              job.Name,
		EventType:         job.EventType,
		Payload:           job.Payload,
		ScheduleType:      job.ScheduleType,
		ScheduledAt:       job.ScheduledAt,
		CronExpression:    job.CronExpression,
		Timezone:          job.Timezone,
		NextRunAt:         job.NextRunAt,
		MaxAttempts:       job.MaxAttempts,
		RetryDelaySeconds: job.RetryDelaySeconds,
		TimeoutSeconds:    job.TimeoutSeconds,
	}
}

// jobToResponse maps a sqlc job row to API JSON (UUID as string, NULL columns as JSON null).
func jobToResponse(job sqlc.Job) JobResponse {
	return JobResponse{
		ID:                job.ID.String(),
		Name:              job.Name,
		EventType:         job.EventType,
		Payload:           job.Payload,
		ScheduleType:      job.ScheduleType,
		ScheduledAt:       nullTimePtr(job.ScheduledAt),
		CronExpression:    nullStringPtr(job.CronExpression),
		Timezone:          job.Timezone,
		NextRunAt:         nullTimePtr(job.NextRunAt),
		MaxAttempts:       job.MaxAttempts,
		RetryDelaySeconds: job.RetryDelaySeconds,
		TimeoutSeconds:    job.TimeoutSeconds,
		CreatedAt:         job.CreatedAt,
		UpdatedAt:         job.UpdatedAt,
	}
}

// latestRunSummary maps the get-job latest-run query row into GetJobResponse.LatestRun.
func latestRunSummary(row sqlc.GetLatestJobRunSummaryRow) LatestRunSummary {
	return LatestRunSummary{
		ID:           row.ID.String(),
		Status:       row.Status,
		AttemptCount: row.AttemptCount,
		ScheduledFor: row.ScheduledFor,
		StartedAt:    nullTimePtr(row.StartedAt),
		FinishedAt:   nullTimePtr(row.FinishedAt),
		LastError:    nullStringPtr(row.LastError),
	}
}

// listJobItemFromRow builds one list entry, attaching a brief latest run when the list query joined one.
func listJobItemFromRow(row sqlc.ListJobsPageRow) ListJobItem {
	item := ListJobItem{JobResponse: jobToResponse(listRowJob(row))}
	if row.LatestRunID.Valid {
		item.LatestRun = &LatestRunBrief{ID: row.LatestRunID.UUID.String(), Status: row.LatestRunStatus.String}
	}
	return item
}

// listRowJob adapts ListJobsPageRow to sqlc.Job so jobToResponse can stay the single mapper for job fields.
func listRowJob(row sqlc.ListJobsPageRow) sqlc.Job {
	return sqlc.Job{
		ID: row.ID, Name: row.Name, EventType: row.EventType, Payload: row.Payload,
		ScheduleType: row.ScheduleType, ScheduledAt: row.ScheduledAt, CronExpression: row.CronExpression,
		Timezone: row.Timezone, NextRunAt: row.NextRunAt, MaxAttempts: row.MaxAttempts,
		RetryDelaySeconds: row.RetryDelaySeconds, TimeoutSeconds: row.TimeoutSeconds,
		CreateIdempotencyKey: row.CreateIdempotencyKey, CreateRequestHash: row.CreateRequestHash,
		CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt,
	}
}
