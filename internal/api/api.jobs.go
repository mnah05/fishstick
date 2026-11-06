// Job HTTP handlers, request/response DTOs, scheduling, and mapping for /v1 job routes.
//
// Flow at a glance:
//   - Handlers (createJob, updateJob, listJobs, getJob) decode HTTP, call *sqlc.Queries, write JSON.
//   - ValidateScheduleFields + computeNextRunAt keep schedule rules and next_run_at in sync with the DB.
//   - buildCreateJobParams / MergeJobUpdate turn API bodies into sqlc rows; jobToResponse maps rows back for JSON.
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
	"github.com/robfig/cron/v3"
)

const (
	defaultListLimit               = int32(50)
	maxListLimit                   = int32(100)
	defaultMaxAttempts       int32 = 3
	defaultRetryDelaySeconds int32 = 30
	defaultTimeoutSeconds    int32 = 120

	ScheduleManual  = "MANUAL"
	ScheduleOnce    = "ONCE"
	ScheduleCron    = "CRON"
	DefaultTimezone = "UTC"
)

// --- Request DTOs (JSON in) ---------------------------------------------------
// Struct tags drive json decoding; validate tags are checked by Validate() in validate.go.

// CreateJobRequest is the JSON body for POST /v1/job/create.
type CreateJobRequest struct {
	Name              string          `json:"name" validate:"required,notblank"`
	EventType         string          `json:"event_type" validate:"required,notblank"`
	Payload           json.RawMessage `json:"payload"`
	ScheduleType      string          `json:"schedule_type" validate:"required,oneof=MANUAL ONCE CRON"`
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
	ScheduleType      *string         `json:"schedule_type,omitempty" validate:"omitempty,oneof=MANUAL ONCE CRON"`
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

// ScheduleFields is a shared view of schedule-related inputs used by both
// ValidateScheduleFields and computeNextRunAt so create and update share one rule set.
type ScheduleFields struct {
	ScheduleType   string
	ScheduledAt    *time.Time
	CronExpression *string
	Timezone       string
	NextRunAt      *time.Time
}

// ScheduleFieldsFromCreate maps a create request into ScheduleFields for validation and next_run_at.
func ScheduleFieldsFromCreate(req CreateJobRequest) ScheduleFields {
	return ScheduleFields{
		ScheduleType:   req.ScheduleType,
		ScheduledAt:    req.ScheduledAt,
		CronExpression: req.CronExpression,
		Timezone:       req.Timezone,
	}
}

// ValidateScheduleFields enforces cross-field rules the JSON schema cannot express
// (for example CRON requires cron_expression but must not include scheduled_at).
func ValidateScheduleFields(s ScheduleFields) error {
	tz := strings.TrimSpace(s.Timezone)
	if tz == "" {
		tz = DefaultTimezone
	}
	if _, err := time.LoadLocation(tz); err != nil {
		return fmt.Errorf("timezone must be a valid IANA location (default %s)", DefaultTimezone)
	}

	switch s.ScheduleType {
	case ScheduleManual:
		if s.ScheduledAt != nil {
			return fmt.Errorf("scheduled_at must be omitted when schedule_type is MANUAL")
		}
		if s.CronExpression != nil && strings.TrimSpace(*s.CronExpression) != "" {
			return fmt.Errorf("cron_expression must be omitted when schedule_type is MANUAL")
		}
		if s.NextRunAt != nil {
			return fmt.Errorf("next_run_at must be omitted when schedule_type is MANUAL")
		}
	case ScheduleOnce:
		if s.ScheduledAt == nil {
			return fmt.Errorf("scheduled_at is required when schedule_type is ONCE")
		}
		if s.CronExpression != nil && strings.TrimSpace(*s.CronExpression) != "" {
			return fmt.Errorf("cron_expression must be omitted when schedule_type is ONCE")
		}
		if s.ScheduledAt.Before(time.Now()) {
			return fmt.Errorf("scheduled_at must be in the future for ONCE jobs")
		}
	case ScheduleCron:
		if s.ScheduledAt != nil {
			return fmt.Errorf("scheduled_at must be omitted when schedule_type is CRON")
		}
		if s.CronExpression == nil || strings.TrimSpace(*s.CronExpression) == "" {
			return fmt.Errorf("cron_expression is required when schedule_type is CRON")
		}
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
		if _, err := parser.Parse(*s.CronExpression); err != nil {
			return fmt.Errorf("invalid cron_expression")
		}
	default:
		return fmt.Errorf("schedule_type must be MANUAL, ONCE, or CRON")
	}
	return nil
}

// TimezoneOrDefault stores a non-empty IANA zone on the job row; empty client input becomes UTC.
func TimezoneOrDefault(tz string) string {
	tz = strings.TrimSpace(tz)
	if tz == "" {
		return DefaultTimezone
	}
	return tz
}

// computeNextRunAt derives jobs.next_run_at for the scheduler worker: NULL for MANUAL,
// scheduled_at for ONCE, and the next cron tick (in job timezone, stored UTC) for CRON.
func computeNextRunAt(spec ScheduleFields, now time.Time) (sql.NullTime, error) {
	switch spec.ScheduleType {
	case ScheduleManual:
		return sql.NullTime{}, nil
	case ScheduleOnce:
		if spec.ScheduledAt == nil {
			return sql.NullTime{}, fmt.Errorf("scheduled_at is required when schedule_type is ONCE")
		}
		return sql.NullTime{Time: *spec.ScheduledAt, Valid: true}, nil
	case ScheduleCron:
		if spec.CronExpression == nil || strings.TrimSpace(*spec.CronExpression) == "" {
			return sql.NullTime{}, fmt.Errorf("cron_expression is required when schedule_type is CRON")
		}
		loc, err := time.LoadLocation(TimezoneOrDefault(spec.Timezone))
		if err != nil {
			return sql.NullTime{}, fmt.Errorf("timezone must be a valid IANA location")
		}
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
		sched, err := parser.Parse(*spec.CronExpression)
		if err != nil {
			return sql.NullTime{}, fmt.Errorf("invalid cron_expression")
		}
		next := sched.Next(now.In(loc))
		return sql.NullTime{Time: next.UTC(), Valid: true}, nil
	default:
		return sql.NullTime{}, fmt.Errorf("schedule_type must be MANUAL, ONCE, or CRON")
	}
}

// int32OrDefault applies server defaults for retry/timeout fields when the client omits them.
func int32OrDefault(value *int32, defaultValue int32) int32 {
	if value == nil {
		return defaultValue
	}
	return *value
}

// createJob handles POST /v1/job/create.
// Order: decode -> struct validation -> schedule rules -> idempotency replay -> insert -> respond.
func createJob(queries *sqlc.Queries) echo.HandlerFunc {
	return func(c echo.Context) error {
		var req CreateJobRequest
		if err := c.Bind(&req); err != nil {
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", "request body must be valid JSON", nil)
		}
		if err := c.Validate(&req); err != nil {
			return writeValidationError(c, err)
		}
		req.Name = strings.TrimSpace(req.Name)
		req.EventType = strings.TrimSpace(req.EventType)
		if err := ValidateScheduleFields(ScheduleFieldsFromCreate(req)); err != nil {
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error(), nil)
		}

		hash, err := requestHashSHA256(req)
		if err != nil {
			return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to hash request", nil)
		}

		ctx := c.Request().Context()
		key, hasKey := idempotencyKeyFromHeader(c.Request())
		if hasKey {
			if job, ok, replayErr := replayCreateJobIfExists(ctx, queries, key, hash); replayErr != nil {
				return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to lookup idempotency key", nil)
			} else if ok {
				if job == nil {
					return writeAPIError(c, http.StatusConflict, "IDEMPOTENCY_KEY_REUSED", "idempotency key was already used with a different request body", nil)
				}
				return writeJSON(c, http.StatusOK, jobToResponse(*job))
			}
		}

		params, err := buildCreateJobParams(req, hash, key, hasKey)
		if err != nil {
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error(), nil)
		}

		if err := queries.CreateJob(ctx, params); err != nil {
			if hasKey && isUniqueViolation(err) {
				if job, ok, replayErr := replayCreateJobIfExists(ctx, queries, key, hash); replayErr != nil {
					return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load job after idempotency conflict", nil)
				} else if ok {
					if job == nil {
						return writeAPIError(c, http.StatusConflict, "IDEMPOTENCY_KEY_REUSED", "idempotency key was already used with a different request body", nil)
					}
					return writeJSON(c, http.StatusOK, jobToResponse(*job))
				}
			}
			return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to create job", nil)
		}

		job, err := queries.GetJob(ctx, params.ID)
		if err != nil {
			return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load created job", nil)
		}
		return writeJSON(c, http.StatusCreated, jobToResponse(job))
	}
}

// updateJob handles POST /v1/job/update/{job_id}.
// Holds the job row lock through validation and update; active executions block edits.
// Worker claims must acquire the same job row lock before transitioning a run.
func updateJob(db *sql.DB, queries *sqlc.Queries) echo.HandlerFunc {
	return func(c echo.Context) error {
		jobID, err := uuid.Parse(c.Param("job_id"))
		if err != nil {
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", "job_id must be a UUID", nil)
		}

		var req UpdateJobRequest
		if err := c.Bind(&req); err != nil {
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", "request body must be valid JSON", nil)
		}
		if err := c.Validate(&req); err != nil {
			return writeValidationError(c, err)
		}

		ctx := c.Request().Context()
		tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to begin transaction", nil)
		}
		defer tx.Rollback()
		txQueries := queries.WithTx(tx)
		existing, err := txQueries.GetJobForUpdate(ctx, jobID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return writeAPIError(c, http.StatusNotFound, "NOT_FOUND", "job not found", nil)
			}
			return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load job", nil)
		}

		busy, err := txQueries.JobHasBusyRun(ctx, jobID)
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
		if err := ValidateScheduleFields(ScheduleFieldsFromJob(merged)); err != nil {
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error(), nil)
		}

		nextRun, err := computeNextRunAt(ScheduleFieldsFromJob(merged), time.Now().UTC())
		if err != nil {
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error(), nil)
		}
		merged.NextRunAt = nextRun
		merged.UpdatedAt = time.Now().UTC()

		rows, err := txQueries.UpdateJob(ctx, jobToUpdateParams(merged))
		if err != nil {
			return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to update job", nil)
		}
		if rows == 0 {
			return writeAPIError(c, http.StatusNotFound, "NOT_FOUND", "job not found", nil)
		}

		updated, err := txQueries.GetJob(ctx, jobID)
		if err != nil {
			return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load updated job", nil)
		}
		if err := tx.Commit(); err != nil {
			return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to commit updated job", nil)
		}
		return writeJSON(c, http.StatusOK, jobToResponse(updated))
	}
}

// listJobs handles GET /v1/job/list with cursor pagination and optional schedule_type / event_type filters.
func listJobs(queries *sqlc.Queries) echo.HandlerFunc {
	return func(c echo.Context) error {
		limit, cursor, err := ParseListCursor(c.Request())
		if err != nil {
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error(), nil)
		}

		params := sqlc.ListJobsPageParams{Limit: limit + 1}
		if v := c.QueryParam("schedule_type"); v != "" {
			if v != ScheduleManual && v != ScheduleOnce && v != ScheduleCron {
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

		rows, err := queries.ListJobsPage(c.Request().Context(), params)
		if err != nil {
			return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list jobs", nil)
		}

		hasMore := len(rows) > int(limit)
		if hasMore {
			rows = rows[:limit]
		}

		data := make([]ListJobItem, 0, len(rows))
		for _, row := range rows {
			data = append(data, listJobItemFromRow(row))
		}

		var nextCursor *string
		if hasMore && len(rows) > 0 {
			nextCursor = nextCursorFromID(rows[len(rows)-1].ID)
		}

		return writeJSON(c, http.StatusOK, ListJobsResponse{Data: data, NextCursor: nextCursor})
	}
}

// getJob handles GET /v1/job/get/{job_id}: job row plus optional latest run and per-status run counts.
func getJob(queries *sqlc.Queries) echo.HandlerFunc {
	return func(c echo.Context) error {
		jobID, err := uuid.Parse(c.Param("job_id"))
		if err != nil {
			return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", "job_id must be a UUID", nil)
		}

		ctx := c.Request().Context()
		job, err := queries.GetJob(ctx, jobID)
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

		summary, err := queries.GetLatestJobRunSummary(ctx, jobID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load latest run", nil)
		}
		if err == nil {
			mapped := latestRunSummary(summary)
			resp.LatestRun = &mapped
		}

		counts, err := queries.JobRunStatusCounts(ctx, jobID)
		if err != nil {
			return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load run counts", nil)
		}
		for _, row := range counts {
			resp.RunCounts[row.Status] = row.Count
		}

		return writeJSON(c, http.StatusOK, resp)
	}
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

// buildCreateJobParams assigns server-side fields (UUIDv7 id, timestamps, defaults, next_run_at)
// and optional idempotency columns before sqlc CreateJob runs.
func buildCreateJobParams(req CreateJobRequest, hash string, idempotencyKey string, hasKey bool) (sqlc.CreateJobParams, error) {
	payload := req.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	if !json.Valid(payload) {
		return sqlc.CreateJobParams{}, fmt.Errorf("payload must be valid JSON")
	}

	id, err := uuid.NewV7()
	if err != nil {
		return sqlc.CreateJobParams{}, err
	}
	now := time.Now().UTC()
	spec := ScheduleFieldsFromCreate(req)
	nextRun, err := computeNextRunAt(spec, now)
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
		CreatedAt:            now,
		UpdatedAt:            now,
	}, nil
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
		if !json.Valid(patch.Payload) {
			return sqlc.Job{}, fmt.Errorf("payload must be valid JSON")
		}
		merged.Payload = patch.Payload
	}
	if patch.ScheduleType != nil {
		merged.ScheduleType = *patch.ScheduleType
		switch *patch.ScheduleType {
		case ScheduleManual:
			merged.ScheduledAt = sql.NullTime{}
			merged.CronExpression = sql.NullString{}
			merged.NextRunAt = sql.NullTime{}
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
	} else if patch.ScheduleType != nil && *patch.ScheduleType != ScheduleCron {
		merged.CronExpression = sql.NullString{}
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
		UpdatedAt:         job.UpdatedAt,
	}
}

// ScheduleFieldsFromJob converts DB null types to pointers so the same validators run on update as on create.
func ScheduleFieldsFromJob(job sqlc.Job) ScheduleFields {
	return ScheduleFields{
		ScheduleType:   job.ScheduleType,
		ScheduledAt:    nullTimePtr(job.ScheduledAt),
		CronExpression: nullStringPtr(job.CronExpression),
		Timezone:       job.Timezone,
		NextRunAt:      nullTimePtr(job.NextRunAt),
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

// nullTimePtr exposes sql.NullTime as JSON-friendly *time.Time (nil means SQL NULL).
func nullTimePtr(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	t := value.Time
	return &t
}

// nullStringPtr exposes sql.NullString as *string for optional JSON fields.
func nullStringPtr(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	s := value.String
	return &s
}

// nextCursorFromID returns the last job id on a full page; clients pass it as cursor for the next page.
func nextCursorFromID(id uuid.UUID) *string {
	s := id.String()
	return &s
}

// nullTime stores an optional API time pointer into sql.NullTime for writes.
func nullTime(value *time.Time) sql.NullTime {
	if value == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: *value, Valid: true}
}

// nullString stores an optional API string pointer into sql.NullString for writes.
func nullString(value *string) sql.NullString {
	if value == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: *value, Valid: true}
}
