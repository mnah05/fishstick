package api

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/fishstick/internal/database/sqlc"
	"github.com/robfig/cron/v3"
)

const (
	ScheduleManual = "MANUAL"
	ScheduleOnce   = "ONCE"
	ScheduleCron   = "CRON"

	DefaultTimezone = "UTC"
)

var validScheduleTypes = map[string]struct{}{
	ScheduleManual: {},
	ScheduleOnce:   {},
	ScheduleCron:   {},
}

func isValidScheduleType(value string) bool {
	_, ok := validScheduleTypes[value]
	return ok
}

// Schedule is the single place that knows how a schedule is validated and how
// its next run is computed. An empty Timezone means DefaultTimezone.
type Schedule struct {
	Type           string
	ScheduledAt    *time.Time
	CronExpression *string
	Timezone       string
}

// ScheduleFromCreate extracts the schedule from a create request.
func ScheduleFromCreate(req CreateJobRequest) Schedule {
	return Schedule{
		Type:           req.ScheduleType,
		ScheduledAt:    req.ScheduledAt,
		CronExpression: req.CronExpression,
		Timezone:       req.Timezone,
	}
}

// ScheduleFromJob extracts the schedule from a stored job.
func ScheduleFromJob(job sqlc.Job) Schedule {
	return Schedule{
		Type:           job.ScheduleType,
		ScheduledAt:    nullTimePtr(job.ScheduledAt),
		CronExpression: nullStringPtr(job.CronExpression),
		Timezone:       job.Timezone,
	}
}

var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// NextRun validates the schedule and returns the value for jobs.next_run_at.
// MANUAL returns an invalid NullTime.
func (s Schedule) NextRun(now time.Time) (sql.NullTime, error) {
	location, err := time.LoadLocation(TimezoneOrDefault(s.Timezone))
	if err != nil {
		return sql.NullTime{}, fmt.Errorf("timezone must be a valid IANA location (default %s)", DefaultTimezone)
	}

	switch s.Type {
	case ScheduleManual:
		if s.ScheduledAt != nil {
			return sql.NullTime{}, fmt.Errorf("scheduled_at must be omitted when schedule_type is MANUAL")
		}
		if !isBlank(s.CronExpression) {
			return sql.NullTime{}, fmt.Errorf("cron_expression must be omitted when schedule_type is MANUAL")
		}
		return sql.NullTime{}, nil
	case ScheduleOnce:
		if s.ScheduledAt == nil {
			return sql.NullTime{}, fmt.Errorf("scheduled_at is required when schedule_type is ONCE")
		}
		if !isBlank(s.CronExpression) {
			return sql.NullTime{}, fmt.Errorf("cron_expression must be omitted when schedule_type is ONCE")
		}
		if s.ScheduledAt.Before(now) {
			return sql.NullTime{}, fmt.Errorf("scheduled_at must be in the future for ONCE jobs")
		}
		return sql.NullTime{Time: *s.ScheduledAt, Valid: true}, nil
	case ScheduleCron:
		if s.ScheduledAt != nil {
			return sql.NullTime{}, fmt.Errorf("scheduled_at must be omitted when schedule_type is CRON")
		}
		if isBlank(s.CronExpression) {
			return sql.NullTime{}, fmt.Errorf("cron_expression is required when schedule_type is CRON")
		}
		parsed, err := cronParser.Parse(*s.CronExpression)
		if err != nil {
			return sql.NullTime{}, fmt.Errorf("invalid cron_expression")
		}
		next := parsed.Next(now.In(location))
		return sql.NullTime{Time: next.UTC(), Valid: true}, nil
	default:
		return sql.NullTime{}, fmt.Errorf("schedule_type must be MANUAL, ONCE, or CRON")
	}
}

// TimezoneOrDefault stores a non-empty IANA zone; empty input becomes UTC.
func TimezoneOrDefault(tz string) string {
	tz = strings.TrimSpace(tz)
	if tz == "" {
		return DefaultTimezone
	}
	return tz
}

func isBlank(value *string) bool {
	return value == nil || strings.TrimSpace(*value) == ""
}
