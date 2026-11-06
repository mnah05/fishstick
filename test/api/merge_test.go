package api_test

import (
	"database/sql"
	"encoding/json"
	"github.com/fishstick/internal/api"
	"testing"
	"time"

	"github.com/fishstick/internal/database/sqlc"
	"github.com/google/uuid"
)

func TestMergeJobUpdateOnceToCronClearsScheduledAt(t *testing.T) {
	t.Parallel()
	when := time.Now().Add(time.Hour)
	existing := sqlc.Job{
		ID:           uuid.New(),
		ScheduleType: api.ScheduleOnce,
		ScheduledAt:  sql.NullTime{Time: when, Valid: true},
		Timezone:     api.DefaultTimezone,
	}
	cron := "0 * * * *"
	merged, err := api.MergeJobUpdate(existing, api.UpdateJobRequest{
		ScheduleType:   strPtr(api.ScheduleCron),
		CronExpression: &cron,
	})
	if err != nil {
		t.Fatal(err)
	}
	if merged.ScheduledAt.Valid {
		t.Fatalf("scheduled_at should be cleared, got %v", merged.ScheduledAt)
	}
	if err := api.ValidateScheduleFields(api.ScheduleFieldsFromJob(merged)); err != nil {
		t.Fatal(err)
	}
}

func TestMergeJobUpdateExplicitNullScheduledAt(t *testing.T) {
	t.Parallel()
	when := time.Now().Add(time.Hour)
	existing := sqlc.Job{
		ID:           uuid.New(),
		ScheduleType: api.ScheduleOnce,
		ScheduledAt:  sql.NullTime{Time: when, Valid: true},
		Timezone:     api.DefaultTimezone,
	}
	var patch api.UpdateJobRequest
	if err := json.Unmarshal([]byte(`{"scheduled_at":null}`), &patch); err != nil {
		t.Fatal(err)
	}
	merged, err := api.MergeJobUpdate(existing, patch)
	if err != nil {
		t.Fatal(err)
	}
	if merged.ScheduledAt.Valid {
		t.Fatal("expected scheduled_at cleared")
	}
}

func TestValidateScheduleRejectsInvalidTimezoneForManual(t *testing.T) {
	t.Parallel()
	err := api.ValidateScheduleFields(api.ScheduleFields{ScheduleType: api.ScheduleManual, Timezone: "Not/A_Zone"})
	if err == nil {
		t.Fatal("expected timezone error")
	}
}

func strPtr(s string) *string { return &s }
