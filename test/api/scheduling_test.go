package api_test

import (
	"github.com/fishstick/internal/api"
	"strings"
	"testing"
	"time"
)

func TestValidateSchedule(t *testing.T) {
	t.Parallel()
	future := time.Now().Add(time.Hour)
	cron := "0 * * * *"
	past := time.Now().Add(-time.Hour)

	for _, test := range []struct {
		name    string
		req     api.CreateJobRequest
		wantErr string
	}{
		{
			name: "manual ok",
			req:  api.CreateJobRequest{ScheduleType: api.ScheduleManual},
		},
		{
			name:    "manual rejects scheduled_at",
			req:     api.CreateJobRequest{ScheduleType: api.ScheduleManual, ScheduledAt: &future},
			wantErr: "scheduled_at must be omitted",
		},
		{
			name: "once ok",
			req:  api.CreateJobRequest{ScheduleType: api.ScheduleOnce, ScheduledAt: &future},
		},
		{
			name:    "once requires future scheduled_at",
			req:     api.CreateJobRequest{ScheduleType: api.ScheduleOnce, ScheduledAt: &past},
			wantErr: "future",
		},
		{
			name: "cron ok with default timezone",
			req:  api.CreateJobRequest{ScheduleType: api.ScheduleCron, CronExpression: &cron},
		},
		{
			name:    "cron requires expression",
			req:     api.CreateJobRequest{ScheduleType: api.ScheduleCron, Timezone: "America/New_York"},
			wantErr: "cron_expression is required",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := api.ValidateScheduleFields(api.ScheduleFieldsFromCreate(test.req))
			if test.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("err = %v, want substring %q", err, test.wantErr)
			}
		})
	}
}

func TestTimezoneOrDefault(t *testing.T) {
	t.Parallel()
	if got := api.TimezoneOrDefault(""); got != api.DefaultTimezone {
		t.Fatalf("got %q, want %q", got, api.DefaultTimezone)
	}
}
