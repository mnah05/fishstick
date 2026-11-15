// Package schedule owns cron syntax and the next-run calculation shared by the
// API and the scheduler. Callers still decide what a schedule type is allowed
// to store.
package schedule

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// Parser accepts the five-field syntax and descriptors stored on jobs.
var Parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// NextCron returns the first occurrence strictly after now, evaluated in
// timezone and stored as UTC. Ticks at or before now are skipped.
func NextCron(expression, timezone string, now time.Time) (time.Time, error) {
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return time.Time{}, fmt.Errorf("load timezone %q: %w", timezone, err)
	}
	parsed, err := Parser.Parse(expression)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse cron expression: %w", err)
	}
	return parsed.Next(now.In(location)).UTC(), nil
}
