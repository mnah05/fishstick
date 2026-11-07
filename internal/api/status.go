package api

const (
	RunStatusScheduled       = "SCHEDULED"
	RunStatusQueued          = "QUEUED"
	RunStatusRunning         = "RUNNING"
	RunStatusRetryWait       = "RETRY_WAIT"
	RunStatusCancelRequested = "CANCEL_REQUESTED"
	RunStatusSucceeded       = "SUCCEEDED"
	RunStatusFailed          = "FAILED"
	RunStatusCancelled       = "CANCELLED"
)

var validRunStatuses = map[string]struct{}{
	RunStatusScheduled:       {},
	RunStatusQueued:          {},
	RunStatusRunning:         {},
	RunStatusRetryWait:       {},
	RunStatusCancelRequested: {},
	RunStatusSucceeded:       {},
	RunStatusFailed:          {},
	RunStatusCancelled:       {},
}

func isValidRunStatus(value string) bool {
	_, ok := validRunStatuses[value]
	return ok
}

func isTerminalRunStatus(value string) bool {
	switch value {
	case RunStatusSucceeded, RunStatusFailed, RunStatusCancelled:
		return true
	default:
		return false
	}
}

// isPreRunStatus reports states a worker has not claimed yet, where cancel can
// move straight to CANCELLED.
func isPreRunStatus(value string) bool {
	switch value {
	case RunStatusQueued, RunStatusScheduled, RunStatusRetryWait:
		return true
	default:
		return false
	}
}

const (
	TriggerSourceManual    = "MANUAL"
	TriggerSourceScheduled = "SCHEDULED"
)

var validTriggerSources = map[string]struct{}{
	TriggerSourceManual:    {},
	TriggerSourceScheduled: {},
}

func isValidTriggerSource(value string) bool {
	_, ok := validTriggerSources[value]
	return ok
}
