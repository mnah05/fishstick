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

type cancelDisposition uint8

const (
	cancelUnsupported cancelDisposition = iota
	cancelImmediately
	cancelCooperatively
	cancelAlreadyRequested
	cancelTerminal
)

// runStatusCancellation is the API's single run-status registry. It drives
// query validation and the cancellation behavior for every persisted status.
var runStatusCancellation = map[string]cancelDisposition{
	RunStatusScheduled:       cancelImmediately,
	RunStatusQueued:          cancelImmediately,
	RunStatusRunning:         cancelCooperatively,
	RunStatusRetryWait:       cancelImmediately,
	RunStatusCancelRequested: cancelAlreadyRequested,
	RunStatusSucceeded:       cancelTerminal,
	RunStatusFailed:          cancelTerminal,
	RunStatusCancelled:       cancelTerminal,
}

func isValidRunStatus(value string) bool {
	_, ok := runStatusCancellation[value]
	return ok
}

func cancelDispositionFor(status string) cancelDisposition {
	if disposition, ok := runStatusCancellation[status]; ok {
		return disposition
	}
	return cancelUnsupported
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
