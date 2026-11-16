package api

import "github.com/fishstick/internal/runstatus"

type cancelDisposition uint8

const (
	cancelUnsupported cancelDisposition = iota
	cancelImmediately
	cancelCooperatively
	cancelAlreadyRequested
	cancelTerminal
)

// runStatusCancellation assigns API cancellation behavior to every status in
// the authoritative internal/runstatus package.
var runStatusCancellation = map[string]cancelDisposition{
	runstatus.Queued:          cancelImmediately,
	runstatus.Running:         cancelCooperatively,
	runstatus.RetryWait:       cancelImmediately,
	runstatus.CancelRequested: cancelAlreadyRequested,
	runstatus.Succeeded:       cancelTerminal,
	runstatus.Failed:          cancelTerminal,
	runstatus.Cancelled:       cancelTerminal,
}

func isValidRunStatus(value string) bool {
	return runstatus.Valid(value)
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
