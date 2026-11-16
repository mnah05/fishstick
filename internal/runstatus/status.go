// Package runstatus defines persisted job run statuses shared by application
// validation and lifecycle code. SQL literals are checked by sqlc drift tests.
package runstatus

const (
	Queued          = "QUEUED"
	Running         = "RUNNING"
	RetryWait       = "RETRY_WAIT"
	CancelRequested = "CANCEL_REQUESTED"
	Succeeded       = "SUCCEEDED"
	Failed          = "FAILED"
	Cancelled       = "CANCELLED"
)

func All() []string {
	return []string{Queued, Running, RetryWait, CancelRequested, Succeeded, Failed, Cancelled}
}

func Valid(status string) bool {
	for _, value := range All() {
		if status == value {
			return true
		}
	}
	return false
}

func Terminal(status string) bool {
	return status == Succeeded || status == Failed || status == Cancelled
}

// Claimable reports statuses that may be claimed. RUNNING requires an expired
// lease; SQL checks that lease timestamp before allowing a reclaim.
func Claimable(status string) bool {
	return status == Queued || status == RetryWait || status == Running
}
