package instanceid

import (
	"fmt"
	"os"

	"github.com/google/uuid"
)

// New combines an operator-provided label with host and process-instance
// identity so concurrent replicas cannot share a lease owner accidentally.
func New(label string) string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	instance, err := uuid.NewV7()
	if err != nil {
		return fmt.Sprintf("%s-%s-%d", label, host, os.Getpid())
	}
	return fmt.Sprintf("%s-%s-%s", label, host, instance)
}
