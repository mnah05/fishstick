package messaging

import (
	"encoding/json"
	"errors"

	"github.com/google/uuid"
)

// DispatchMessage is the durable JOB_DISPATCH payload shared by producers and
// the worker consumer.
type DispatchMessage struct {
	RunID uuid.UUID `json:"run_id"`
}

func EncodeDispatch(runID uuid.UUID) ([]byte, error) {
	if runID == uuid.Nil {
		return nil, errors.New("dispatch run ID is required")
	}
	return json.Marshal(DispatchMessage{RunID: runID})
}

func DecodeDispatch(body []byte) (uuid.UUID, error) {
	var message DispatchMessage
	if err := json.Unmarshal(body, &message); err != nil {
		return uuid.Nil, err
	}
	if message.RunID == uuid.Nil {
		return uuid.Nil, errors.New("dispatch run ID is required")
	}
	return message.RunID, nil
}
