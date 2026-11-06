package api

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/fishstick/internal/database/sqlc"
)

const idempotencyHeader = "Idempotency-Key"

// requestHashSHA256 returns a stable hex digest of JSON request bytes for idempotency pairing.
func requestHashSHA256(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func idempotencyKeyFromHeader(r *http.Request) (string, bool) {
	key := strings.TrimSpace(r.Header.Get(idempotencyHeader))
	if key == "" {
		return "", false
	}
	return key, true
}

// replayCreateJobIfExists handles idempotency lookup after a miss or unique-key race.
// When ok is false, the key is unused. When ok is true and job is nil, the key was reused with another body.
func replayCreateJobIfExists(ctx context.Context, queries *sqlc.Queries, key, hash string) (job *sqlc.Job, ok bool, err error) {
	existing, err := queries.GetJobByCreateIdempotencyKey(ctx, sql.NullString{String: key, Valid: true})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if existing.CreateRequestHash.Valid && existing.CreateRequestHash.String != hash {
		return nil, true, nil
	}
	return &existing, true, nil
}
