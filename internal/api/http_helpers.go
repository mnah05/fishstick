package api

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"
)

func writeJSON(c echo.Context, status int, body any) error {
	return c.JSON(status, body)
}

func writeAPIError(c echo.Context, status int, code, message string, details any) error {
	payload := map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": message,
		},
	}
	if details != nil {
		payload["error"].(map[string]any)["details"] = details
	}
	return writeJSON(c, status, payload)
}

func writeValidationError(c echo.Context, err error) error {
	var fields ValidationErrors
	if errors.As(err, &fields) {
		return writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", "invalid request fields", map[string]any{"fields": fields})
	}
	return writeAPIError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "internal server error", nil)
}
