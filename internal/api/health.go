package api

import (
	"context"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
)

// DatabaseHealth runs sqlc HealthCheck (SELECT 1) per request with a 3s deadline.
func DatabaseHealth(healthCheck func(context.Context) (int32, error)) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx, cancel := context.WithTimeout(c.Request().Context(), 3*time.Second)
		defer cancel()

		c.Response().Header().Set("Cache-Control", "no-store")
		var healthy int32
		var err error
		if healthCheck != nil {
			healthy, err = healthCheck(ctx)
		}
		if err != nil || healthy != 1 {
			return c.JSON(http.StatusServiceUnavailable, map[string]any{
				"error": map[string]string{
					"code":    "DATABASE_UNAVAILABLE",
					"message": "database health check failed",
				},
			})
		}
		return c.JSON(http.StatusOK, map[string]string{
			"status": "ok", "database": "connected",
		})
	}
}
