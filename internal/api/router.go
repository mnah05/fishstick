package api

import (
	"context"
	"database/sql"

	"github.com/fishstick/internal/database/sqlc"
	"github.com/labstack/echo/v4"
)

// NewRouter registers job API routes under /v1 (production middleware: CORS, logging, etc.).
func NewRouter(db *sql.DB) *echo.Echo {
	return newRouter(db, false)
}

// NewTestRouter is the same routes with minimal Echo middleware for tests.
func NewTestRouter(db *sql.DB) *echo.Echo {
	return newRouter(db, true)
}

func newRouter(db *sql.DB, testMode bool) *echo.Echo {
	queries := sqlc.New(db)
	e := echo.New()
	if testMode {
		configureEchoForTest(e)
	} else {
		configureEcho(e)
	}

	v1 := e.Group("/v1")
	var healthCheck func(context.Context) (int32, error)
	if db != nil {
		healthCheck = queries.HealthCheck
	}
	v1.GET("/health/db", DatabaseHealth(healthCheck))

	v1.POST("/job/create", createJob(queries))
	v1.GET("/list_runs", listRuns(queries))
	v1.GET("/job/get/:job_id", getJob(queries))
	v1.GET("/job/list", listJobs(queries))
	v1.GET("/job/:job_id/runs", listJobRuns(queries))
	v1.GET("/job/:job_id/run/:run_id", getJobRun(queries))
	v1.POST("/cancel_run/:run_id", cancelRun(db, queries))
	v1.POST("/job/update/:job_id", updateJob(db, queries))
	v1.POST("/job/trigger/:job_id", triggerJob(db, queries))

	return e
}
