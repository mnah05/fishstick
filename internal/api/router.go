package api

import (
	"context"
	"database/sql"

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
	s := newServer(db)
	e := newEcho(testMode)

	v1 := e.Group("/v1")
	var healthCheck func(context.Context) (int32, error)
	if s.db != nil {
		healthCheck = s.queries.HealthCheck
	}
	v1.GET("/health/db", DatabaseHealth(healthCheck))

	v1.POST("/job/create", s.createJob)
	v1.GET("/list_runs", s.listRuns)
	v1.GET("/job/get/:job_id", s.getJob)
	v1.GET("/job/list", s.listJobs)
	v1.GET("/job/:job_id/runs", s.listJobRuns)
	v1.GET("/job/:job_id/run/:run_id", s.getJobRun)
	v1.POST("/cancel_run/:run_id", s.cancelRun)
	v1.POST("/job/update/:job_id", s.updateJob)
	v1.POST("/job/trigger/:job_id", s.triggerJob)

	return e
}
