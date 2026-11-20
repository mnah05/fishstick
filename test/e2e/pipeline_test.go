package e2e_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type jobResponse struct {
	ID string `json:"id"`
}
type run struct {
	ID            string          `json:"id"`
	Status        string          `json:"status"`
	TriggerSource string          `json:"trigger_source"`
	AttemptCount  int32           `json:"attempt_count"`
	StartedAt     *time.Time      `json:"started_at"`
	FinishedAt    *time.Time      `json:"finished_at"`
	Result        json.RawMessage `json:"result"`
}
type runPage struct {
	Data []run `json:"data"`
}

func TestManualRunPublishesAndCompletes(t *testing.T)       { runPipeline(t, "MANUAL") }
func TestScheduledOnceRunPublishesExactlyOnce(t *testing.T) { runPipeline(t, "ONCE") }

func TestScheduledBatchAcrossSchedulerReplicas(t *testing.T) {
	apiURL, databaseURL := endpoints(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 5 * time.Second}
	db := openDatabase(t, databaseURL)
	defer db.Close()
	const jobCount = 12
	due := time.Now().Add(5 * time.Second).UTC()
	jobIDs := make([]string, jobCount)
	started := time.Now()
	for i := range jobIDs {
		jobIDs[i] = createJob(t, ctx, client, apiURL, "ONCE", &due)
	}
	totalAttempts, totalDispatches := 0, 0
	for _, jobID := range jobIDs {
		row := waitForRun(t, ctx, db, client, apiURL, jobID)
		if row.TriggerSource != "SCHEDULED" || row.Status != "SUCCEEDED" || row.AttemptCount != 1 {
			t.Fatalf("scheduled run state: %+v", row)
		}
		status, source, outbox, attempts, retry, events := inspectRun(t, ctx, db, row.ID)
		if status != "SUCCEEDED" || source != "SCHEDULED" || outbox != "PUBLISHED" || attempts != 1 || retry != 13 || events != 1 {
			t.Fatalf("scheduled snapshot status=%s source=%s outbox=%s attempts=%d retry=%d events=%d", status, source, outbox, attempts, retry, events)
		}
		var occurrences int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM job_runs WHERE job_id=$1 AND trigger_source='SCHEDULED'`, jobID).Scan(&occurrences); err != nil {
			t.Fatal(err)
		}
		if occurrences != 1 {
			t.Fatalf("job %s has %d scheduled occurrences, want 1", jobID, occurrences)
		}
		totalAttempts += int(attempts)
		totalDispatches += events
	}
	if totalAttempts != jobCount || totalDispatches != jobCount {
		t.Fatalf("scheduled totals jobs=%d attempts=%d dispatches=%d", jobCount, totalAttempts, totalDispatches)
	}
	t.Logf("scheduler_replicas=3 due_jobs=%d unique_runs=%d published_dispatches=%d total_attempts=%d elapsed=%s", jobCount, jobCount, totalDispatches, totalAttempts, time.Since(started).Round(time.Millisecond))
}

func runPipeline(t *testing.T, scheduleType string) {
	t.Helper()
	apiURL, databaseURL := endpoints(t)
	ctx, cancel := context.WithTimeout(context.Background(), e2eTimeout(t))
	defer cancel()
	client := &http.Client{Timeout: 5 * time.Second}
	started := time.Now()
	var scheduledAt *time.Time
	if scheduleType == "ONCE" {
		future := time.Now().Add(5 * time.Second).UTC()
		scheduledAt = &future
	}
	jobID := createJob(t, ctx, client, apiURL, scheduleType, scheduledAt)
	if scheduleType == "MANUAL" {
		resp := request(t, ctx, client, http.MethodPost, apiURL+"/v1/job/trigger/"+jobID, nil, uuid.NewString())
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("trigger HTTP %d: %s", resp.StatusCode, readBody(t, resp))
		}
		resp.Body.Close()
	}
	db := openDatabase(t, databaseURL)
	defer db.Close()
	completed := waitForRun(t, ctx, db, client, apiURL, jobID)
	wantSource := "MANUAL"
	if scheduleType == "ONCE" {
		wantSource = "SCHEDULED"
	}
	if completed.TriggerSource != wantSource || completed.AttemptCount != 1 || completed.StartedAt == nil || completed.FinishedAt == nil || string(completed.Result) != "{}" {
		t.Fatalf("unexpected completed run: %+v result=%s", completed, completed.Result)
	}
	status, source, outboxStatus, attempts, retryDelay, dispatches := inspectRun(t, ctx, db, completed.ID)
	if status != "SUCCEEDED" || source != wantSource || attempts != 1 || retryDelay != 13 || outboxStatus != "PUBLISHED" || dispatches != 1 {
		t.Fatalf("persisted run status=%s source=%s attempts=%d retry_delay=%d outbox=%s dispatches=%d", status, source, attempts, retryDelay, outboxStatus, dispatches)
	}
	if scheduleType == "ONCE" {
		var count int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM job_runs WHERE job_id=$1 AND trigger_source='SCHEDULED'`, jobID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("three schedulers created %d scheduled runs; want 1", count)
		}
	}
	t.Logf("schedule=%s job=%s run=%s status=%s attempts=%d retry_delay_seconds=%d outbox=%s dispatches=%d elapsed=%s", scheduleType, jobID, completed.ID, status, attempts, retryDelay, outboxStatus, dispatches, time.Since(started).Round(time.Millisecond))
}

func TestParallelRunsUseMultipleWorkers(t *testing.T) {
	apiURL, databaseURL := endpoints(t)
	ctx, cancel := context.WithTimeout(context.Background(), e2eTimeout(t))
	defer cancel()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 64
	transport.MaxConnsPerHost = 64
	client := &http.Client{Timeout: 15 * time.Second, Transport: transport}
	defer transport.CloseIdleConnections()
	db := openDatabase(t, databaseURL)
	defer db.Close()
	expectedWorkers := expectedWorkerCount(t)
	consumerCount := waitForWorkerConsumers(t, ctx, client, expectedWorkers)
	// [1] Grow each burst until extra queued work no longer raises worker throughput.
	for _, want := range loadBatchSizes(t) {
		jobID := createJob(t, ctx, client, apiURL, "MANUAL", nil)
		type triggerResult struct{ err error }
		results := make(chan triggerResult, want)
		started := time.Now()
		for i := 0; i < want; i++ {
			go func() {
				resp, err := client.Do(buildRequest(ctx, http.MethodPost, apiURL+"/v1/job/trigger/"+jobID, nil, uuid.NewString()))
				if err != nil {
					results <- triggerResult{err: err}
					return
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusAccepted {
					results <- triggerResult{err: fmt.Errorf("trigger HTTP %d: %s", resp.StatusCode, readResponse(resp))}
					return
				}
				results <- triggerResult{}
			}()
		}
		triggerFailures := 0
		for i := 0; i < want; i++ {
			if result := <-results; result.err != nil {
				triggerFailures++
			}
		}

		// [2] Sample run outcomes and worker ownership as the burst drains.
		maxRunning, maxOwners := 0, 0
		var submitted, terminal, succeeded int
		for {
			if err := ctx.Err(); err != nil {
				t.Fatalf("batch=%d timed out; submitted=%d terminal=%d succeeded=%d peak concurrent runs=%d workers=%d", want, submitted, terminal, succeeded, maxRunning, maxOwners)
			}
			var running, owners int
			err := db.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER(WHERE status IN ('SUCCEEDED','FAILED','CANCELLED')),count(*) FILTER(WHERE status='SUCCEEDED'),count(*) FILTER(WHERE status='RUNNING'),count(DISTINCT worker_id) FILTER(WHERE status='RUNNING') FROM job_runs WHERE job_id=$1 AND trigger_source='MANUAL'`, jobID).Scan(&submitted, &terminal, &succeeded, &running, &owners)
			if err != nil {
				t.Fatal(err)
			}
			if running > maxRunning {
				maxRunning = running
			}
			if owners > maxOwners {
				maxOwners = owners
			}
			if terminal == submitted {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
		elapsed := time.Since(started)
		var total, published, attemptTotal, runFailures int
		if err := db.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(attempt_count),0),count(*) FILTER(WHERE status IN ('FAILED','CANCELLED')) FROM job_runs WHERE job_id=$1 AND trigger_source='MANUAL'`, jobID).Scan(&total, &attemptTotal, &runFailures); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM outbox_events o JOIN job_runs r ON r.id=o.run_id WHERE r.job_id=$1 AND o.status='PUBLISHED'`, jobID).Scan(&published); err != nil {
			t.Fatal(err)
		}
		if total > want {
			t.Fatalf("batch=%d created %d runs from %d triggers", want, total, want)
		}
		if submitted >= expectedWorkers && (maxRunning < expectedWorkers || maxOwners < expectedWorkers) {
			t.Fatalf("batch=%d did not saturate workers: running=%d workers=%d expected=%d", want, maxRunning, maxOwners, expectedWorkers)
		}
		jobFailures := want - succeeded
		jobFailureRate := float64(jobFailures) / float64(want)
		triggerFailureRate := float64(triggerFailures) / float64(want)
		t.Logf("load_batch=%d submitted_runs=%d succeeded=%d failed_or_missing=%d published=%d attempts=%d trigger_errors=%d job_failure_rate=%.2f%% trigger_error_rate=%.2f%% workers=%d consumers=%d peak_running=%d peak_owners=%d elapsed_seconds=%.3f jobs_per_second=%.3f", want, total, succeeded, jobFailures, published, attemptTotal, triggerFailures, 100*jobFailureRate, 100*triggerFailureRate, expectedWorkers, consumerCount, maxRunning, maxOwners, elapsed.Seconds(), float64(succeeded)/elapsed.Seconds())
		if jobFailureRate > 0.05 || triggerFailureRate > 0.05 {
			t.Fatalf("batch=%d exceeded 5%% failure threshold: job failures=%.2f%% trigger errors=%.2f%%", want, 100*jobFailureRate, 100*triggerFailureRate)
		}
	}
}

func TestSustainedTriggerRate(t *testing.T) {
	rawRate := strings.TrimSpace(os.Getenv("E2E_TARGET_RPS"))
	if rawRate == "" {
		t.Skip("set E2E_TARGET_RPS to run a sustained API arrival-rate test")
	}
	rate, err := strconv.Atoi(rawRate)
	if err != nil || rate < 1 || rate > 10000 {
		t.Fatalf("invalid E2E_TARGET_RPS %q", rawRate)
	}
	duration := 10 * time.Second
	if raw := strings.TrimSpace(os.Getenv("E2E_RATE_DURATION")); raw != "" {
		duration, err = time.ParseDuration(raw)
		if err != nil || duration <= 0 {
			t.Fatalf("invalid E2E_RATE_DURATION %q", raw)
		}
	}

	apiURL, databaseURL := endpoints(t)
	ctx, cancel := context.WithTimeout(context.Background(), e2eTimeout(t))
	defer cancel()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 512
	transport.MaxConnsPerHost = 512
	client := &http.Client{Timeout: 15 * time.Second, Transport: transport}
	defer transport.CloseIdleConnections()
	db := openDatabase(t, databaseURL)
	defer db.Close()
	expectedWorkers := expectedWorkerCount(t)
	consumerCount := waitForWorkerConsumers(t, ctx, client, expectedWorkers)
	jobID := createJob(t, ctx, client, apiURL, "MANUAL", nil)
	type triggerResult struct{ err error }
	results := make(chan triggerResult, rate*int(duration/time.Second)+rate+1)

	// [1] Offer requests on a fixed clock so worker completion speed cannot throttle arrivals.
	started := time.Now()
	deadline := started.Add(duration)
	const schedulerTick = 10 * time.Millisecond
	tick := time.NewTicker(schedulerTick)
	defer tick.Stop()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	sent := 0
	basePerTick, fractionalPerTick, fractionalRemainder := rate/100, rate%100, 0
produce:
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("arrival phase timed out after %d triggers: %v", sent, ctx.Err())
		case <-timer.C:
			break produce
		case <-tick.C:
			if time.Now().After(deadline) {
				break produce
			}
			batch := basePerTick
			fractionalRemainder += fractionalPerTick
			if fractionalRemainder >= 100 {
				batch++
				fractionalRemainder -= 100
			}
			for i := 0; i < batch; i++ {
				sent++
				go func() {
					resp, err := client.Do(buildRequest(ctx, http.MethodPost, apiURL+"/v1/job/trigger/"+jobID, nil, uuid.NewString()))
					if err != nil {
						results <- triggerResult{err: err}
						return
					}
					defer resp.Body.Close()
					if resp.StatusCode != http.StatusAccepted {
						results <- triggerResult{err: fmt.Errorf("trigger HTTP %d: %s", resp.StatusCode, readResponse(resp))}
						return
					}
					results <- triggerResult{}
				}()
			}
		}
	}
	produceElapsed := time.Since(started)
	triggerErrors := 0
	for i := 0; i < sent; i++ {
		if result := <-results; result.err != nil {
			triggerErrors++
		}
	}

	// [2] Snapshot the queue at the end of the 100-RPS offer, then let all accepted runs drain.
	var submitted, queued, completedAtCutoff int
	if err := db.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER(WHERE status IN ('QUEUED','RETRY_WAIT','RUNNING','CANCEL_REQUESTED')),count(*) FILTER(WHERE status='SUCCEEDED') FROM job_runs WHERE job_id=$1 AND trigger_source='MANUAL'`, jobID).Scan(&submitted, &queued, &completedAtCutoff); err != nil {
		t.Fatal(err)
	}
	drainStarted := time.Now()
	maxRunning, maxOwners := 0, 0
	var terminal, succeeded int
	for {
		if err := ctx.Err(); err != nil {
			t.Fatalf("drain timed out: offered=%d submitted=%d terminal=%d succeeded=%d", sent, submitted, terminal, succeeded)
		}
		var running, owners int
		err := db.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER(WHERE status IN ('SUCCEEDED','FAILED','CANCELLED')),count(*) FILTER(WHERE status='SUCCEEDED'),count(*) FILTER(WHERE status='RUNNING'),count(DISTINCT worker_id) FILTER(WHERE status='RUNNING') FROM job_runs WHERE job_id=$1 AND trigger_source='MANUAL'`, jobID).Scan(&submitted, &terminal, &succeeded, &running, &owners)
		if err != nil {
			t.Fatal(err)
		}
		if running > maxRunning {
			maxRunning = running
		}
		if owners > maxOwners {
			maxOwners = owners
		}
		if terminal == submitted {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	drainElapsed := time.Since(drainStarted)
	totalElapsed := time.Since(started)
	var published, attemptTotal int
	if err := db.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(r.attempt_count),0) FROM outbox_events o JOIN job_runs r ON r.id=o.run_id WHERE r.job_id=$1 AND o.status='PUBLISHED'`, jobID).Scan(&published, &attemptTotal); err != nil {
		t.Fatal(err)
	}

	// [3] Missing runs and unsuccessful terminal runs count against the offered-job SLO.
	jobFailures := sent - succeeded
	jobFailureRate := float64(jobFailures) / float64(max(sent, 1))
	triggerFailureRate := float64(triggerErrors) / float64(max(sent, 1))
	offeredRPS := float64(sent) / produceElapsed.Seconds()
	completedAtCutoffRPS := float64(completedAtCutoff) / produceElapsed.Seconds()
	t.Logf("target_rps=%d offered=%d submitted_runs=%d completed_during_offer=%d succeeded_total=%d failed_or_missing=%d published=%d attempts=%d trigger_errors=%d job_failure_rate=%.2f%% trigger_error_rate=%.2f%% queue_at_cutoff=%d workers=%d consumers=%d peak_running=%d peak_owners=%d produce_seconds=%.3f drain_seconds=%.3f total_seconds=%.3f offered_rps=%.2f completed_during_offer_rps=%.2f completed_jobs_per_second=%.2f", rate, sent, submitted, completedAtCutoff, succeeded, jobFailures, published, attemptTotal, triggerErrors, 100*jobFailureRate, 100*triggerFailureRate, queued, expectedWorkers, consumerCount, maxRunning, maxOwners, produceElapsed.Seconds(), drainElapsed.Seconds(), totalElapsed.Seconds(), offeredRPS, completedAtCutoffRPS, float64(succeeded)/totalElapsed.Seconds())
	if offeredRPS < float64(rate)*0.95 {
		t.Fatalf("load generator achieved %.2f requests/s; target was %d/s", offeredRPS, rate)
	}
	if jobFailureRate > 0.05 || triggerFailureRate > 0.05 {
		t.Fatalf("target=%d RPS exceeded 5%% failure ceiling: jobs=%.2f%% triggers=%.2f%%", rate, 100*jobFailureRate, 100*triggerFailureRate)
	}
}

func loadBatchSizes(t *testing.T) []int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("E2E_LOAD_BATCHES"))
	if raw == "" {
		return []int{18, 36, 72, 144, 288}
	}
	parts := strings.Split(raw, ",")
	batches := make([]int, 0, len(parts))
	for _, part := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < 1 {
			t.Fatalf("invalid E2E_LOAD_BATCHES value %q", part)
		}
		batches = append(batches, n)
	}
	return batches
}

func expectedWorkerCount(t *testing.T) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("E2E_EXPECTED_WORKERS"))
	if raw == "" {
		t.Skip("set E2E_EXPECTED_WORKERS to verify the scaled worker pool")
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		t.Fatalf("invalid E2E_EXPECTED_WORKERS %q", raw)
	}
	return n
}

func waitForWorkerConsumers(t *testing.T, ctx context.Context, client *http.Client, want int) int {
	t.Helper()
	base := strings.TrimRight(os.Getenv("RABBITMQ_MANAGEMENT_URL"), "/")
	if base == "" {
		t.Fatal("RABBITMQ_MANAGEMENT_URL is required for worker pool verification")
	}
	user, password := os.Getenv("RABBITMQ_USER"), os.Getenv("RABBITMQ_PASSWORD")
	type consumer struct {
		ConsumerTag string `json:"consumer_tag"`
		Queue       struct {
			Name string `json:"name"`
		} `json:"queue"`
	}
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/consumers", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.SetBasicAuth(user, password)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("query RabbitMQ consumers: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("RabbitMQ consumers HTTP %d: %s", resp.StatusCode, readResponse(resp))
		}
		var consumers []consumer
		err = json.NewDecoder(resp.Body).Decode(&consumers)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		tags := map[string]bool{}
		for _, item := range consumers {
			if item.Queue.Name == "fishstick.worker" {
				tags[item.ConsumerTag] = true
			}
		}
		if len(tags) >= want {
			return len(tags)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("RabbitMQ has %d/%d worker consumers", len(tags), want)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func endpoints(t *testing.T) (string, string) {
	t.Helper()
	apiURL := strings.TrimRight(os.Getenv("API_URL"), "/")
	dbURL := strings.TrimSpace(os.Getenv("E2E_DATABASE_URL"))
	if apiURL == "" || dbURL == "" {
		t.Skip("set API_URL and E2E_DATABASE_URL to run cross-process tests")
	}
	return apiURL, dbURL
}
func e2eTimeout(t *testing.T) time.Duration {
	t.Helper()
	if raw := os.Getenv("E2E_TIMEOUT"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	return 60 * time.Second
}
func openDatabase(t *testing.T, raw string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", raw)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Ping(); err != nil {
		db.Close()
		t.Fatal(err)
	}
	return db
}
func createJob(t *testing.T, ctx context.Context, client *http.Client, apiURL, scheduleType string, scheduledAt *time.Time) string {
	t.Helper()
	body := map[string]any{"name": "pipeline e2e " + uuid.NewString(), "event_type": "e2e.echo", "schedule_type": scheduleType, "payload": map[string]any{"value": 42}, "max_attempts": 4, "retry_delay_seconds": 13, "timeout_seconds": 20}
	if scheduledAt != nil {
		body["scheduled_at"] = scheduledAt.Format(time.RFC3339Nano)
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp := request(t, ctx, client, http.MethodPost, apiURL+"/v1/job/create", encoded, "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create job HTTP %d: %s", resp.StatusCode, readBody(t, resp))
	}
	var created jobResponse
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if _, err := uuid.Parse(created.ID); err != nil {
		t.Fatalf("invalid job ID %q", created.ID)
	}
	return created.ID
}
func waitForRun(t *testing.T, ctx context.Context, db *sql.DB, client *http.Client, apiURL, jobID string) run {
	t.Helper()
	for {
		if err := ctx.Err(); err != nil {
			t.Fatalf("run for job %s did not complete: %v", jobID, err)
		}
		resp := request(t, ctx, client, http.MethodGet, apiURL+"/v1/job/"+jobID+"/runs?limit=10", nil, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("list runs HTTP %d: %s", resp.StatusCode, readBody(t, resp))
		}
		var page runPage
		err := json.NewDecoder(resp.Body).Decode(&page)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Data) > 0 {
			row := page.Data[0]
			if row.Status == "FAILED" || row.Status == "CANCELLED" {
				t.Fatalf("run ended in %s", row.Status)
			}
			if row.Status == "SUCCEEDED" {
				for {
					status, _, outbox, _, _, _ := inspectRun(t, ctx, db, row.ID)
					if outbox == "PUBLISHED" {
						if status != "SUCCEEDED" {
							t.Fatalf("run status changed to %s", status)
						}
						return row
					}
					time.Sleep(25 * time.Millisecond)
				}
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
}
func inspectRun(t *testing.T, ctx context.Context, db *sql.DB, runID string) (string, string, string, int32, int32, int) {
	t.Helper()
	var status, source, outbox string
	var attempts, retry int32
	var count int
	err := db.QueryRowContext(ctx, `SELECT r.status,r.trigger_source,r.attempt_count,r.retry_delay_seconds,o.status,(SELECT count(*) FROM outbox_events WHERE run_id=r.id) FROM job_runs r JOIN outbox_events o ON o.run_id=r.id WHERE r.id=$1`, runID).Scan(&status, &source, &attempts, &retry, &outbox, &count)
	if err != nil {
		t.Fatalf("inspect run %s: %v", runID, err)
	}
	return status, source, outbox, attempts, retry, count
}
func request(t *testing.T, ctx context.Context, client *http.Client, method, url string, body []byte, key string) *http.Response {
	t.Helper()
	resp, err := client.Do(buildRequest(ctx, method, url, body, key))
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return resp
}
func buildRequest(ctx context.Context, method, url string, body []byte, key string) *http.Request {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		panic(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	return req
}
func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	return readResponse(resp)
}
func readResponse(resp *http.Response) string {
	var body bytes.Buffer
	_, _ = body.ReadFrom(resp.Body)
	return body.String()
}
