package jobs

import "time"

// Config configures the jobs subsystem attached to a Colmena node.
type Config struct {
	// Workers is the number of in-process worker goroutines per node.
	// Default: max(2, GOMAXPROCS).
	Workers int

	// PollInterval is how often each worker polls for new jobs when idle.
	// Default: 1s.
	PollInterval time.Duration

	// DefaultTimeout is applied to jobs enqueued without an explicit timeout.
	// Default: 5m.
	DefaultTimeout time.Duration

	// DefaultMaxAttempts is applied to jobs enqueued without an explicit value.
	// Default: 5.
	DefaultMaxAttempts int

	// SweepInterval controls how often the leader scans for orphaned jobs
	// (claimed_at + timeout_ms < now and status in claimed/running). Default: 30s.
	SweepInterval time.Duration

	// ScheduleInterval is the cadence of the cron scheduler tick. Default: 30s.
	ScheduleInterval time.Duration

	// DefaultBackoff is the base backoff for retries when a handler does not
	// register a custom backoff. Default: 5s base, 1h cap.
	DefaultBackoff Backoff
}

func (c *Config) applyDefaults() {
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 1 * time.Second
	}
	if c.DefaultTimeout <= 0 {
		c.DefaultTimeout = 5 * time.Minute
	}
	if c.DefaultMaxAttempts <= 0 {
		c.DefaultMaxAttempts = 5
	}
	if c.SweepInterval <= 0 {
		c.SweepInterval = 30 * time.Second
	}
	if c.ScheduleInterval <= 0 {
		c.ScheduleInterval = 30 * time.Second
	}
	if c.DefaultBackoff.Base <= 0 {
		c.DefaultBackoff.Base = 5 * time.Second
	}
	if c.DefaultBackoff.Max <= 0 {
		c.DefaultBackoff.Max = 1 * time.Hour
	}
}
