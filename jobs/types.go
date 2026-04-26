package jobs

import "time"

// Status is the lifecycle state of a job.
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusDead      Status = "dead"
)

// Priority controls ordering among ready-to-run jobs. Higher values run first.
type Priority int

const (
	PriorityLow    Priority = -10
	PriorityNormal Priority = 0
	PriorityHigh   Priority = 10
)

// Job is the persisted record for a single unit of work.
type Job struct {
	ID          string
	Type        string
	Payload     []byte
	Status      Status
	Priority    Priority
	Attempts    int
	MaxAttempts int
	EnqueuedAt  time.Time
	RunAt       time.Time
	ClaimedAt   *time.Time
	ClaimedBy   string
	StartedAt   *time.Time
	FinishedAt  *time.Time
	LastError   string
	UniqueKey   string
	Timeout     time.Duration
}

// Backoff describes exponential backoff between retries.
type Backoff struct {
	Base time.Duration
	Max  time.Duration
}

// Rate is a token-bucket rate limit: at most N jobs of a given type may be
// claimed per Per duration cluster-wide.
type Rate struct {
	N   int
	Per time.Duration
}
