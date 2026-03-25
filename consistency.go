package colmena

import "context"

// ConsistencyLevel defines the read consistency guarantee.
type ConsistencyLevel int

const (
	// ConsistencyNone reads directly from local SQLite with no checks.
	// Fastest, but may return stale data if this node is partitioned.
	ConsistencyNone ConsistencyLevel = iota

	// ConsistencyWeak reads from local SQLite after verifying this node
	// believes it is the leader. Small window of staleness possible if
	// leadership was just lost.
	ConsistencyWeak

	// ConsistencyStrong ensures linearizable reads by verifying leadership
	// with a quorum before reading. Adds latency from the quorum round-trip.
	ConsistencyStrong
)

type contextKey int

const consistencyKey contextKey = 0

// WithConsistency returns a context that carries the specified consistency level.
// Use this with QueryContext to override the node's default consistency.
//
//	ctx := colmena.WithConsistency(ctx, colmena.ConsistencyStrong)
//	rows, err := db.QueryContext(ctx, "SELECT ...")
func WithConsistency(ctx context.Context, level ConsistencyLevel) context.Context {
	return context.WithValue(ctx, consistencyKey, level)
}

func consistencyFromContext(ctx context.Context, defaultLevel ConsistencyLevel) ConsistencyLevel {
	if v, ok := ctx.Value(consistencyKey).(ConsistencyLevel); ok {
		return v
	}
	return defaultLevel
}
