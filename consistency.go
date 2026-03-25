package colmena

import "context"

// ConsistencyLevel defines the read consistency guarantee.
type ConsistencyLevel int

const (
	// ConsistencyNone reads from the local SQLite on this node, with no
	// communication to other nodes. Fastest option (~8µs) but the data
	// may be stale if this node is a follower behind on replication or
	// is partitioned from the cluster.
	// Use for: dashboards, analytics, data where momentary staleness is OK.
	ConsistencyNone ConsistencyLevel = iota

	// ConsistencyWeak reads from the leader. If this node is the leader,
	// it reads locally. If not, it forwards the query to the leader.
	// This ensures you always read from the node that processes writes,
	// so data is fresh. However, there is a small window (~1 heartbeat
	// timeout) where a just-deposed leader still believes it is the leader
	// and serves a stale local read before stepping down.
	// Use for: most applications — fresh data with minimal overhead.
	ConsistencyWeak

	// ConsistencyStrong provides linearizable reads. The leader contacts
	// a quorum of nodes to confirm it still holds leadership before
	// reading. If this node is not the leader, the query is forwarded.
	// Guarantees you read the latest committed state — impossible to get
	// stale data, even during leadership transitions.
	// Use for: financial transactions, uniqueness checks, anything where
	// reading stale data would cause incorrect behavior.
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
