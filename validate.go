package colmena

import (
	"fmt"
	"regexp"
	"strings"
)

// ErrNonDeterministicSQL is returned when a write statement references a SQL
// function whose result would vary per-node (e.g. RANDOM(), datetime('now')),
// which would silently diverge the replicas since each one evaluates the SQL
// locally. Pass the value as a parameter instead.
type ErrNonDeterministicSQL struct {
	Call string
	SQL  string
}

func (e *ErrNonDeterministicSQL) Error() string {
	return fmt.Sprintf("colmena: non-deterministic SQL %q in write — replicas would diverge; compute the value in Go and pass it as a parameter instead (sql: %s)", e.Call, trimForErr(e.SQL))
}

func trimForErr(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 140 {
		return s[:137] + "..."
	}
	return s
}

// Functions/keywords whose output differs per node when evaluated against a
// replica's local SQLite. Non-time helpers are banned outright; the time
// family is only banned when it defaults to "now" (zero args or an explicit
// 'now' modifier anywhere in the argument list). Deterministic calls like
// datetime('2020-01-01') are allowed.
var (
	reStripStrings = regexp.MustCompile(`'(?:''|[^'])*'`)
	reNonDetFuncs  = regexp.MustCompile(`(?i)\b(random|randomblob|last_insert_rowid|changes|total_changes)\s*\(`)
	reNonDetKeys   = regexp.MustCompile(`(?i)\b(current_timestamp|current_date|current_time)\b`)
	reTimeNoArgs   = regexp.MustCompile(`(?i)\b(datetime|date|time|julianday|strftime|unixepoch)\s*\(\s*\)`)
	reTimeNow      = regexp.MustCompile(`(?i)\b(datetime|date|time|julianday|strftime|unixepoch)\s*\([^)]*\bNOW_LITERAL\b[^)]*\)`)
)

// validateWriteSQL rejects replicated statements whose evaluation would give
// different answers on different nodes. Called from node.execute before the
// command enters Raft so the failure is surfaced synchronously to the caller
// and the cluster state stays consistent.
func validateWriteSQL(sql string) error {
	// Replace string literals with a sentinel so we can distinguish user data
	// (safe — ignored) from function tokens (unsafe — rejected). The sentinel
	// keeps a visible marker when the original literal was exactly 'now'.
	stripped := reStripStrings.ReplaceAllStringFunc(sql, func(lit string) string {
		inner := lit[1 : len(lit)-1]
		inner = strings.ReplaceAll(inner, "''", "'")
		if strings.EqualFold(strings.TrimSpace(inner), "now") {
			return " NOW_LITERAL "
		}
		return " STR_LITERAL "
	})

	if m := reNonDetFuncs.FindString(stripped); m != "" {
		return &ErrNonDeterministicSQL{Call: normalizeCall(m), SQL: sql}
	}
	if m := reNonDetKeys.FindString(stripped); m != "" {
		return &ErrNonDeterministicSQL{Call: strings.ToUpper(strings.TrimSpace(m)), SQL: sql}
	}
	if m := reTimeNoArgs.FindString(stripped); m != "" {
		return &ErrNonDeterministicSQL{Call: normalizeCall(m) + ")", SQL: sql}
	}
	if m := reTimeNow.FindString(stripped); m != "" {
		// normalizeCall returns e.g. "datetime(" from the captured prefix.
		// Append the diagnostic suffix showing the 'now' modifier.
		return &ErrNonDeterministicSQL{Call: timeNowCall(m), SQL: sql}
	}
	return nil
}

// normalizeCall turns "DateTime  (" into "datetime(".
func normalizeCall(raw string) string {
	raw = strings.ToLower(raw)
	raw = strings.Join(strings.Fields(raw), "")
	return raw
}

// timeNowCall extracts the function name from a reTimeNow match (which can
// include arbitrary intervening args) and returns a clean "fn('now')" string.
func timeNowCall(raw string) string {
	lower := strings.ToLower(raw)
	for _, fn := range []string{"datetime", "date", "time", "julianday", "strftime", "unixepoch"} {
		if strings.HasPrefix(strings.TrimLeft(lower, " \t\n"), fn) {
			return fn + "('now')"
		}
	}
	return normalizeCall(raw) + "'now')"
}

// validateWriteStatements checks every statement in the command. Returns the
// first offender with full SQL for the error message.
func validateWriteStatements(stmts []Statement) error {
	for _, s := range stmts {
		if err := validateWriteSQL(s.SQL); err != nil {
			return err
		}
	}
	return nil
}
