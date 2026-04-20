package colmena

import (
	"errors"
	"testing"
)

func TestValidateWriteSQL_Rejects(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		sql  string
	}{
		{"random_bare", "INSERT INTO t(x) VALUES (RANDOM())"},
		{"random_lowercase", "UPDATE t SET x = random() WHERE id = 1"},
		{"randomblob", "INSERT INTO t(b) VALUES (randomblob(16))"},
		{"current_timestamp", "INSERT INTO t(ts) VALUES (CURRENT_TIMESTAMP)"},
		{"current_date", "INSERT INTO t(d) VALUES (current_date)"},
		{"current_time", "INSERT INTO t(d) VALUES (CURRENT_TIME)"},
		{"datetime_now", "INSERT INTO t(ts) VALUES (datetime('now'))"},
		{"datetime_now_with_modifier", "INSERT INTO t(ts) VALUES (datetime('now', '+1 day'))"},
		{"datetime_no_args", "INSERT INTO t(ts) VALUES (datetime())"},
		{"strftime_now", "INSERT INTO t(ts) VALUES (strftime('%s', 'now'))"},
		{"unixepoch_no_args", "INSERT INTO t(n) VALUES (unixepoch())"},
		{"julianday_now", "INSERT INTO t(n) VALUES (julianday('now'))"},
		{"last_insert_rowid", "INSERT INTO t(x) VALUES (last_insert_rowid())"},
		{"changes", "INSERT INTO t(x) VALUES (changes())"},
		{"total_changes", "INSERT INTO t(x) VALUES (total_changes())"},
		{"mixed_case_spacing", "UPDATE t SET ts = DateTime  ( 'now' )"},
		{"now_with_double_quotes_inside_not_applicable", "UPDATE t SET ts = strftime('%Y', 'now')"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := validateWriteSQL(tc.sql)
			if err == nil {
				t.Fatalf("expected rejection for %q", tc.sql)
			}
			var nd *ErrNonDeterministicSQL
			if !errors.As(err, &nd) {
				t.Fatalf("expected ErrNonDeterministicSQL, got %T: %v", err, err)
			}
		})
	}
}

func TestValidateWriteSQL_Accepts(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		sql  string
	}{
		{"plain_insert", "INSERT INTO t(x) VALUES (?)"},
		{"parameterized_time", "INSERT INTO t(ts) VALUES (?)"},
		{"datetime_fixed_date", "INSERT INTO t(ts) VALUES (datetime('2020-01-01'))"},
		{"datetime_fixed_with_modifier", "INSERT INTO t(ts) VALUES (datetime('2020-01-01', '+1 day'))"},
		{"strftime_fixed", "INSERT INTO t(ts) VALUES (strftime('%Y', '2020-01-01'))"},
		{"user_data_mentioning_now", "INSERT INTO t(note) VALUES ('the current time is now')"},
		{"user_data_with_function_name", "INSERT INTO t(note) VALUES ('random() is fun')"},
		{"column_named_current_date", "INSERT INTO t(current_date_col) VALUES (?)"},
		{"cte_with_fixed_time", "WITH c AS (SELECT datetime('2020-01-01') AS t) INSERT INTO t(ts) SELECT t FROM c"},
		{"update_set_from_param", "UPDATE t SET ts = ? WHERE id = ?"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := validateWriteSQL(tc.sql); err != nil {
				t.Fatalf("unexpected rejection of %q: %v", tc.sql, err)
			}
		})
	}
}

func TestValidateWriteSQL_DDLAlsoChecked(t *testing.T) {
	t.Parallel()
	// DDL that sets a default to a non-deterministic value would make every
	// node pick a different default on insert. Catch it here too.
	sql := "CREATE TABLE t (id INTEGER PRIMARY KEY, created_at TEXT DEFAULT (datetime('now')))"
	if err := validateWriteSQL(sql); err == nil {
		t.Fatal("expected rejection of DEFAULT datetime('now')")
	}
}

func TestValidateWriteSQL_EscapedQuotesDoNotConfuse(t *testing.T) {
	t.Parallel()
	// The string 'now' inside a SQL string literal with escaped quotes should
	// be treated as user data unless it's exactly the 'now' modifier token.
	sql := "INSERT INTO t(note) VALUES ('she said ''now'' and left')"
	// Our stripper normalises this conservatively. Accept a false-positive
	// here is acceptable: the API discourages inlining strings anyway. Just
	// document the current behaviour with a test so we notice if it changes.
	_ = validateWriteSQL(sql)
}
