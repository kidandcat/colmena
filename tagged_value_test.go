package colmena

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"io"
	"testing"
	"time"
)

func TestRPCQuery_TimeColumnPreservesType(t *testing.T) {
	// End-to-end: write a DATETIME row via the leader, then call the RPC
	// Query path directly (the same code path a follower forwards through)
	// and ensure the forwarded row can scan into *time.Time on the caller.
	t.Parallel()
	node := testNode(t)
	db := node.DB()

	if _, err := db.Exec("CREATE TABLE tag_dt (id INTEGER PRIMARY KEY, created_at DATETIME)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	when := time.Date(2026, 4, 20, 10, 11, 12, 0, time.UTC)
	if _, err := db.Exec("INSERT INTO tag_dt (id, created_at) VALUES (1, ?)", when); err != nil {
		t.Fatalf("insert: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	svc := &RPCService{node: node}
	var resp RPCQueryResponse
	if err := svc.Query(&RPCQueryRequest{DB: "default", SQL: "SELECT created_at FROM tag_dt WHERE id = 1"}, &resp); err != nil {
		t.Fatalf("rpc query: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("rpc error: %s", resp.Error)
	}
	if len(resp.TaggedRows) != 1 {
		t.Fatalf("expected 1 tagged row, got %d", len(resp.TaggedRows))
	}

	rows := &rpcRows{columns: resp.Columns, tagged: resp.TaggedRows}
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("next: %v", err)
	}
	got, ok := dest[0].(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T (%v)", dest[0], dest[0])
	}
	if !got.Equal(when) {
		t.Fatalf("time mismatch: got %v want %v", got, when)
	}
}

func TestTaggedValue_Roundtrip(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 4, 20, 12, 34, 56, 789_000_000, time.UTC)
	cases := []struct {
		name string
		in   any
		want any
	}{
		{"null", nil, nil},
		{"bool_true", true, true},
		{"bool_false", false, false},
		{"int64", int64(42), int64(42)},
		{"int_as_int64", 42, int64(42)},
		{"float64", 3.14, 3.14},
		{"string", "hello", "hello"},
		{"bytes", []byte{0x01, 0x02, 0xff}, []byte{0x01, 0x02, 0xff}},
		{"time", now, now},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			tv := encodeTaggedValue(tc.in)
			// must survive JSON marshal+unmarshal (the wire boundary).
			b, err := json.Marshal(tv)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var back TaggedValue
			if err := json.Unmarshal(b, &back); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			got, err := decodeTaggedValue(back)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			switch want := tc.want.(type) {
			case []byte:
				gb, ok := got.([]byte)
				if !ok || !bytes.Equal(gb, want) {
					t.Fatalf("bytes mismatch: got %v want %v", got, want)
				}
			case time.Time:
				gt, ok := got.(time.Time)
				if !ok || !gt.Equal(want) {
					t.Fatalf("time mismatch: got %v want %v", got, want)
				}
			default:
				if got != tc.want {
					t.Fatalf("value mismatch: got %v (%T) want %v (%T)", got, got, tc.want, tc.want)
				}
			}
		})
	}
}

func TestTaggedValue_UnknownKind(t *testing.T) {
	t.Parallel()
	if _, err := decodeTaggedValue(TaggedValue{T: "weird", V: json.RawMessage(`"x"`)}); err == nil {
		t.Fatalf("expected error for unknown kind")
	}
}

func TestRpcRows_PrefersTaggedOverLegacy(t *testing.T) {
	t.Parallel()
	tv := encodeTaggedValue(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	rows := &rpcRows{
		columns: []string{"created_at"},
		tagged:  [][]TaggedValue{{tv}},
		legacy:  [][]json.RawMessage{{json.RawMessage(`"IGNORED"`)}},
	}
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("next: %v", err)
	}
	if _, ok := dest[0].(time.Time); !ok {
		t.Fatalf("expected time.Time from tagged row, got %T (%v)", dest[0], dest[0])
	}
	if err := rows.Next(dest); err != io.EOF {
		t.Fatalf("expected EOF after one row, got %v", err)
	}
}

func TestRpcRows_FallsBackToLegacyWhenTaggedEmpty(t *testing.T) {
	t.Parallel()
	rows := &rpcRows{
		columns: []string{"n"},
		legacy:  [][]json.RawMessage{{json.RawMessage(`42`)}},
	}
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("next: %v", err)
	}
	// JSON numbers decode to float64 under json.Unmarshal into any — that is
	// the legacy behavior and callers already tolerate it.
	if f, ok := dest[0].(float64); !ok || f != 42 {
		t.Fatalf("expected 42 via legacy path, got %v (%T)", dest[0], dest[0])
	}
}
