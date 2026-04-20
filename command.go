package colmena

import (
	"encoding/json"
	"fmt"
)

// CommandType identifies the type of command in the Raft log.
type CommandType uint8

const (
	// CommandExecute is a write operation (INSERT, UPDATE, DELETE, DDL).
	CommandExecute CommandType = iota
	// CommandExecuteMulti is an atomic batch of write operations (transaction).
	CommandExecuteMulti
)

// Command is the unit of replication in the Raft log.
type Command struct {
	Type       CommandType `json:"type"`
	DB         string      `json:"db"`
	Statements []Statement `json:"stmts"`
}

// Statement is a single SQL statement with optional arguments.
type Statement struct {
	SQL  string        `json:"sql"`
	Args []interface{} `json:"args,omitempty"`
}

// ExecResult holds the result of an executed statement.
type ExecResult struct {
	LastInsertID int64 `json:"last_id"`
	RowsAffected int64 `json:"rows"`
}

// ApplyResult is returned by the FSM after applying a command.
type ApplyResult struct {
	Results []ExecResult `json:"results,omitempty"`
	Error   string       `json:"error,omitempty"`
}

// marshalCommand serializes cmd with the v1 envelope:
//
//	[10-byte header: magic|kind=Command|version=1] [JSON payload]
//
// Older Colmena versions (<= v0.5.x) wrote raw JSON with no envelope. Those
// entries can still be read back by unmarshalCommand, so a cluster's existing
// Raft log survives an upgrade — only new entries get the envelope.
func marshalCommand(cmd *Command) ([]byte, error) {
	payload, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("colmena: marshal command: %w", err)
	}
	return encodeEnvelope(FormatKindCommand, CommandFormatVersion, payload), nil
}

// unmarshalCommand parses a Raft log entry written by any Colmena version.
// It recognises:
//   - v1 envelope (current): magic + kind=Command + version=1 + JSON
//   - legacy unenveloped JSON (<= v0.5.x): raw `{...}` bytes
//
// An envelope with kind=Command but an unknown version returns
// ErrUnsupportedFormatVersion so the node refuses to apply garbage rather
// than silently diverging from its peers.
func unmarshalCommand(data []byte) (*Command, error) {
	payload := data
	if hasEnvelopeMagic(data) {
		kind, version, p, err := decodeEnvelope(data)
		if err != nil {
			return nil, fmt.Errorf("colmena: unmarshal command: %w", err)
		}
		if kind != FormatKindCommand {
			return nil, fmt.Errorf("colmena: unmarshal command: unexpected envelope kind %d", kind)
		}
		switch version {
		case 1:
			payload = p
		default:
			return nil, fmt.Errorf("colmena: unmarshal command version %d: %w", version, ErrUnsupportedFormatVersion)
		}
	} else if !looksLikeLegacyCommand(data) {
		return nil, fmt.Errorf("colmena: unmarshal command: unrecognized format (first byte 0x%02x)", firstByte(data))
	}

	var cmd Command
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return nil, fmt.Errorf("colmena: unmarshal command: %w", err)
	}
	return &cmd, nil
}

// looksLikeLegacyCommand returns true if data looks like the pre-v0.6
// unenveloped JSON Command (starts with '{' after optional whitespace).
func looksLikeLegacyCommand(data []byte) bool {
	for _, b := range data {
		switch b {
		case ' ', '\t', '\r', '\n':
			continue
		case '{':
			return true
		default:
			return false
		}
	}
	return false
}

func firstByte(data []byte) byte {
	if len(data) == 0 {
		return 0
	}
	return data[0]
}
