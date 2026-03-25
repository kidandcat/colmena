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

func marshalCommand(cmd *Command) ([]byte, error) {
	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("colmena: marshal command: %w", err)
	}
	return data, nil
}

func unmarshalCommand(data []byte) (*Command, error) {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, fmt.Errorf("colmena: unmarshal command: %w", err)
	}
	return &cmd, nil
}
