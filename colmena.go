// Package colmena provides a distributed SQLite database as an embeddable Go library.
//
// Colmena uses Raft consensus (via hashicorp/raft) for replication and
// modernc.org/sqlite (pure Go, no CGo) for storage. It exposes a standard
// database/sql interface, making it a drop-in distributed database for Go programs.
//
// Quick start:
//
//	node, err := colmena.New(colmena.Config{
//	    NodeID:    "node-1",
//	    DataDir:   "./data/node1",
//	    Bind:      "0.0.0.0:9000",
//	    Bootstrap: true,
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer node.Close()
//
//	db := node.DB()
//	db.Exec("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)")
//	db.Exec("INSERT INTO kv (key, value) VALUES (?, ?)", "hello", "world")
//
//	var value string
//	db.QueryRow("SELECT value FROM kv WHERE key = ?", "hello").Scan(&value)
package colmena
