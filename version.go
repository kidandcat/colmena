package colmena

// LibraryVersion is the semver string for the current Colmena release.
// 2.x is the raft-free rewrite: embedded SQLite + continuous backup (PITR).
// 2.1 adds streaming snapshots, chunked WAL spool, and decoupled checkpoint.
// 2.1.1 fixes a *sql.DB (and goroutine) leak on every Node.DB()/OpenDB call.
const LibraryVersion = "2.1.1"
