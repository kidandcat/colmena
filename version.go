package colmena

// LibraryVersion is the semver string for the current Colmena release.
// 2.x is the raft-free rewrite: embedded SQLite + continuous backup (PITR).
// 2.1 adds streaming snapshots, chunked WAL spool, and decoupled checkpoint.
const LibraryVersion = "2.1.0"
