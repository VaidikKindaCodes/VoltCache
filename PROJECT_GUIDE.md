# VoltCache Project Guide

## Folder Structure

- `cmd/`
  - `server/` - server entrypoint
  - `cli/` - simple command-line client
- `config/`
  - `config.go` - Redis-style config file loader and defaults
- `internals/`
  - `domain/` - interfaces and shared types
  - `handler/` - command dispatch and request lifecycle
  - `rdb/` - RDB file loading
  - `replication/` - leader and follower replication behavior
  - `storage/` - in-memory key-value store implementation
- `pkg/`
  - `resp/` - RESP encode/decode helpers
- `Dockerfile` - multi-stage Docker build for leader/replica
- `docker-compose.yml` - leader and replica services with shared volume

## Important Files

### `cmd/server/main.go`
- Purpose: starts the VoltCache server.
- Contains: CLI flag parsing, config loading, persistence startup, server listener loop, graceful shutdown.
- Called by: none (application entrypoint).
- Depends on: `config/config.go`, `internals/storage`, `internals/handler`, `internals/rdb`, `internals/replication`.
- Lifecycle: reads config, loads AOF or RDB, creates handler, starts TCP listener, handles SIGINT/SIGTERM, flushes persistence.

### `config/config.go`
- Purpose: load simple Redis-style config files and hold default values.
- Contains: `Config`, `NewConfig()`, `LoadConfig()`.
- Depends on: standard library.
- Called by: `cmd/server/main.go`.

### `internals/domain/store.go`
- Purpose: shared storage interface and entry type.
- Contains: `Store`, `Entry`.
- Depends on: `time`.
- Called by: `internals/storage`, `internals/handler`, `internals/replication`.

### `internals/domain/commandhandler.go`
- Purpose: defines command handler interface.
- Contains: `CommandHandler`.
- Called by: `cmd/server/main.go`, `internals/replication/followermanager.go`.

### `internals/domain/replication.go`
- Purpose: leader and follower replication interfaces.
- Contains: `LeaderManager`, `FollowerManager`.
- Called by: `internals/handler`, `cmd/server/main.go`, `internals/replication`.

### `internals/storage/inmemorystore.go`
- Purpose: thread-safe key-value storage.
- Contains: `inMemoryStore`, `Set`, `Get`, `Entries`.
- Depends on: `sync`, `time`, shared `domain.Entry`.

### `internals/rdb/rdbparser.go`
- Purpose: load RDB files into memory.
- Contains: `LoadRDBFile` and helper parsers.
- Depends on: binary parsing and `domain.Store`.

### `internals/replication/leadermanager.go`
- Purpose: manage leader replication state.
- Contains: full resync, follower list, propagation, acknowledgments.
- Depends on: `resp`, `net`, `sync`, `time`.

### `internals/replication/followermanager.go`
- Purpose: follower connection and command processing from leader.
- Contains: `ConnectToLeader`, PSYNC handshake, command loop.
- Depends on: `resp`, `net`, `bufio`, `io`, `strings`.

### `internals/handler/commandhandler.go`
- Purpose: parse client commands and dispatch responses.
- Contains: command handlers for `PING`, `ECHO`, `SET`, `GET`, `INFO`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, replication commands, and shutdown logic.
- Depends on: `resp`, `config`, `domain`, `sync`, `net`, `time`.
- Key features: AOF append, Pub/Sub, thread-safe writes, connection cleanup.

### `pkg/resp/resp.go`
- Purpose: RESP encoding/decoding for network protocol.
- Contains: RESP encoders for strings, arrays, integers, errors, nulls, and `ParseRESP`.
- Called by: command handler, replication, CLI.

### `Dockerfile`
- Purpose: multi-stage build for deployment.
- Contains: Golang build stage and minimal Alpine runtime.
- Used by: Docker Compose.

### `docker-compose.yml`
- Purpose: define leader and replica services.
- Contains: shared volume, replicated service startup flags, ports.

## Request Lifecycle

1. Client connects to the TCP server.
2. `cmd/server/main.go` calls `handler.HandleClient` in a goroutine.
3. `HandleClient` reads RESP messages with `resp.ParseRESP`.
4. Parsed command is passed to `ProcessCommand`.
5. `ProcessCommand` dispatches on the command name.
6. Write commands (`SET`) update the store, append to AOF, and propagate to followers.
7. Pub/Sub commands update subscriber maps or dispatch messages asynchronously.
8. Responses are encoded using `resp.Encode...` and written safely.

## RESP Parser

- Reads request line beginning with `*N`.
- Parses each bulk string with `$len` and reads exactly that many bytes.
- Returns a string slice of arguments.
- Only supports array-based client requests.

## Command Dispatch

- Implemented by `ProcessCommand` in `internals/handler/commandhandler.go`.
- Uses switch-case on uppercase command names.
- `SET` and `GET` operate on the store.
- `SUBSCRIBE`/`UNSUBSCRIBE` manage channel maps.
- `PUBLISH` sends messages to subscribers asynchronously.

## Storage Organization

- In-memory store uses `sync.Mutex` for concurrency.
- Data held in `domain.Entry` values with optional expiration.
- `Entries()` returns a safe copy for persistence.

## Replication

- Leader sends `FULLRESYNC` and then writes commands to followers.
- Follower connects, performs `PSYNC`, reads RDB snapshot, then processes commands.
- Leader tracks follower connections and can send `REPLCONF GETACK`.

## AOF

- AOF enabled via `appendonly yes` in config or `-appendonly` flag.
- `SET` commands are appended in RESP format.
- On startup, AOF is replayed before RDB.
- Writer uses a mutex to protect the append file.

## RDB Loading/Saving

- RDB loader reads Redis-style binary format in `internals/rdb/rdbparser.go`.
- On startup, if AOF is absent or disabled, RDB is loaded.
- On shutdown, the server saves the store to RDB.

## Pub/Sub

- `SUBSCRIBE` stores net.Conn references in channel maps.
- `UNSUBSCRIBE` removes them.
- `PUBLISH` sends messages asynchronously and does not block the publisher.
- Slow or closed subscribers are cleaned up.

## Docker Services Communication

- `leader` and `replica` share a named volume `voltcache-data`.
- Replica uses `-replicaof leader 6379` to connect to the leader.
- Both services persist files to the shared `/data` volume.

## Graceful Shutdown

- `cmd/server/main.go` uses `signal.NotifyContext` for SIGINT/SIGTERM.
- On shutdown, it closes the listener and calls `CommandHandler.Shutdown()`.
- It flushes AOF or saves RDB depending on config.
- Open client connections are closed cleanly.

## Concurrency Primitives

- `sync.Mutex` in the store for safe map access.
- `sync.RWMutex` in handler for Pub/Sub channel access.
- `sync.Mutex` per connection for serialized writes.
- Goroutines for each client, replication loop, and asynchronous publish.

## Time Complexity

- `Get`: O(1)
- `Set`: O(1)
- `Entries`: O(N)
- `SUBSCRIBE`/`UNSUBSCRIBE`: O(1) per channel
- `PUBLISH`: O(M) where M is number of subscribers in the channel

## Common Interview Questions

- Why use RESP? It is Redis-compatible and simple for byte-framed commands.
- Why not use `map[string]string` directly in command handler? A separate store interface keeps persistence and replication clean.
- Why does `PUBLISH` use goroutines? To avoid blocking the publisher on slow subscribers.
- Why is there both AOF and RDB? AOF is for command persistence and replay, RDB is for faster snapshot loading.
- How does graceful shutdown work? It closes the listener and active connections, then persists data.
