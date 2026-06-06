# Volt Cache

A lightweight, Redis-like in-memory data structure store implemented in Go. This project aims to provide basic Redis functionality with a focus on simplicity.

## Features

- In-memory key-value storage
- Support for basic Redis commands (SET, GET, PING, ECHO)
- Key expiration with millisecond precision
- Leader-Follower replication
- RESP (Redis Serialization Protocol) implementation
- RDB Persistence: Save and load the database to and from an RDB file for data persistence

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/VaidikKindaCodes/VoltCache.git
   ```
2. Navigate to the project directory:
   ```
   cd VoltCache
   ```
3. Build the project:
   ```
   go build -o volt-cache-server.exe cmd/server/main.go
   ```
4. Build the CLI:
   ```
   go build -o volt-cache-cli.exe cmd/cli/main.go
   ```

### Usage

To start the server:

```
./volt-cache-server
```

By default, the server runs on port 6379. You can specify a different port using the `-port` flag:

```
./volt-cache-server -port 6380
```

To run as a follower of another Redis server:

```
./volt-cache-server -replicaof <leader-host> <leader-port>
```

### CLI

This project includes its own CLI written in Go.

To open an interactive prompt:

```
./volt-cache-cli
```

To connect to a different host or port:

```
./volt-cache-cli -host 127.0.0.1 -port 6380
```

To run a single command and exit:

```
./volt-cache-cli SET name Vaidik
./volt-cache-cli GET name
```

The CLI supports quoted arguments in interactive mode, for example:

```
ECHO "hello redis"
SET greeting "hello world" PX 10000 (this time is in milliseconds)
```

It sends commands using RESP, so every server command can be sent through it.

#### RDB Persistence

To enable RDB persistence, specify the directory and filename for the RDB file:
```bash
./go-redis-clone -dir <directory> -dbfilename <filename>
```
The server will automatically load the database from the specified RDB file on startup and save the current state to the RDB file on shutdown.


## Supported Commands

- `PING`: Test the connection
- `ECHO`: Echo the given string
- `SET`: Set a key-value pair (with optional expiration)
- `GET`: Get the value of a key
- `INFO`: Get information about the server
- `REPLCONF`: Used in replication
- `PSYNC`: Used in replication
- `WAIT`: Wait for replication
- `KEYS`: Retrieve all keys that match a given pattern (currently only supports the `*` pattern)
- `CONFIG`: Retrieve server configuration settings (currently supports `CONFIG GET dir` and `CONFIG GET dbfilename`)
- **RDB Persistence:**
   - The server supports loading data from an RDB file and saving the current state to an RDB file.

## Architecture

The project is structured into several packages:

- `cmd`: Entry point of the application
- `handler`: Handles incoming commands
- `storage`: Implements the in-memory store
- `replication`: Manages leader-follower replication
- `domain`: Defines interfaces and common types
- `resp`: Implements the RESP protocol
- `rdb`: Manages RDB file persistence

