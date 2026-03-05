# tabularis-redis-plugin-rust

A [Redis](https://redis.io/) plugin for [Tabularis](https://github.com/debba/tabularis), the lightweight database management tool.

This plugin connects to Redis and exposes logical databases, metadata tables (Hash/RedisJSON row storage), and virtual key views — browse keys by prefix or type, inspect hashes, lists, sets, sorted sets, and streams, all through a SQL-like interface. Communication is JSON-RPC 2.0 over stdio.

## Table of Contents

- [Features](#features)
- [Connection](#connection)
- [Installation](#installation)
  - [Automatic (via Tabularis)](#automatic-via-tabularis)
  - [Manual Installation](#manual-installation)
- [How It Works](#how-it-works)
- [Supported Operations](#supported-operations)
- [Building from Source](#building-from-source)
- [Development](#development)
- [License](#license)

## Features

- **Logical databases** — exposes Redis databases 0-15 for selection in the connection form.
- **Metadata tables** — store structured row data in Redis as Hash or RedisJSON; full SQL with WHERE, ORDER BY, LIMIT, aggregates, and GROUP BY.
- **Virtual key browser** — `__redis_keys__` lists all keys with type, value preview, and TTL; key-pattern tables (`__keys:prefix__`) group keys by prefix automatically.
- **Type-specific tables** — `hashes`, `lists`, `sets`, `zsets`, and `streams` expose key contents by data structure.
- **Inline editing** — insert, update, and delete records on both metadata and virtual key tables directly from the Tabularis data grid.
- **Full SQL on metadata** — SELECT with AND/OR/IN/BETWEEN/LIKE, column aliases, INSERT/UPDATE/DELETE, CREATE INDEX, DROP TABLE via `execute_query`.
- **TLS support** — optional TLS via rustls (no OpenSSL dependency).
- **Pipelined reads** — type, value preview, and TTL fetched in batches to reduce round-trips.
- **Binary key support** — non-UTF-8 keys exposed via `key_raw` (Base64) for lossless round-trip.
- **Pub/Sub visibility** — list active channels and subscriber counts.
- **Server stats** — parsed Redis INFO (version, memory, clients, keyspace) as structured JSON.
- **Cross-platform** — pre-built binaries for Linux (x86_64, aarch64), macOS (x86_64, aarch64), and Windows (x86_64).

## Connection

| Parameter | Description | Default |
| --- | --- | --- |
| **Host** | Redis server host | `127.0.0.1` |
| **Port** | Redis port | `6379` |
| **Database** | Logical database index (0-15) | `0` |
| **Username** | ACL user (Redis 6+) | `default` |
| **Password** | AUTH password | — |
| **TLS** | `tls: true` or `ssl_mode` to connect over TLS (`rediss://`) | off |
| **Timeouts** | `connect_timeout_ms`, `read_timeout_ms`, `write_timeout_ms` | 5000, 10000, 10000 |

## Installation

### Automatic (via Tabularis)

If your version of Tabularis supports plugin management, the Redis plugin can be installed from the application (Settings → Available Plugins) when it is listed in the registry.

### Manual Installation

1. Download the latest release for your platform from the [Releases page](https://github.com/tabularis-plugins/tabularis-redis-plugin/releases).
2. Extract the archive.
3. Copy `tabularis-redis-plugin-rust` (or `tabularis-redis-plugin-rust.exe` on Windows), `manifest.json`, and `icon.svg` into the Tabularis plugins directory:
4. Restart Tabularis to load the plugin.

| OS | Plugins Directory |
| --- | --- |
| **Linux** | `~/.local/share/tabularis/plugins/redis-rust/` |
| **macOS** | `~/Library/Application Support/com.debba.tabularis/plugins/redis-rust/` |
| **Windows** | `%APPDATA%\com.debba.tabularis\plugins\redis-rust\` |

## How It Works

The plugin is a standalone Rust binary that communicates with Tabularis through **JSON-RPC 2.0 over stdio**:

1. Tabularis spawns the plugin as a child process.
2. Requests are sent as newline-delimited JSON-RPC messages to the plugin's `stdin`.
3. Responses are written to `stdout` in the same format.

One Redis connection is cached per session. Key discovery uses SCAN with pipelining to limit round-trips.

## Supported Operations

| Method | Description |
| --- | --- |
| `test_connection` | Ping Redis |
| `get_databases` | List logical databases (0-15) |
| `get_schemas` | Returns empty (Redis has no schemas) |
| `get_tables` | List metadata tables, virtual key tables, and type-specific tables |
| `get_columns` | Get column metadata for a table |
| `get_foreign_keys` | Returns empty |
| `get_indexes` | Get indexes for metadata tables |
| `get_pubsub_channels` | List active Pub/Sub channels with subscriber counts |
| `get_server_info` | Redis INFO as structured JSON |
| `execute_query` | Execute SQL with pagination support |
| `insert_record` | Insert a row or create a Redis key |
| `insert_records_batch` | Bulk insert with pipelining |
| `update_record` | Update a row or key value/TTL |
| `delete_record` | Delete a row or Redis key |
| `delete_records_batch` | Bulk delete with pipelining |
| `drop_table` | Drop a metadata table and its data |
| `get_schema_snapshot` | Full schema dump in one call |
| `get_all_columns_batch` | All columns for all tables |
| `get_all_foreign_keys_batch` | All foreign keys (empty for Redis) |
| `get_create_table_sql` | Generate CREATE TABLE DDL |
| `get_add_column_sql` | Generate ADD COLUMN DDL |
| `get_alter_column_sql` | Generate ALTER COLUMN DDL |
| `get_create_index_sql` | Generate CREATE INDEX DDL |
| `get_create_foreign_key_sql` | DDL stub |
| `drop_index` | Drop an index |
| `drop_foreign_key` | No-op |

## Building from Source

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (edition 2021)
- A running Redis instance (for integration tests)

### Build

```bash
cargo build --release
```

The binary will be located at `target/release/tabularis-redis-plugin-rust`.

### Install Locally

A convenience script is provided to build and copy the plugin to the Tabularis plugins directory:

```bash
./sync.sh
```

## Development

### Testing the Plugin

A test binary simulates Tabularis by sending JSON-RPC requests to the plugin over stdio:

```bash
cargo run --bin test_plugin
```

### Performance baselines

```bash
TABULARIS_PLUGIN_PERF=1 cargo run --bin test_plugin
```

Timings for virtual key queries and pagination are printed to stderr.

### Tech Stack

- **Language:** Rust (edition 2021)
- **Redis client:** [redis](https://crates.io/crates/redis) 1.x (TLS via rustls, cluster support)
- **SQL parsing:** [sqlparser](https://crates.io/crates/sqlparser) for metadata-table queries
- **Serialization:** serde + serde_json
- **Protocol:** JSON-RPC 2.0 over stdio

## License

Apache License 2.0
