# Tabularis Redis Plugin

A [Redis](https://redis.io/) plugin for [Tabularis](https://github.com/debba/tabularis), the lightweight database management tool.

This plugin connects to Redis and exposes logical databases (0-15), metadata tables (Hash/RedisJSON row storage), and virtual key views. You can browse all keys via `__redis_keys__`, filter by key pattern or type, and use optional JSON path for ReJSON-RL and hash value previews. Communication is JSON-RPC 2.0 over stdio.

## Table of Contents

- [Features](#features)
- [Connection](#connection)
- [Virtual Tables and Query Syntax](#virtual-tables-and-query-syntax)
- [Installation](#installation)
  - [Automatic (via Tabularis)](#automatic-via-tabularis)
  - [Manual Installation](#manual-installation)
- [How It Works](#how-it-works)
- [Supported Operations](#supported-operations)
- [Building from Source](#building-from-source)
- [Development](#development)
- [License](#license)

## Features

- **Logical databases** — Exposes Redis databases 0-15 (configurable) for selection in the connection form.
- **Metadata tables** — Tabularis metadata is stored in Redis as Hash or RedisJSON; the plugin lists these as tables and supports CRUD.
- **`__redis_keys__`** — Virtual table that lists all keys in the current database via SCAN, with columns: key, type, value (preview), ttl_seconds, key_raw (Base64 for lossless binary key round-trip).
- **Key-pattern virtual tables** — Top-level prefixes (e.g. `stress:*`, `batch:*`) are discovered and exposed as tables like `__keys:stress__`, so you can open a table and see only those keys without writing a WHERE clause.
- **Filtering and sorting** — For `__redis_keys__` and key-pattern tables: `WHERE key = 'x'` / `WHERE key LIKE 'x%'` (SCAN MATCH), `WHERE type = 'hash'` / `WHERE type != 'string'`, `ORDER BY key` / `ORDER BY type` / `ORDER BY value` ASC/DESC, and `LIMIT n`.
- **TTL column** — Each key’s TTL is shown (-1 for no expiry, seconds remaining, or empty if key missing).
- **Pipelined reads** — Type and value preview for the current page are fetched in batches to reduce round-trips.
- **Optional JSON path** — When Redis has the JSON module (Redis Stack), pass `json_path` (e.g. `"$.field"`) in execute_query params: ReJSON-RL keys use JSON.GET with that path for the value column; for hashes, the first path segment (e.g. `$.field` -> `field`) is used with HGET for the value preview.
- **Stream preview** — Stream keys show entry count via XLEN (e.g. `[stream N entries]`) in the value column.
- **Binary keys** — Non-UTF-8 keys are exposed via the `key_raw` column (Base64). Use `key_raw` in insert/update/delete to address binary keys without data loss.
- **Batch CRUD** — `insert_records_batch` and `delete_records_batch` reduce round-trips for bulk inserts and deletes on metadata and virtual key tables.
- **Pub/Sub visibility** — `get_pubsub_channels` lists active channels and subscriber counts (PUBSUB CHANNELS / NUMSUB).
- **Server stats** — `get_server_info` returns parsed Redis INFO (version, memory, clients, stats, keyspace, etc.) as structured JSON.
- **Schema snapshot and ER** — `get_schema_snapshot` and `get_all_columns_batch` include `__redis_keys__` and key-pattern tables. If the app calls `get_all_columns_batch` without a table list, the plugin uses the full table list so the ER diagram works.
- **Execution timing** — Query results include `execution_time_ms` for diagnostics.
- **Structured errors** — JSON-RPC errors include a `data.kind` field (e.g. INVALID_PARAMS, NOT_FOUND, BACKEND_ERROR) for programmatic handling.
- **TLS (rustls)** — Optional TLS via `tls` / `ssl_mode`; uses `rediss://` when enabled.
- **Connection timeouts and health** — Configurable connect/read/write timeouts; ping before reuse with automatic reconnection on failure.
- **Redis Cluster** — Connect to a cluster via `cluster_nodes`; all commands go through the cluster connection.
- **Full SQL on metadata tables** — SELECT with multi-condition WHERE (AND/OR/IN/BETWEEN/LIKE), ORDER BY, LIMIT; aggregates (COUNT/SUM/AVG/MIN/MAX) and GROUP BY; column aliases; INSERT/UPDATE/DELETE via `execute_query`.

## Connection

- **Host** — Redis server host (default `127.0.0.1`).
- **Port** — Redis port (default `6379`).
- **Database** — Logical database index 0-15. Select one in the connection form.
- **Username** — Redis often does not use a username. Tabularis may disable "Load Databases" when the Username field is empty; if you need to load the database list, enter a placeholder (e.g. `default`); it is not sent to Redis.
- **Password** — Optional; used for AUTH when provided.

### TLS and timeouts

Connection params (when supported by the app or connection string) can include:

- **TLS** — Set `tls: true` or use `ssl_mode: "require"` / `"verify-full"` / `"verify-ca"` / `"prefer"` to connect over TLS (`rediss://`). Uses rustls (no OpenSSL dependency).
- **Timeouts** — `connect_timeout_ms`, `read_timeout_ms`, `write_timeout_ms` (defaults 5000, 10000, 10000). The plugin uses `get_connection_with_timeout` and sets read/write timeouts on the connection.
- **Health checks** — Before reusing a cached connection, the plugin pings Redis; on failure it evicts the connection and opens a new one.

### Redis Cluster

For a Redis Cluster deployment, pass `cluster_nodes: ["redis://host1:6379", "redis://host2:6379", ...]` in the connection params. The plugin builds a `ClusterClient` and uses a cluster connection for all commands. Sentinel is not yet supported.

## Virtual Tables and Query Syntax

For the virtual table `__redis_keys__` (and key-pattern tables like `__keys:stress__`), the plugin accepts SQL-like queries and maps them to Redis operations:

| Clause | Example | Behavior |
|--------|---------|----------|
| `WHERE key = 'x'` | `SELECT * FROM __redis_keys__ WHERE key = 'batch'` | SCAN with MATCH `batch*` |
| `WHERE key LIKE 'x%'` | `WHERE key LIKE 'batch%'` | SCAN MATCH `batch*` |
| `WHERE key LIKE '%x%'` | `WHERE key LIKE '%stress%'` | SCAN MATCH `*stress*` |
| `WHERE type = 'hash'` | Only keys of type hash | Filter by TYPE after scan |
| `WHERE type != 'string'` | Exclude string keys | Negated type filter |
| `WHERE value = 'x'` | Value preview equals | Filter by value preview (exact) |
| `WHERE value LIKE 'x%'` | Value preview starts with | Filter by value preview |
| `WHERE value LIKE '%x%'` | Value preview contains | Filter by value preview |
| `ORDER BY key` | ASC (default) or DESC | Sort keys lexicographically |
| `ORDER BY type` | ASC or DESC | Sort by key type, then key |
| `ORDER BY value` | ASC or DESC | Sort by value preview, then key |
| `LIMIT n` | `LIMIT 500` | Cap rows (and page size when no page_size) |

The app may send `limit` and `page`; the plugin uses `limit` as page size when `page_size` is not provided.

### SQL on metadata tables

For **metadata tables** (Hash/RedisJSON row storage), `execute_query` accepts full SQL parsed with [sqlparser](https://crates.io/crates/sqlparser):

- **SELECT** — `SELECT col1, col2 AS alias, * FROM table [WHERE ...] [ORDER BY col ASC|DESC] [LIMIT n]`. WHERE supports multiple conditions with `AND` / `OR`, `IN (v1, v2)`, `BETWEEN a AND b`, and comparisons (`=`, `!=`, `<`, `>`, `<=`, `>=`). `LIKE 'pattern'` is supported (`%` = any sequence, `_` = any character). Column aliases (e.g. `col AS alias`) are reflected in result headers.
- **Aggregates** — `COUNT(*)`, `COUNT(col)`, `SUM(col)`, `AVG(col)`, `MIN(col)`, `MAX(col)` with optional `GROUP BY col1, col2`. Result is one row per group (or one row when no GROUP BY).
- **INSERT / UPDATE / DELETE** — `INSERT INTO table (col1, col2) VALUES ('a', 'b')`, `UPDATE table SET col = 'x' [WHERE ...]`, `DELETE FROM table [WHERE ...]`. DML is executed via the same CRUD layer as the app’s insert/update/delete methods; result returns `affected_rows` and `execution_time_ms`.
- **DDL** — `CREATE [UNIQUE] INDEX name ON table (col1, col2)` and `DROP TABLE [IF EXISTS] table` are supported as before.

Virtual key tables (`__redis_keys__`, `__keys:prefix__`) continue to use the legacy SQL-like parser (WHERE key/type/value, ORDER BY, LIMIT) as described above.

### CRUD on virtual key tables

For `__redis_keys__` and key-pattern tables (`__keys:prefix__`), insert/update/delete operate on real Redis keys (not metadata):

- **Insert:** provide `key` (or `key_raw` for binary keys, Base64) and optionally `value` and `ttl_seconds`. String keys use SET; hash/list/set/zset accept structured value (e.g. hash: object, list: array, set: array of members, zset: array of `[score, member]`). Optional EXPIRE when `ttl_seconds` is a number.
- **Update:** edit the `value` column or `ttl_seconds` (integer; -1 clears expiry). Supported by type: **string** (SET); **hash** (HSET: use `value` as JSON object or column `field:<name>` for one field); **list** (LSET by index, e.g. column `index:<n>` or value `{ "index": n, "value": "..." }`); **set** (SREM + SADD); **zset** (ZADD with score). Unsupported types return a clear error.
- **Delete:** deletes the Redis key (DEL). Use `key` or `key_raw` for binary keys.

Keys whose name is empty or starts with `tabularis:` are rejected to avoid touching plugin metadata. Row-key listing is capped (e.g. 500k keys) to avoid OOM on very large tables; drop_table uses pipelined DEL/SREM for performance.

## Installation

### Automatic (via Tabularis)

If your version of Tabularis supports plugin management, the Redis plugin can be installed from the application (Settings → Available Plugins) when it is listed in the registry.

### Manual Installation

1. Build the plugin (see [Building from Source](#building-from-source)) or download a release for your platform.
2. Copy the executable, `manifest.json`, and `icon.svg` into the Tabularis plugins directory:

| OS | Plugins Directory |
|----|--------------------|
| Linux | `~/.local/share/tabularis/plugins/redis/` |
| macOS | `~/Library/Application Support/com.debba.tabularis/plugins/redis/` |
| Windows | `%APPDATA%\com.debba.tabularis\plugins\redis\` |

3. Restart Tabularis.

## How It Works

The plugin is a standalone Rust binary that communicates with Tabularis through **JSON-RPC 2.0 over stdio**:

1. Tabularis spawns the plugin as a child process.
2. Requests are sent as newline-delimited JSON-RPC messages to the plugin’s stdin.
3. The plugin connects to Redis using the connection params and writes responses to stdout.

One Redis connection is used for the session. Key discovery (SCAN, TYPE, value preview, TTL) uses pipelining where possible to limit round-trips.

## Supported Operations

| Method | Description |
|--------|-------------|
| `test_connection` | Ping Redis |
| `get_databases` | List logical databases (0-15) |
| `get_schemas` | Returns `[]` (Redis has no schemas) |
| `get_tables` | List metadata tables plus `__redis_keys__` and key-pattern tables (`__keys:prefix__`) |
| `get_columns` | Column metadata for a table; for `__redis_keys__` / key-pattern tables returns key, type, value, ttl_seconds, key_raw |
| `get_foreign_keys` | Returns `[]` |
| `get_indexes` | Index metadata for metadata tables |
| `get_pubsub_channels` | List active Pub/Sub channels with subscriber counts |
| `get_server_info` | Redis server info (INFO ALL) as structured JSON |
| `execute_query` | Run SQL: SELECT (with WHERE/ORDER BY/LIMIT, aggregates, GROUP BY, aliases, LIKE on metadata tables), INSERT/UPDATE/DELETE (DML), CREATE INDEX, DROP TABLE; for `__redis_keys__` / key-pattern tables supports WHERE/ORDER BY/LIMIT as in Virtual Tables |
| `insert_record` | Insert row into a metadata table; for virtual key tables creates a Redis key (string/hash/list/set/zset) |
| `insert_records_batch` | Bulk insert: `table` + `rows`; returns affected count |
| `update_record` | Update row by primary key; for virtual key tables supports `value` and `ttl_seconds` (string/hash/list/set/zset) |
| `delete_record` | Delete row by primary key; for virtual key tables deletes the Redis key (DEL) |
| `delete_records_batch` | Bulk delete: `table` + `primary_keys`; returns deleted count |
| `drop_table` | Remove a metadata table and all its data (pipelined); virtual tables cannot be dropped |
| `get_schema_snapshot` | Full schema: all tables (including virtual) and columns |
| `get_all_columns_batch` | Columns for requested tables, or all tables if none specified |
| `get_all_foreign_keys_batch` | Foreign keys (empty for Redis) |
| `get_create_table_sql` | DDL stub |
| `get_add_column_sql` | DDL stub |
| `get_alter_column_sql` | ALTER COLUMN DDL (alter_column capability enabled) |
| `get_create_index_sql` | CREATE INDEX DDL |
| `get_create_foreign_key_sql` | DDL stub |
| `drop_index` | Drop an index |
| `drop_foreign_key` | No-op (no FKs) |

Views and routines are not supported; the plugin returns empty or stub responses for those methods.

## Building from Source

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (edition 2021)
- A running Redis instance (for integration tests)

### Build

```bash
cargo build --release
```

The binary will be at `target/release/tabularis-redis-plugin`. Copy it, `manifest.json`, and `icon.svg` into the Tabularis plugins directory (see [Manual Installation](#manual-installation)).

## Development

### Testing the Plugin

A test binary simulates Tabularis by sending JSON-RPC requests to the plugin over stdio:

```bash
cargo run --bin test_plugin
```

Use a Redis instance with test data (e.g. keys like `batch:*`, `stress:*`) to exercise key-pattern tables and type filtering.

### Tech Stack

- **Language:** Rust (edition 2021)
- **Redis client:** [redis](https://crates.io/crates/redis) 1.x (TLS via rustls, cluster support)
- **SQL parsing:** [sqlparser](https://crates.io/crates/sqlparser) for metadata-table SELECT/DML/DDL
- **Serialization:** serde + serde_json
- **Protocol:** JSON-RPC 2.0 over stdio

## License

Apache License 2.0
