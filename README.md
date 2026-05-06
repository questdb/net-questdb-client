  <a href="https://slack.questdb.io">
    <img src="https://slack.questdb.io/badge.svg" alt="QuestDB community Slack channel"/>
  </a>

<div align="center">
  <a href="https://questdb.io/" target="blank"><img alt="QuestDB Logo" src="https://questdb.io/img/questdb-logo-themed.svg" width="305px"/></a>
</div>

<p>&nbsp;</p>

> A .NET client for high performance time-series writes into [QuestDB](https://questdb.io).

## Contents

- [Getting started](#getting-started)
- [Usage](#usage)
- [Configuration parameters](#configuration-parameters)
- [Properties and methods](#properties-and-methods)
- [Examples](#examples)
- [FAQ](#faq-)
- [Contribute](#contribute-)
- [License](#license-)

---

## Getting started

Use NuGet to add a dependency on this library: `dotnet add package net-questdb-client`

See: [https://www.nuget.org/packages/net-questdb-client/](https://www.nuget.org/packages/net-questdb-client/)

## Usage

`Sender` is single-threaded, and uses a single connection to the database.

If you want to send in parallel, you can use multiple senders and standard async tasking.

See more in-depth documentation [here](https://questdb.io/docs/clients/ingest-dotnet/).

### Basic usage

```csharp
using var sender =  Sender.New("http::addr=localhost:9000;");
await sender.Table("trades")
    .Symbol("symbol", "ETH-USD")
    .Symbol("side", "sell")
    .Column("price", 2615.54m)
    .Column("amount", 0.00044)
    .AtAsync(new DateTime(2021, 11, 25, 0, 46, 26));
await sender.SendAsync();
```

### Multi-line send (sync)

```csharp
using var sender = Sender.New("http::addr=localhost:9000;");
for(int i = 0; i < 100; i++)
{
    sender.Table("trades")
      .Symbol("symbol", "ETH-USD")
      .Symbol("side", "sell")
      .Column("price", 2615.54m)
      .Column("amount", 0.00044)
      .At(DateTime.UtcNow);
}
sender.Send();
```

### Auto-Flush

By default, the client will flush every 75,000 rows (HTTP) or 600 rows (TCP).

Alternatively, it will flush every 1000ms.

This is equivalent to a config string of:

```csharp
using var sender = Sender.New("http:addr=localhost:9000;auto_flush=on;auto_flush_rows=75000;auto_flush_interval=1000;");
```

A final flush or send should always be used, as auto flush is not guaranteed to send all pending data before
the sender is disposed.

#### Flush every 1000 rows or every 1 second

```csharp
using var sender = Sender.New("http::addr=localhost:9000;auto_flush=on;auto_flush_rows=1000;");
```

#### Flush every 1000 rows (no time-based trigger)

```csharp
using var sender = Sender.New("http::addr=localhost:9000;auto_flush=on;auto_flush_rows=1000;auto_flush_interval=off;");
```

#### Flush after 5 seconds

```csharp
using var sender = Sender.New("http::addr=localhost:9000;auto_flush=on;auto_flush_rows=off;auto_flush_interval=5000;");
```

#### Flush only when buffer is 4kb

```csharp
using var sender = Sender.New("http::addr=localhost:9000;auto_flush=on;auto_flush_bytes=4096;auto_flush_rows=off;auto_flush_interval=off;");
```

### Authenticated

#### HTTP Authentication (Basic)

```csharp
using var sender = Sender.New("https::addr=localhost:9000;tls_verify=unsafe_off;username=admin;password=quest;");
```

#### HTTP Authentication (Token)

```csharp
using var sender = Sender.New("https::addr=localhost:9000;tls_verify=unsafe_off;username=admin;token=<bearer token>");
```

#### TCP Authentication

```csharp
using var sender = Sender.New("tcps::addr=localhost:9009;tls_verify=unsafe_off;username=admin;token=NgdiOWDoQNUP18WOnb1xkkEG5TzPYMda5SiUOvT1K0U=;");
```

### WebSocket / QWP (columnar binary, requires .NET 7+)

The `ws::` and `wss::` schemes use the QuestDB columnar binary protocol (QWP) over a WebSocket. Compared to `http::` / `tcp::` (text ILP), QWP delivers higher sustained throughput at lower CPU cost — payloads are smaller because columns share schema once per connection.

```csharp
using var sender = Sender.New("ws::addr=localhost:9000;");
sender.Table("trades")
      .Symbol("symbol", "ETH-USD")
      .Column("price", 2615.54)
      .Column("amount", 0.00044)
      .At(DateTime.UtcNow);
sender.Send();
```

`wss::` adds TLS:

```csharp
using var sender = Sender.New("wss::addr=q.example.com:443;username=admin;password=quest;");
```

#### Pipelined async mode

By default the WebSocket sender pipelines up to 128 batches in flight. Use the `*Async` API to keep the calling thread free while frames are on the wire:

```csharp
await using var sender = Sender.New("ws::addr=localhost:9000;");

for (var i = 0; i < 1_000_000; i++)
{
    sender.Table("trades")
          .Symbol("symbol", "ETH-USD")
          .Column("price", 2615.54);
    await sender.AtAsync(DateTime.UtcNow);
}

await sender.SendAsync();
```

`in_flight_window` controls the pipeline depth; valid range is `2..N`. The WebSocket transport is async-only — `in_flight_window=1` is rejected.

#### Multi-address failover

Pass a comma-separated list to `addr=` to enable role-aware failover across multiple QuestDB nodes:

```csharp
using var sender = Sender.New("ws::addr=node-a:9000,node-b:9000,node-c:9000;");
```

The sender walks the list in order. If a node returns `503 + X-QuestDB-Role`, it is skipped — `REPLICA` is shelved as structurally unwritable, `PRIMARY_CATCHUP` is treated as transiently unavailable, and the sender retries them after a backoff (`PRIMARY_CATCHUP` is preferred over `REPLICA` on retry since it tends to recover quickly). `PRIMARY` and `STANDALONE` accept the upgrade. Auth failures (`401`/`403`) remain terminal and do not fall through to the next address.

In SF mode (`sf_dir=...`), the same rotation applies on every reconnect — when the active node loses its primary role, the engine's reconnect loop walks past the demoted node and picks up wherever the new primary lands. Backoff applies once per full round, not per host attempt.

#### Examples

Working sample projects (drop-in copies):

- [`src/example-qwp-ingest`](src/example-qwp-ingest/Program.cs) — minimal `ws::` sender.
- [`src/example-qwp-ingest-auth-tls`](src/example-qwp-ingest-auth-tls/Program.cs) — `wss::` with Basic auth and a custom TLS root.
- [`src/example-qwp-query`](src/example-qwp-query/Program.cs) — `ws::` query client demo (basic / binds / errors).

Run with `dotnet run --project src/example-qwp-ingest`.

#### Gorilla timestamp compression

Set `gorilla=on` to enable delta-of-delta compression for timestamp columns. Best fit for steady-tick streams (sensor readings, evenly spaced ticks). The encoder transparently falls back to uncompressed per column when DoDs overflow int32:

```csharp
using var sender = Sender.New("ws::addr=localhost:9000;gorilla=on;");
```

For irregular timestamps (event-driven workloads) Gorilla can be larger than uncompressed; benchmark with your actual data before enabling.

#### Durable acknowledgements

Set `request_durable_ack=on` to opt into per-table object-store watermarks. The sender exposes them via the `IQwpWebSocketSender` interface:

```csharp
using var sender = Sender.New("ws::addr=localhost:9000;request_durable_ack=on;");
sender.Table("trades").Column("v", 1L).At(DateTime.UtcNow);
sender.Send();

if (sender is IQwpWebSocketSender ws)
{
    long committed = ws.GetHighestAckedSeqTxn("trades");   // -1 if none yet
    long durable   = ws.GetHighestDurableSeqTxn("trades"); // requires the opt-in
    ws.Ping();                                             // wait for in-flight to drain
}
```

#### Defaults

| Knob                          | WebSocket default | HTTP / TCP for comparison       |
|-------------------------------|-------------------|---------------------------------|
| Default port                  | 9000              | 9000 (HTTP), 9009 (TCP)         |
| Endpoint path                 | `/write/v4`       | `/write` (HTTP)                 |
| `auto_flush_rows`             | 1000              | 75000 (HTTP), 600 (TCP)         |
| `auto_flush_interval`         | 100 ms            | 1000 ms                         |
| `auto_flush_bytes`            | `int.MaxValue`    | `int.MaxValue`                  |
| `in_flight_window`            | 128               | n/a                             |
| `close_timeout`               | 5000 ms           | n/a                             |
| `max_schemas_per_connection`  | 65535             | n/a                             |
| `request_durable_ack`         | `off`             | n/a                             |
| `gorilla`                     | `off`             | n/a                             |

#### Store-and-forward (durable client buffer)

Set `sf_dir=/path/to/dir` to opt into the on-disk store-and-forward buffer. Outgoing batches are persisted to mmap'd segments before going on the wire, and a background I/O thread silently reconnects + replays whatever's still on disk if the network drops or the process restarts. User code is shielded from transient disconnects; a `Send` can still surface terminal errors when the bounded retry / drain budgets (`sf_append_deadline_millis`, `reconnect_max_duration_millis`) expire.

```csharp
using var sender = Sender.New(
    "ws::addr=localhost:9000;sf_dir=/var/lib/myapp/qwp;sender_id=ingester-01;");
```

Each sender owns one slot directory at `<sf_dir>/<sender_id>/`. `sender_id` defaults to `"default"` and **must be unique per process** sharing the same `sf_dir`. To reclaim slots left by a crashed sibling process, set `drain_orphans=on`:

```csharp
using var sender = Sender.New(
    "ws::addr=localhost:9000;sf_dir=/var/lib/myapp/qwp;sender_id=ingester-01;drain_orphans=on;");
```

SF caveats:

- **Local filesystem only.** `FileShare.None` advisory locking does not behave reliably on NFS or other networked filesystems. Point `sf_dir` at a local disk.
- **SF frames are larger.** The sender uses self-sufficient encoding (every frame carries the full schema + symbol dictionary) so any frame can be replayed against a fresh server connection. Expect somewhat larger payload-per-batch vs non-SF mode.
- Only `sf_durability=memory` is supported in v1 (matches Java).

#### Caveats

- **`ws::` / `wss::` requires .NET 7 or later.** HTTP and TCP transports keep working on net6.0.
- The transport disables HTTP proxies by default; long-lived WebSocket connections rarely survive them. Override with `proxy=system` to use the system proxy or `proxy=http://host:port` for an explicit URI.
- Multi-address `addr=h1,h2,...` is supported with role-aware failover (see "Multi-address failover" above).
- **Use long-lived senders.** WebSocket upgrade is significantly more expensive than an HTTP POST; create the sender once at startup and keep it alive for the process lifetime, rather than per request.
- **Connect-string quoting differs from Java/Go.** This client parses connect strings via `System.Data.Common.DbConnectionStringBuilder`, which uses ADO.NET-style `'`/`"` quoting with internal doubling. Java and Go implement `;;` → `;` escaping. A connect string with a literal semicolon in a value (rare; mostly passwords or paths) parses differently across clients — quote the value or escape per the local parser.

### Multiple database endpoints

The client can be configured with multiple `addr` entries pointing to different instances of QuestDB.

This is **not** for publishing data concurrently to multiple databases.

Rather, this allows you to configure a backup database where data will be sent to in the event the primary database is unavailable.

The swap happens transparently within a given `retry_timeout`, and is performed in a round-robin fashion (try the next endpoint and write if it is available). Once a new endpoint is selected, it continues to be used for the lifetime of that `Sender`.

## Configuration Parameters

These options are set either using a config string, or by initialising QuestDBOptions.

The config string format is:

```
{http/https/tcp/tcps}::addr={host}:{port};key1=val1;key2=val2;keyN=valN;
```

| Name                     | Default                    | Description                                                                                                                                                                                                                   |
| ------------------------ | -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `protocol` (schema)      | `http`                     | The transport protocol to use. Options are http(s)/tcp(s)/ws(s). `ws::` / `wss::` requires .NET 7+.                                                                                                                            |
| `addr`                   | `localhost:9000`           | The {host}:{port} pair denoting the QuestDB server. Default port 9000 for HTTP and ws/wss, 9009 for TCP.                                                                                                                       |
| `auto_flush`             | `on`                       | Enables or disables auto-flushing functionality. By default, the buffer will be flushed every 75,000 rows, or every 1000ms, whichever comes first.                                                                            |
| `auto_flush_rows`        | `75000 (HTTP)` `600 (TCP)` | The row count after which the buffer will be flushed. Effectively a batch size.                                                                                                                                               |
| `auto_flush_bytes`       | `Int.MaxValue`             | The byte buffer length which when exceeded, will trigger a flush.                                                                                                                                                             |
| `auto_flush_interval`    | `1000`                     | The millisecond interval, which once has elapsed, the next row triggers a flush.                                                                                                                                              |
| `init_buf_size`          | `65536`                    | The starting byte buffer length. Overflowing this buffer will cause the allocation `init_buf_size` bytes (an additional buffer).                                                                                              |
| `max_buf_size`           | `104857600`                | Maximum size of the byte buffer in bytes. If exceeded, an exception will be thrown.                                                                                                                                           |
| `username`               |                            | The username for authentication. Used for Basic Authentication and TCP JWK Authentication.                                                                                                                                    |
| `password`               |                            | The password for authentication. Used for Basic Authentication.                                                                                                                                                               |
| `token`                  |                            | The token for authentication. Used for Token Authentication and TCP JWK Authentication.                                                                                                                                       |
| `tls_verify`             | `on`                       | Denotes whether TLS certificates should or should not be verified. Options are on/unsafe_off.                                                                                                                                  |
| `tls_roots`              |                            | Used to specify the filepath for a custom .pem certificate.                                                                                                                                                                   |
| `tls_roots_password`     |                            | Used to specify the filepath for the private key/password corresponding to the `tls_roots` certificate.                                                                                                                       |
| `auth_timeout`           | `15000`                    | The time period to wait for authenticating requests, in milliseconds.                                                                                                                                                         |
| `request_timeout`        | `30000`                    | Base timeout for HTTP requests before any additional time is added.                                                                                                                                                           |
| `request_min_throughput` | `102400`                   | Expected minimum throughput of requests in bytes per second. Used to add additional time to `request_timeout` to prevent large requests timing out prematurely.                                                               |
| `retry_timeout`          | `10000`                    | The time period during which retries will be attempted, in milliseconds.                                                                                                                                                      |
| `max_name_len`           | `127`                      | The maximum allowed bytes, in UTF-8 format, for column and table names.                                                                                                                                                       |
| `protocol_version`       |                            | Explicitly specifies the version of InfluxDB Line Protocol to use for sender. Valid options are:<br>• protocol_version=1<br>• protocol_version=2<br>• protocol_version=3<br>• protocol_version=auto (default, if unspecified) |

### WebSocket / QWP-only parameters

| Name                              | Default      | Description                                                                                              |
| --------------------------------- | ------------ | -------------------------------------------------------------------------------------------------------- |
| `in_flight_window`                | `128`        | Max pipelined batches awaiting ACK. Minimum is `2` — `in_flight_window=1` is rejected.                   |
| `close_timeout`                   | `5000` ms    | Per-flush ACK-drain timeout, applied to `Send` and `Dispose`.                                            |
| `max_schemas_per_connection`      | `65535`      | Per-connection cap on distinct schema IDs. Hitting it requires recreating the sender.                    |
| `gorilla`                         | `off`        | `on` / `off` — enables Gorilla DoD compression on timestamp columns.                                     |
| `request_durable_ack`             | `off`        | `on` / `off` — opts into per-table object-store ACK watermarks (cast to `IQwpWebSocketSender`).         |
| `sf_dir`                          |              | Path to a local directory enabling store-and-forward. Sets the SF stack on this sender.                  |
| `sender_id`                       | `default`    | Slot identifier under `<sf_dir>/<sender_id>/`. Must be unique per process sharing the same `sf_dir`.     |
| `sf_max_bytes`                    | `4194304`    | Per-segment rotation threshold in bytes (default 4 MiB).                                                 |
| `sf_max_total_bytes`              | `10 GiB` with `sf_dir`, `128 MiB` otherwise | Hard cap on total disk usage; back-pressures the producer when exceeded.            |
| `sf_durability`                   | `memory`     | Durability mode. Only `memory` is supported in v1.                                                       |
| `sf_append_deadline_millis`       | `30000`      | Max wait when the disk cap is hit before `Send` throws.                                                  |
| `reconnect_initial_backoff_millis`| `100`        | Starting backoff for reconnect attempts.                                                                 |
| `reconnect_max_backoff_millis`    | `5000`       | Cap on per-attempt backoff.                                                                              |
| `reconnect_max_duration_millis`   | `300000`     | Total per-outage budget; sender becomes terminal if exceeded.                                            |
| `initial_connect_retry`           | `off`        | `on` makes the first connect honour the same backoff loop. Default is "fail fast on first connect".      |
| `close_flush_timeout_millis`      | `5000`       | Max wait at `Dispose` for the SF engine to drain. `0` or `-1` for fast close.                            |
| `drain_orphans`                   | `off`        | `on` adopts unlocked sibling slots on startup and drains them in the background.                         |
| `max_background_drainers`         | `4`          | Cap on concurrent orphan-drain workers.                                                                  |

### Protocol Version

Behavior details:

| Value  | Behavior                                                                                                                                                    |
| ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1      | - Plain text serialization<br>- Compatible with InfluxDB servers<br>- No array type support                                                                 |
| 2      | - Binary encoding for double arrays<br>- Full support for array                                                                                             |
| 3      | - Support for decimal                                                                                                                                       |
| `auto` | - **HTTP/HTTPS**: Auto-detects server capability during handshake (supports version negotiation)<br>- **TCP/TCPS**: Defaults to version 1 for compatibility |

### net-questdb-client specific parameters

| Name           | Default  | Description                                                                           |
| -------------- | -------- | ------------------------------------------------------------------------------------- |
| `own_socket`   | `true`   | Specifies whether the internal TCP data stream will own the underlying socket or not. |
| `pool_timeout` | `120000` | Sets the idle timeout for HTTP connections in SocketsHttpHandler.                     |

## Properties and methods

| Name                                                                                                  | Returns         | Description                                                                |
| ----------------------------------------------------------------------------------------------------- | --------------- | -------------------------------------------------------------------------- |
| `Length`                                                                                              | `int`           | Current length in bytes of the buffer (not capacity!)                      |
| `RowCount`                                                                                            | `int`           | Current row count of the buffer                                            |
| `LastFlush`                                                                                           | `DateTime`      | Returns the UTC DateTime of the last flush sending data to the server.     |
| `WithinTransaction`                                                                                   | `bool`          | Whether or not the Sender is currently in a transactional state.           |
| `Transaction(ReadOnlySpan<char>)`                                                                     | `ISender`       | Starts a new transaction for the table.                                    |
| `Commit() / CommitAsync()`                                                                            | `void` / `Task` | Commits the current transaction.                                           |
| `Rollback()`                                                                                          | `void`          | Rolls back the current unsent transaction.                                 |
| `Table(ReadOnlySpan<char>)`                                                                           | `ISender`       | Sets the table name for the next row.                                      |
| `Column(ReadOnlySpan<char>, ReadOnlySpan<char> / string / long / double / DateTime / DateTimeOffset)` | `ISender`       | Specify column name and value                                              |
| `Column(ReadOnlySpan<char>, string? / long? / double? / DateTime? / DateTimeOffset?)`                 | `ISender`       | Specify column name and value                                              |
| `Symbol(ReadOnlySpan<char>, ReadOnlySpan<char> / string)`                                             | `ISender`       | Specify a symbol column name and value                                     |
| `At(DateTime / DateTimeOffset / long, CancellationToken)`                                             | `void`          | Designated timestamp for the line. May flush data according to auto-flush. |
| `AtAsync(DateTime / DateTimeOffset / long, CancellationToken)`                                        | `ValueTask`     | Designated timestamp for the line. May flush data according to auto-flush. |
| `AtNow(CancellationToken)`                                                                            | `void`          | Finishes line, leaving the QuestDB server to set the timestamp             |
| `AtNowAsync(CancellationToken)`                                                                       | `ValueTask`     | Finishes line, leaving the QuestDB server to set the timestamp             |
| `Send() / SendAsync()`                                                                                | `void` / `Task` | Send IO Buffers to QuestDB                                                 |
| `CancelRow()`                                                                                         | `void`          | Cancels current row.                                                       |
| `Truncate()`                                                                                          | `void`          | Trims empty buffers.                                                       |
| `Clear()`                                                                                             | `void`          | Clears the sender's buffer.                                                |

## Examples

* [Basic](src/example-basic/Program.cs)
* [Auth + TLS](src/example-auth-tls/Program.cs)

## FAQ 🔮

### Does this client perform both read and write operations?

No. This client is for writing data only. For querying, see
the [Query & SQL overview](https://questdb.io/docs/reference/sql/overview/)

### Where do I report issues with the client?

If something is not working as expected, please open
an [issue](https://github.com/questdb/net-questdb-client/issues/new).

### Where can I learn more about QuestDB?

Your best bet is to read the [documentation](https://questdb.io/docs/).

### Where else can I go to get help?

Come visit the [QuestDB community Slack](https://slack.questdb.io).

## Contribute 🚀

We welcome contributors to the project. Before you begin, a couple notes...

- Prior to opening a pull request, please create an issue
  to [discuss the scope of your proposal](https://github.com/questdb/net-questdb-client/issues).

- Please write simple code and concise documentation, when appropriate.

## License 📗

[Apache 2.0](https://github.com/questdb/net-questdb-client/tree/main?tab=Apache-2.0-1-ov-file)

Thank you to all the [contributors](https://github.com/questdb/net-questdb-client/graphs/contributors)!
