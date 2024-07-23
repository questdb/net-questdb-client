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

Use NuGet to add a dependency on this library.

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
    .Column("price", 2615.54)
    .Column("amount", 0.00044)
    .AtAsync(new DateTime(2021, 11, 25, 0, 46, 26));
await sender.SendAsync();
```

### Multi-line send (sync)

```csharp
using var sender = Sender.New("http::addr=localhost:9000;");
for(int i = 0; i < 100; i++)
{
    await sender.Table("trades")
    .Symbol("symbol", "ETH-USD")
    .Symbol("side", "sell")
    .Column("price", 2615.54)
    .Column("amount", 0.00044)
    .AtNowAsync();
}
await sender.SendAsync();
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

#### Flush every 5000 rows

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
using var sender = Sender.New("https::addr=localhost:9009;tls_verify=unsafe_off;username=admin;password=quest;");
```

#### HTTP Authentication (Token)

```csharp
using var sender = Sender.New("https::addr=localhost:9009;tls_verify=unsafe_off;username=admin;token=<bearer token>");
```

#### TCP Authentication

```csharp
using var sender = Sender.New("tcps::addr=localhost:9009;tls_verify=unsafe_off;username=admin;token=NgdiOWDoQNUP18WOnb1xkkEG5TzPYMda5SiUOvT1K0U=;");
```

## Configuration Parameters

These options are set either using a config string, or by initialising QuestDBOptions.

The config string format is:

```
{http/https/tcp/tcps}::addr={host}:{port};key1=val1;key2=val2;keyN=valN;
```

| Name                     | Default                    | Description                                                                                                                                                     |
|--------------------------|----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `protocol` (schema)      | `http`                     | The transport protocol to use. Options are http(s)/tcp(s).                                                                                                      |
| `addr`                   | `localhost:9000`           | The {host}:{port} pair denoting the QuestDB server. By default, port 9000 for HTTP, port 9009 for TCP.                                                          |
| `auto_flush`             | `on`                       | Enables or disables auto-flushing functionality. By default, the buffer will be flushed every 75,000 rows, or every 1000ms, whichever comes first.              |
| `auto_flush_rows`        | `75000 (HTTP)` `600 (TCP)` | The row count after which the buffer will be flushed. Effectively a batch size.                                                                                 |
| `auto_flush_bytes`       | `Int.MaxValue`             | The byte buffer length which when exceeded, will trigger a flush.                                                                                               |
| `auto_flush_interval`    | `1000`                     | The millisecond interval, which once has elapsed, the next row triggers a flush.                                                                                |
| `init_buf_size`          | `65536`                    | The starting byte buffer length. Overflowing this buffer will cause the allocation `init_buf_size` bytes (an additional buffer).                                |
| `max_buf_size`           | `104857600`                | Maximum size of the byte buffer in bytes. If exceeded, an exception will be thrown.                                                                             |
| `username`               |                            | The username for authentication. Used for Basic Authentication and TCP JWK Authentication.                                                                      |
| `password`               |                            | The password for authentication. Used for Basic Authentication.                                                                                                 |
| `token`                  |                            | The token for authentication. Used for Token Authentication and TCP JWK Authentication.                                                                         |
| `token_x`                |                            | Un-used.                                                                                                                                                        |
| `token_y`                |                            | Un-used.                                                                                                                                                        |
| `tls_verify`             | `on`                       | Denotes whether TLS certificates should or should not be verifed. Options are on/unsafe_off.                                                                    |
| `tls_ca`                 |                            | Un-used.                                                                                                                                                        |
| `tls_roots`              |                            | Used to specify the filepath for a custom .pem certificate.                                                                                                     |
| `tls_roots_password`     |                            | Used to specify the filepath for the private key/password corresponding to the `tls_roots` certificate.                                                         |
| `auth_timeout`           | `15000`                    | The time period to wait for authenticating requests, in milliseconds.                                                                                           |
| `request_timeout`        | `10000`                    | Base timeout for HTTP requests before any additional time is added.                                                                                             |
| `request_min_throughput` | `102400`                   | Expected minimum throughput of requests in bytes per second. Used to add additional time to `request_timeout` to prevent large requests timing out prematurely. |
| `retry_timeout`          | `10000`                    | The time period during which retries will be attempted, in milliseconds.                                                                                        |
| `max_name_len`           | `127`                      | The maximum allowed bytes, in UTF-8 format, for column and table names.                                                                                         |

### net-questdb-client specific parameters

| Name           | Default  | Description                                                                           |
|----------------|----------|---------------------------------------------------------------------------------------|
| `own_socket`   | `true`   | Specifies whether the internal TCP data stream will own the underlying socket or not. |
| `pool_timeout` | `120000` | Sets the idle timeout for HTTP connections in SocketsHttpHandler.                     |

## Properties and methods

| Name                                                                                                  | Returns         | Description                                                                |
|-------------------------------------------------------------------------------------------------------|-----------------|----------------------------------------------------------------------------|
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
  to [discuss the scope of your proposal](https://github.com/questdb/c-questdb-client/issues).

- Please write simple code and concise documentation, when appropriate.

## License 📗

[Apache 2.0](https://github.com/questdb/net-questdb-client/tree/main?tab=Apache-2.0-1-ov-file)

Thank you to all the [contributors](https://github.com/questdb/net-questdb-client/graphs/contributors)!
