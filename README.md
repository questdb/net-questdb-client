  <a href="https://slack.questdb.io">
    <img src="https://slack.questdb.io/badge.svg" alt="QuestDB community Slack channel"/>
  </a>
  
<div align="center">
  <a href="https://questdb.io/" target="blank"><img alt="QuestDB Logo" src="https://questdb.io/img/questdb-logo-themed.svg" width="305px"/></a>
</div>

<p>&nbsp;</p>

> A .NET client for high performance time-series writes into [QuestDB](https://www.elastic.co/products/app-search).

## Contents

- [Getting started](#getting-started)
- [Usage](#usage)
- [Construction parameters](#construction-parameters)
- [Properties and methods](#properties-and-methods)
- [Examples](#examples)
- [FAQ](#faq-)
- [Contribute](#contribute-)
- [License](#license-)

---

## Getting started

Use NuGet to add a depency on this library.

See: [https://www.nuget.org/packages/net-questdb-client/](https://www.nuget.org/packages/net-questdb-client/)

## Usage

### Basic usage

```c#
using var ls = await LineTcpSender.ConnectAsync("localhost", 9009, tlsMode: TlsMode.Disable);
ls.Table("metric_name")
    .Symbol("Symbol", "value")
    .Column("number", 10)
    .Column("double", 12.23)
    .Column("string", "born to shine")
    .At(new DateTime(2021, 11, 25, 0, 46, 26));
await ls.SendAsync();
```

### Multi-line send

```c#
using var ls = await LineTcpSender.ConnectAsync("localhost", 9009, tlsMode: TlsMode.Disable);
for(int i = 0; i < 1E6; i++)
{
    ls.Table("metric_name")
        .Column("counter", i)
        .AtNow();
}
ls.Send();
```

### Authenticated

```c#
 using var ls = await LineTcpSender.ConnectAsync("localhost", 9009);
 await ls.Authenticate("admin", "NgdiOWDoQNUP18WOnb1xkkEG5TzPYMda5SiUOvT1K0U=");
 ls.Table("metric_name")
    .Column("counter", i)
    .AtNow();
await ls.SendAsync();
```

### Fixed IO Buffer size

```c#
using var ls = await LineTcpSender.ConnectAsync("localhost", 9009, bufferOverflowHandling: BufferOverflowHandling.SendImmediately);
await ls.Authenticate("admin", "NgdiOWDoQNUP18WOnb1xkkEG5TzPYMda5SiUOvT1K0U=");
ls.Table("metric_name")
    .Column("counter", i)
    .AtNow();
await ls.SendAsync();
```

## Construction parameters

| Name                     | Default  | Description                                                                                                                                                                                                                            |
| ------------------------ | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`                   |          | Host or IP address of QuestDB server.                                                                                                                                                                                                   |
| `port`                   |          | QuestDB Port. Default ILP port is 9009                                                                                                                                                                                                 |
| `bufferSize`             | 4096     | Default send buffer size                                                                                                                                                                                                               |
| `bufferOverflowHandling` | `Extend` | There are 2 modes: <br/> - `Extend` will grow input buffer until `Send()` or `SendAsync()` method called<br/> - `SendImmediately` will no extend the IO Buffer and automatically executes `Send()` immediatly when IO Buffer overflown |
| `tslMode`                | `Enable` | There are 3 TSL modes:<br/>- `Enable`. TLS is enabled, server certificate is checked<br/> - `AllowAnyServerCertificate`. TLS enabled, server certificate is not checked<br/>- `Disable`                                                |

## Properties and methods

| Name                                                                 | Description                                                                                                    |
| -------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `AuthenticateAsync(string keyId, string privateKey)`                 | Authenticates with QuestDB certificates                                                                        |
| `Table(string name)`                                                 | Starts new line from table name                                                                                |
| `Symbol(string sybolName, string value)`                             | Symbol column value                                                                                            |
| `Column(string columnName, string / long / double / DateTime value)` | Column name and value                                                                                          |
| `At(DateTime / long timestamp)`                                      | Designated timestamp for the line                                                                              |
| `AtNow()`                                                            | Finishes line leaving QuestDB server to set the timestamp                                                      |
| `Send() / SendAsync() `                                              | Send IO Buffers to QuestDB                                                                                     |
| `CancelLine()`                                                       | Cancels current line. Works only when `bufferOverflowHandling` set to `Extend`                                 |
| `TrimExcessBuffers()`                                                | Trims empty buffers used to grow IO Buffer. Only useful when `bufferOverflowHandling` set to `Extend`          |
| int `WriteTimeout`                                                   | Value, in milliseconds, that determines how long the underlying stream will attempt to write before timing out |
| `IsConnected`                                                        | Indicates if the connection to QuestDB open                                                                    |

## Examples

* [Basic](src/example-basic/Program.cs)
* [Auth + TLS](src/example-auth-tls/Program.cs)

## FAQ 🔮

### Does this client perform both read and write operations?

No. This client is for write only. For querying, see the [Query & SQL overview](https://questdb.io/docs/reference/sql/overview/)

### Where do I report issues with the client?

If something is not working as expected, please open an [issue](https://github.com/questdb/c-questdb-client/issues/new).

### Where can I learn more about QuestDB?

Your best bet is to read the [documentation](https://questdb.io/docs/).

### Where else can I go to get help?

Come visit the [QuestDB community Slack]([https://discuss.elastic.co/c/app-search](https://slack.questdb.io)).

## Contribute 🚀

We welcome contributors to the project. Before you begin, a couple notes...

- Prior to opening a pull request, please create an issue to [discuss the scope of your proposal](https://github.com/questdb/c-questdb-client/issues).
- Please write simple code and concise documentation, when appropriate.

## License 📗

[Apache 2.0](https://github.com/questdb/net-questdb-client/tree/main?tab=Apache-2.0-1-ov-file)

Thank you to all the [contributors](https://github.com/questdb/c-questdb-client/graphs/contributors)!
