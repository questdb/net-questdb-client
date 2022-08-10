# net-questdb-client

.NET client for QuestDB's Influx Line Protocol over TCP.

### Usage

#### Basic usage

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

#### Multi-line send

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

#### Authenticated

```c#
 using var ls = await LineTcpSender.ConnectAsync("localhost", 9009);
 await ls.Authenticate("admin", "NgdiOWDoQNUP18WOnb1xkkEG5TzPYMda5SiUOvT1K0U=");
 ls.Table("metric_name")
    .Column("counter", i)
    .AtNow();
await ls.SendAsync();
```

#### Fixed IO Buffer size

```c#
using var ls = await LineTcpSender.ConnectAsync("localhost", 9009, bufferOverflowHandling: BufferOverflowHandling.SendImmediately);
await ls.Authenticate("admin", "NgdiOWDoQNUP18WOnb1xkkEG5TzPYMda5SiUOvT1K0U=");
ls.Table("metric_name")
    .Column("counter", i)
    .AtNow();
await ls.SendAsync();
```

### Construction parameters

| Name                     | Default  | Description                                                                                                                                                                                                                            |
| ------------------------ | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`                   |          | Host or IP address of QuestDB server.                                                                                                                                                                                                   |
| `port`                   |          | QuestDB Port. Default ILP port is 9009                                                                                                                                                                                                 |
| `bufferSize`             | 4096     | Default send buffer size                                                                                                                                                                                                               |
| `bufferOverflowHandling` | `Extend` | There are 2 modes: <br/> - `Extend` will grow input buffer until `Send()` or `SendAsync()` method called<br/> - `SendImmediately` will no extend the IO Buffer and automatically executes `Send()` immediatly when IO Buffer overflown |
| `tslMode`                | `Enable` | There are 3 TSL modes:<br/>- `Enable`. TLS is enabled, server certificate is checked<br/> - `AllowAnyServerCertificate`. TLS enabled, server certificate is not checked<br/>- `Disable`                                                |

### Properties and methods

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
