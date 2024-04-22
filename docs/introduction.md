  <a href="https://slack.questdb.io">
    <img src="https://slack.questdb.io/badge.svg" alt="QuestDB community Slack channel"/>
  </a>

<div align="center">
  <a href="https://questdb.io/" target="blank"><img alt="QuestDB Logo" src="https://questdb.io/img/questdb-logo-themed.svg" width="305px"/></a>
</div>

This is the official .NET client library for [QuestDB](https://questdb.io/).

This library implements QuestDB's variant of
the [InfluxDB Line Protocol](https://questdb.io/docs/reference/api/ilp/overview/) (ILP)
over HTTP and TCP transports.

ILP provides the fastest way to insert data into QuestDB.

## Quickstart

The latest version of the library
is [2.0.0](https://www.nuget.org/packages/net-questdb-client/) ([changelog](https://github.com/questdb/net-questdb-client/releases/tag/v2.0.0))

The NuGet package can be installed using the dotnet CLI:

```shell
dotnet add package net-questdb-client
```

## Ingestion

For detailed information, see [ingestion](ingestion.md).

`Sender` is single-threaded, and uses a single connection to the database.

If you want to send in parallel, you can use multiple senders and standard async tasking.

### Basic usage

```c#
using var sender = Sender.New("http::addr=localhost:9000;");
await sender.Table("metric_name")
    .Symbol("Symbol", "value")
    .Column("number", 10)
    .Column("double", 12.23)
    .Column("string", "born to shine")
    .AtAsync(new DateTime(2021, 11, 25, 0, 46, 26));
await sender.SendAsync();
```

### Multi-line send (sync)

```c#
using var sender = Sender.New("http::addr=localhost:9000;auto_flush=off;");
for(int i = 0; i < 100; i++)
{
    sender.Table("metric_name")
        .Column("counter", i)
        .AtNow();
}
sender.Send();
```