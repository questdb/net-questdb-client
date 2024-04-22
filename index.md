---
_layout: landing
---

[![questdb-logo](images/banner.svg){width=473 height=114}](https://www.questdb.io)

# QuestDB Client Library for .NET

This is the official .NET client library for [QuestDB](https://questdb.io).

This client library implements QuestDB's variant of
the [InfluxDB Line Protocol](https://questdb.io/docs/reference/api/ilp/overview/),
over HTTP and TCP transports.

ILP is the fastest way to stream data into QuestDB.

# Quickstart

The latest version of the library is 2.0.0.

Please start by [setting up QuestDB](https://questdb.io/docs/reference/api/ilp/overview/)

Then install the [NuGet](https://www.nuget.org/packages/net-questdb-client/) package into your project.

## Overview

The library provides the [Sender](xref:QuestDB.Sender) factory to initialise a client. It is recommended to
use a [config string](docs/conf.md) if possible.

```c#
using System;
using QuestDB;

using var sender =  Sender.New("http::addr=localhost:9000;");
await sender.Table("trades")
    .Symbol("pair", "USDGBP")
    .Symbol("type", "buy")
    .Column("traded_price", 0.83)
    .Column("limit_price", 0.84)
    .Column("qty", 100)
    .Column("traded_ts", new DateTime(
        2022, 8, 6, 7, 35, 23, 189, DateTimeKind.Utc))
    .AtAsync(DateTime.UtcNow);
await sender.Table("trades")
    .Symbol("pair", "GBPJPY")
    .Column("traded_price", 135.97)
    .Column("qty", 400)
    .AtAsync(DateTime.UtcNow);
await sender.SendAsync();

```
