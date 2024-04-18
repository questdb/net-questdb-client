﻿using System;
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
