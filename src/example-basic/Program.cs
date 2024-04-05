﻿using System;
using System.Threading.Tasks;
using QuestDB.Ingress;

namespace QuestDBDemo;

internal class Program
{
    private static async Task Main(string[] args)
    {
        using var sender = new SenderOld("http::addr=localhost:9000;");
        sender.Table("trades")
            .Symbol("pair", "USDGBP")
            .Symbol("type", "buy")
            .Column("traded_price", 0.83)
            .Column("limit_price", 0.84)
            .Column("qty", 100)
            .Column("traded_ts", new DateTime(
                2022, 8, 6, 7, 35, 23, 189, DateTimeKind.Utc))
            .At(DateTime.UtcNow);
        sender.Table("trades")
            .Symbol("pair", "GBPJPY")
            .Column("traded_price", 135.97)
            .Column("qty", 400)
            .At(DateTime.UtcNow);
        await sender.SendAsync();
    }
}