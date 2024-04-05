using System;
using QuestDB.Ingress;



//    Demonstrates TCPS connection against QuestDB Enterprise

await using var sender =
    Sender.New(
        "tcps::addr=localhost:9009;tls_verify=unsafe_off;username=admin;token=NgdiOWDoQNUP18WOnb1xkkEG5TzPYMda5SiUOvT1K0U=;");
// See: https://questdb.io/docs/reference/api/ilp/authenticate
await sender.Table("trades")
    .Symbol("pair", "USDGBP")
    .Symbol("type", "buy")
    .Column("traded_price", 0.83)
    .Column("limit_price", 0.84)
    .Column("qty", 100)
    .Column("traded_ts", new DateTime(
        2022, 8, 6, 7, 35, 23, 189, DateTimeKind.Utc))
    .At(DateTime.UtcNow);
await sender.Table("trades")
    .Symbol("pair", "GBPJPY")
    .Column("traded_price", 135.97)
    .Column("qty", 400)
    .At(DateTime.UtcNow);
await sender.SendAsync();

