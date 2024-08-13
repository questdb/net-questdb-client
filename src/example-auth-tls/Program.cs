using System;
using QuestDB;


//    Demonstrates TCPS connection against QuestDB Enterprise
//    Disabling tls verification. Note this is not a best practice in production

using var sender =
    Sender.New(
        "tcps::addr=localhost:9009;tls_verify=unsafe_off;username=admin;token=NgdiOWDoQNUP18WOnb1xkkEG5TzPYMda5SiUOvT1K0U=;");
    // See: https://questdb.io/docs/operations/rbac/#authentication

await sender.Table("trades")
    .Symbol("symbol", "ETH-USD")
    .Symbol("side", "sell")
    .Column("price", 2615.54)
    .Column("amount", 0.00044)
    .AtAsync(DateTime.UtcNow);

await sender.Table("trades")
    .Symbol("symbol", "BTC-USD")
    .Symbol("side", "sell")
    .Column("price", 39269.98)
    .Column("amount", 0.001)
    .AtAsync(DateTime.UtcNow);

await sender.SendAsync();

