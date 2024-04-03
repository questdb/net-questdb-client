using System;
using System.Threading.Tasks;
using QuestDB.Ingress;

namespace QuestDBDemo;

internal class Program
{
    /// <summary>
    /// Demonstrates TCPS connection against QuestDB Enterprise
    /// </summary>
    /// <param name="args"></param>
    private static async Task Main(string[] args)
    {
        using var sender =
            new Sender(
                "tcps::addr=localhost:9009;tls_verify=unsafe_off;username=admin;token=NgdiOWDoQNUP18WOnb1xkkEG5TzPYMda5SiUOvT1K0U=;");
        // See: https://questdb.io/docs/reference/api/ilp/authenticate
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