using System.Diagnostics;
using QuestDB.Ingress;

var rowsToSend = 1e6;

await using var sender = Sender.New("http::addr=localhost:9000;auto_flush=on;auto_flush_rows=10;auto_flush_interval=off;");

var timer = new Stopwatch();
timer.Start();

for (var i = 0; i < rowsToSend; i++)
{
    await sender.Table("trades")
        .Symbol("pair", "USDGBP")
        .Symbol("type", "buy")
        .Column("traded_price", 0.83)
        .Column("limit_price", 0.84)
        .Column("qty", 100)
        .Column("traded_ts", new DateTime(
            2022, 8, 6, 7, 35, 23, 189, DateTimeKind.Utc))
        .At(DateTime.UtcNow);
}

// Ensure no pending rows.
await sender.SendAsync();


timer.Stop();

Console.WriteLine(
    $"Wrote {rowsToSend} rows in {timer.Elapsed.TotalSeconds} seconds at a rate of {rowsToSend / timer.Elapsed.TotalSeconds} rows/second.");