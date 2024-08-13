using System.Diagnostics;
using QuestDB;

var rowsToSend = 1e6;

using var sender = Sender.New("http::addr=localhost:9000;auto_flush=on;auto_flush_rows=75000;auto_flush_interval=off;");

var timer = new Stopwatch();
timer.Start();

for (var i = 0; i < rowsToSend; i++)
{
    await sender.Table("trades")
        .Symbol("symbol", "ETH-USD")
        .Symbol("side", "sell")
        .Column("price", 2615.54)
        .Column("amount", 0.00044)
        .AtAsync(DateTime.UtcNow);
}

// Ensure no pending rows.
await sender.SendAsync();


timer.Stop();

Console.WriteLine(
    $"Wrote {rowsToSend} rows in {timer.Elapsed.TotalSeconds} seconds at a rate of {rowsToSend / timer.Elapsed.TotalSeconds} rows/second.");
