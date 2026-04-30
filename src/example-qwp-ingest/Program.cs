using System;
using QuestDB;
using QuestDB.Senders;

// Connect via the WebSocket / QWP transport. The wire format is a columnar binary
// protocol — much smaller and faster than the text ILP that http:: and tcp:: use.
//
// Connect-string knobs that matter for ws::
//   addr                 host:port (default port 9000, shared with HTTP)
//   auto_flush_rows      rows before an automatic flush is triggered (default 1000 for ws)
//   auto_flush_interval  milliseconds before an automatic flush (default 100 for ws)
//   in_flight_window     pipelined batches in flight (default 128; set to 1 for sync send-and-wait)
//   close_timeout        ms to wait for in-flight ACKs on Dispose / Ping (default 5000)
//   request_durable_ack  on/off — opt in to per-table durable seqTxn watermarks
//   username/password    Basic auth, or
//   token                Bearer auth
await using var sender = Sender.New("ws::addr=localhost:9000;request_durable_ack=on;");

await sender.Table("trades")
    .Symbol("symbol", "ETH-USD")
    .Symbol("side", "sell")
    .Column("price", 2615.54)
    .Column("amount", 0.00044)
    .AtAsync(DateTime.UtcNow);

await sender.Table("trades")
    .Symbol("symbol", "BTC-USD")
    .Symbol("side", "buy")
    .Column("price", 39269.98)
    .Column("amount", 0.001)
    .AtAsync(DateTime.UtcNow);

await sender.SendAsync();

// When `request_durable_ack=on` is set, the WebSocket sender exposes per-table seqTxn watermarks
// via the IQwpWebSocketSender interface.
if (sender is IQwpWebSocketSender ws)
{
    Console.WriteLine($"trades committed seqTxn: {ws.GetHighestAckedSeqTxn("trades")}");
    Console.WriteLine($"trades durable seqTxn:   {ws.GetHighestDurableSeqTxn("trades")}");
}
