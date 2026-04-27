// QWP transport example. Demonstrates the WebSocket and UDP variants of the
// QuestDB Wire Protocol via the public ISender API. Both protocols use the
// same per-row builder (.Table().Symbol().Column().AtAsync()) — only the
// connection string changes.
//
// Run a local QuestDB instance with QWP enabled, then:
//   dotnet run --project src/example-qwp
//
// For the WebSocket path:
//   ws::addr=localhost:9000;        // plain ws://
//   wss::addr=localhost:9000;       // WSS (TLS)
//
// For the UDP fire-and-forget path:
//   udp::addr=localhost:9009;
//
// WebSocket supports durable acks (request_durable_ack=on), which causes the
// server to ack each batch only after it's fsync'd.

using System;
using System.Threading.Tasks;
using QuestDB;

// ---- WebSocket: durable acks + auto-flush ----
{
    using var sender = Sender.New(
        "ws::addr=localhost:9000;auto_flush=on;auto_flush_rows=10000;");

    for (var i = 0; i < 100; i++)
    {
        await sender.Table("trades")
            .Symbol("symbol", "ETH-USD")
            .Symbol("side", "sell")
            .Column("price", 2615.54 + i * 0.01)
            .Column("amount", 0.00044)
            .AtAsync(DateTime.UtcNow);
    }
    await sender.SendAsync();
    Console.WriteLine("WebSocket: sent 100 rows.");
}

// ---- UDP: fire-and-forget ----
{
    using var sender = Sender.New("udp::addr=localhost:9009;");

    for (var i = 0; i < 100; i++)
    {
        sender.Table("trades")
            .Symbol("symbol", "BTC-USD")
            .Symbol("side", "buy")
            .Column("price", 39269.98 + i * 0.5)
            .Column("amount", 0.001)
            .At(DateTime.UtcNow);
    }
    sender.Send();
    Console.WriteLine("UDP: sent 100 rows.");
}
