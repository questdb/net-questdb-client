using System;
using QuestDB;
using QuestDB.Senders;

// Authenticated wss:// (WebSocket + TLS) ingest. The wire format is the same QWP columnar
// binary protocol as ws::, with TLS layered on. Authentication: Basic (username + password)
// or Bearer (token). The two are mutually exclusive — set one or the other.
//
// Connect-string knobs that matter for wss:: with auth:
//   addr                 host:port (default 9000 for ws/wss)
//   username/password    Basic auth (mutually exclusive with token)
//   token                Bearer auth
//   tls_verify           on (default) or unsafe_off — disable cert validation only for testing
//   tls_roots            path to a custom .pem CA bundle / client cert
//   tls_roots_password   password / private-key file paired with tls_roots
//   request_durable_ack  on/off — opt in to per-table durable seqTxn watermarks
//
// In production: use tls_verify=on with the system trust store, or tls_roots pointing at a
// pinned CA. Below uses unsafe_off for self-signed dev / test setups; never ship that to prod.
using var sender =
    Sender.New(
        "wss::addr=localhost:9000;username=admin;password=quest;tls_verify=unsafe_off;request_durable_ack=on;");

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

// Per-table durable / committed seqTxn watermarks are exposed via IQwpWebSocketSender. They
// require request_durable_ack=on for the durable watermark; the committed watermark works
// regardless.
if (sender is IQwpWebSocketSender ws)
{
    Console.WriteLine($"trades committed seqTxn: {ws.GetHighestAckedSeqTxn("trades")}");
    Console.WriteLine($"trades durable seqTxn:   {ws.GetHighestDurableSeqTxn("trades")}");
}
