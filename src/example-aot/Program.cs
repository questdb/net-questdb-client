using QuestDB;

// AOT-compatibility smoke test. Exercises both an HTTP ILP path and a QWP UDP
// path so `dotnet publish -r <rid> -c Release` validates that neither relies on
// reflection-only code paths the AOT compiler would have to drop.
//
// QWP WebSocket needs ClientWebSocket which uses HTTP/1.1 upgrade machinery
// that isn't fully AOT-friendly on every runtime; UDP is the cleaner AOT
// target for QWP and is shown here.

// ---- HTTP ILP ----
{
    using var sender = Sender.New("http::addr=localhost:9000;");
    await sender.Table("trades")
        .Symbol("symbol", "ETH-USD")
        .Symbol("side", "sell")
        .Column("price", 2615.54)
        .Column("amount", 0.00044)
        .AtAsync(DateTime.UtcNow);
    await sender.SendAsync();
}

// ---- QWP UDP ----
{
    using var sender = Sender.New("udp::addr=localhost:9009;");
    sender.Table("trades")
        .Symbol("symbol", "BTC-USD")
        .Symbol("side", "buy")
        .Column("price", 39269.98)
        .Column("amount", 0.001)
        .At(DateTime.UtcNow);
    sender.Send();
}

// Test with:
// dotnet publish -r linux-x64 -c Release src/example-aot/example-aot.csproj
