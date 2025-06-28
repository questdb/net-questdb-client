using QuestDB;

using var sender = Sender.New("http::addr=localhost:9000;");
var       x      = sender.Options.ToString();
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