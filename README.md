# net-questdb-client

### Dotnet QuestDB ILP TCP client

- Basic usage

```c#
using var ls = new LineTcpSender(IPAddress.Loopback.ToString(), 9009);
ls.Table("metric_name")
    .Symbol("Symbol", "value")
    .Column("number", 10)
    .Column("double", 12.23)
    .Column("string", "born to shine")
    .At(new DateTime(2021, 11, 25, 0, 46, 26));
ls.Flush();
```

- Multi-line send

```c#
using var ls = new LineTcpSender(IPAddress.Loopback.ToString(), 9009);
for(int i = 0; i < 1E6; i++) 
{
    ls.Table("metric_name")
        .Column("counter", i)
        .AtNow();
}
ls.Flush();
```

- Dynamic add of tables

```c#
using var ls = new LineTcpSender(IPAddress.Loopback.ToString(), 9009);
ls.Table("metric_name", true)
    .Symbol("Symbol", "value")
    .Column("number", 10);
    
ls.Table("metric_name", true)
    .Column("string", "born to shine")
    .At(new DateTime(2021, 11, 25, 0, 46, 26));
ls.Flush();
```