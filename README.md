# net-questdb-client

### Dotnet QuestDB ILP TCP client

- Basic usage

```c#
using var ls = new LineTcpSender(IPAddress.Loopback.ToString(), 9009);
ls.Table("metric_name")
    .Symbol("Symbol", "value")
    .Colum("number", 10)
    .Colum("double", 12.23)
    .Colum("string", "born to shine")
    .At(new DateTime(2021, 11, 25, 0, 46, 26));
ls.Flush();
```

- Muliline send

```c#
using var ls = new LineTcpSender(IPAddress.Loopback.ToString(), 9009);
for(int i = 0; i < 1E6; i++) 
{
    ls.Metric("metric_name")
        .Colum("counter", i)
        .AtNow();
}
ls.Flush();
```