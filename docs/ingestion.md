# Ingestion

The .NET ILP client streams data to QuestDB using the ILP format.

The format is a text protocol with the following form:

`table,symbol=value column1=value1 column2=value2 nano_timestamp`

The client provides a useful API to manage the construction and sending of ILP rows.

## Initialisation

Construction of new [Senders](xref:QuestDB.Senders.ISender) can be performed using the [Sender](xref:QuestDB.Sender)
factory.

### Constructing with a configuration string

It is recommended, where possible, to initialise the sender using
a [configuration string](https://questdb.io/docs/reference/api/ilp/overview/#configuration-strings).

Configuration strings provide a convenient shorthand for defining client properties, and are validated during
construction of the [Sender](xref:QuestDB.Senders.ISender).

```c#
await using var sender = Sender.New("http::addr=localhost:9000;");
```

If you want to initialise some properties programmatically after the initial config string, you can
use [Configure](xref:QuestDB.Sender.Configure(System.String)) and
[Build](xref:QuestDB.Utils.SenderOptions.Build*).