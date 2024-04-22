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

```c#
(Sender.Configure("http::addr=localhost:9000;") with { auto_flush = AutoFlushType.off }).Build()
```

### Constructing from options

The sender API also supports construction from [SenderOptions](xref:QuestDB.Utils.SenderOptions).

```c#
await using var sender = Sender.New(new SenderOptions());
```

You might use this when binding options from configuration:

```json
{
  "QuestDB": {
    "addr": "localhost:9000",
    "tls_verify": "unsafe_off"
  }
}
```

```c#
var options = new ConfigurationBuilder()
    .AddJsonFile("config.json")
    .Build()
    .GetSection("QuestDB")
    .Get<SenderOptions>();
```

## Preparing Data

Senders use an internal buffer to convert input values into an ILP-compatible UTF-8 byte-string.

This buffer can be controlled using the ```init_buf_size``` and ```max_buf_size``` parameters.

### Build a row

#### Specify the table

An ILP row starts with a table name, using [Table](QuestDB.Senders.ISender.Table*).

```c#
sender.Table("table_name");
```

The table name must always be called before other builder functions.

#### Add symbols

A [symbol](https://questdb.io/docs/concept/symbol/) is a dictionary-encoded string, used to efficiently store commonly
repeated data.
This is frequently used for identifiers, and symbol columns can
have [secondary indexes](https://questdb.io/docs/concept/indexes/) defined upon them.

Symbols can be added using calls to [symbol](QuestDB.Senders.ISender.Symbol*), which expects a symbol column name, and
string value.

```c#
sender.Symbol("foo", "bah");
```

All symbol columns must be defined before any other column definition.

#### Add other columns

A number of data types can be submitted to [QuestDB](https://www.questdb.io) via ILP,
including [string](xref:System.String) / [long](xref:System.Int64) / [double](xref:System.Double) / [DateTime](xref:System.DateTime) / [DateTimeOffset](xref:System.DateTimeOffset).

These can be written using the [column](QuestDB.Senders.ISender.Column*) functions.

```c#
sender.Column("baz", 102);
```

#### Finish the row

A row is completed by defining the designated timestamp value.

```c#
sender.At(DateTime.UtcNow);
```

Generation of the timestamp can be offloaded to the server, using [AtNow()](xref:QuestDB.Senders.ISender.AtNow*).

### Auto-flushing

When the [At](xref:QuestDB.Senders.ISender.At*) functions are called, the auto-flushing parameters are checked to see
if it is appropriate
to flush the buffer. If an auto-flush is triggered, data will be sent to QuestDB.

To avoid blocking the calling thread, one can use the Async overloads of the [At](xref:QuestDB.Senders.ISender.At*)
functions
i.e [AtAsync](xref:QuestDB.Senders.ISender.AtAsync*) and [AtNowAsync](xref:QuestDB.Senders.ISender.AtNowAsync*).




