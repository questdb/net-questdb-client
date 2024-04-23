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
using var sender = Sender.New("http::addr=localhost:9000;");
```

If you want to initialise some properties programmatically after the initial config string, you can
use [Configure](xref:QuestDB.Sender.Configure(System.String)) and
[Build](xref:QuestDB.Utils.SenderOptions.Build*).

```c#
(Sender.Configure("http::addr=localhost:9000;") with { auto_flush = AutoFlushType.off }).Build()
```

### Constructing from options

The sender API also supports construction from [SenderOptions](xref:QuestDB.Utils.SenderOptions).

```csharp
await using var sender = Sender.New(new SenderOptions());
```

You might use this when binding options from configuration:

```json
{
  "QuestDB": {
    "addr": "localhost:9000",
    "tls_verify": "unsafe_off;"
  }
}
```

```csharp
var options = new ConfigurationBuilder()
    .AddJsonFile("config.json")
    .Build()
    .GetSection("QuestDB")
    .Get<SenderOptions>();
```

## Preparing Data

Senders use an internal buffer to convert input values into an ILP-compatible UTF-8 byte-string.

This buffer can be controlled using the ```init_buf_size``` and ```max_buf_size``` parameters.

Here is how to build a buffer of rows ready to be sent to QuestDB.

### Specify the table

An ILP row starts with a table name, using [Table](QuestDB.Senders.ISender.Table*).

```csharp
sender.Table("table_name");
```

The table name must always be called before other builder functions.

### Add symbols

A [symbol](https://questdb.io/docs/concept/symbol/) is a dictionary-encoded string, used to efficiently store commonly
repeated data.
This is frequently used for identifiers, and symbol columns can
have [secondary indexes](https://questdb.io/docs/concept/indexes/) defined upon them.

Symbols can be added using calls to [symbol](QuestDB.Senders.ISender.Symbol*), which expects a symbol column name, and
string value.

```csharp
sender.Symbol("foo", "bah");
```

All symbol columns must be defined before any other column definition.

### Add other columns

A number of data types can be submitted to [QuestDB](https://www.questdb.io) via ILP,
including [string](xref:System.String) / [long](xref:System.Int64) / [double](xref:System.Double) / [DateTime](xref:System.DateTime) / [DateTimeOffset](xref:System.DateTimeOffset).

These can be written using the [column](QuestDB.Senders.ISender.Column*) functions.

```csharp
sender.Column("baz", 102);
```

### Finish the row

A row is completed by defining the designated timestamp value.

```csharp
sender.At(DateTime.UtcNow);
```

Generation of the timestamp can be offloaded to the server, using [AtNow()](xref:QuestDB.Senders.ISender.AtNow*).


## Flushing
Once the buffer is filled with data ready to be sent, it can be flushed to the database automatically, or manually.


### Auto-flushing

When the [At](xref:QuestDB.Senders.ISender.At*) functions are called, the auto-flushing parameters are checked to see
if it is appropriate
to flush the buffer. If an auto-flush is triggered, data will be sent to QuestDB.

```csharp
sender.At(new DateTime(0,0,1));
```

To avoid blocking the calling thread, one can use the Async overloads of the [At](xref:QuestDB.Senders.ISender.At*)
functions
i.e [AtAsync](xref:QuestDB.Senders.ISender.AtAsync*) and [AtNowAsync](xref:QuestDB.Senders.ISender.AtNowAsync*).

```csharp
await sender.AtNowAsync();
```

> [!CAUTION]
> Using a server generated timestamp via AtNow/AtNowAsync is not compatible with QuestDB's deduplication feature, and
> should be avoided where possible.

Auto-flushing can be enabled or disabled:

```csharp
using var sender = Sender.New("http::localhost:9000;auto_flush=off"); // or `on`, defaults to `on`
```

#### Flush by rows

Users can specify a threshold of rows to flush. This is effectively a submission batch size by number of rows.

```csharp
using var sender = Sender.New("http::localhost:9000;auto_flush=on;auto_flush_rows=5000;"); 
```

By default, HTTP senders will send after `75,000` rows, and TCP after `600` rows.

> [!NOTE]
> `auto_flush_rows` and `auto_flush_interval` are both enabled by default. If you wish to only auto-flush based on
> one of these properties, you can disable the other using `off` or `-1`.

#### Flush by interval

Users can specify a time interval between batches. This is the elapsed time from the last flush, and is checked
when the `At` functions are called.

```csharp
using var sender = Sender.New("http::localhost:9000;auto_flush=on;auto_flush_interval=5000;"); 
```

By default, `auto_flush_interval` is set to `1000` ms.

#### Flush by bytes

Users can specify a buffer length after which to flush, effectively a batch size in UTF-8 bytes. This should be set
according to `init_buf_size` < `auto_flush_bytes` <= `max_buf_size`.

This can be useful if a user has variety in their row sizes and wants to limit the request sizes.

```csharp
using var sender = Sender.New("http::localhost:9000;auto_flush=on;auto_flush_bytes=65536;"); 
```

By default, this is disabled, but set to `100 KiB`.

### Explicit flushing

Users can manually flush the buffer using [Send](xref:QuestDB.Senders.ISender.Send*)
and [SendAsync](xref:QuestDB.Senders.ISender.SendAsync*). This will send any outstanding data to the QuestDB server.

```csharp
using var sender = Sender.New("http::localhost:9000;auto_flush=off;");
sender.Table("foo").Symbol("bah", "baz").Column("num", 123).At(DateTime.UtcNow);
await sender.SendAsync(); // send non-blocking
// OR
sender.Send(); // send synchronously
```

> [!TIP]
> It is recommended to always end your submission code with a manual flush. This will ensure that all data has been sent
> before disposing of the Sender.



## Transactions

The HTTP transport provides transactionality for requests. Each request in a flush sends a batch of rows, which will be
committed
at once, or not at all.

Server-side transactions are only for a single table. Therefore, a request containing multiple tables will be split into
a single transactions per table.

For true transactionality, one can use the transaction feature to enforce a batch only for a single table.

> [!TIP]
> It is still recommended to enable deduplication keys on your tables. This is because an early request timeout,
> or failure to read the response stream, could cause an error in the client, even though the server was returning
> a success response. Therefore, making the table idempotent is best to allow for safe retries.
> With TCP, this is a much greater risk.

### Opening a transaction

A transaction is started by calling [Transaction](xref:QuestDB.Senders.ISender.Transaction*) and passing the name of the
table.

```csharp
sender.Transaction("foo");
```

The sender will return errors if you try to specify an alternate table whilst a transaction is open.

### Adding data

Data can be added to a transaction in the same way a usual, without the need to
call [Table](xref:QuestDB.Senders.ISender.Table*) between rows.

```csharp
sender.Symbol("bah", "baz").Column("num", 123).AtNow(); // adds a symbol, integer column, and ends with current timestamp
```

### Closing a transaction

Transactions can be committed and flushed using [Commit](xref:QuestDB.Senders.ISender.Commit*)
or [CommitAsync](xref:QuestDB.Senders.ISender.CommitAsync*). This will flush data
to the database, and remove the transactional state.

```csharp
await sender.CommitAsync();
```

Alternatively, if you wish to discard the transaction, you can use [Rollback](xref:QuestDB.Senders.ISender.Rollback*).
This will clear the buffer and transactional state, without sending data to the server.

```csharp
sender.Rollback();
```

## Other utilities

There are a few other functionalities, and more may be added in the future.

### Cancelling rows

Users can cancel the current line using [CancelRow](xref:QuestDB.Senders.ISender.CancelRow*). This must be called before
the row is complete, as otherwise it may have been sent already.

```csharp
sender.Table("foo").Symbol("bah", "baz").CancelRow(); // cancels the current row
sender.Table("foo").Symbol("bah", "baz").AtNow(); // invalid - no row to cancel
```

This can be useful if a row is being built step-by-step, and an error is thrown. The user can cancel the row
and preserve the rest of the buffer that was built correctly.

### Trimming the buffer

Users can control the buffer size by setting the relevant properties in the configuration string.

However, it may be that the user needs the buffer to grow for earlier use, and then does not require such a large
buffer later on.

In this scenario, the user can call [Truncate](xref:QuestDB.Senders.ISender.Truncate*). This will trim the internal
buffer,
removing extra pages (each of which is the size of `init_buf_size`), reducing overall memory consumption.

```csharp
using var sender = Sender.New("http::addr=localhost:9000;init_buf_size=1024;");
for (int i = 0; i < 100_000; i++) {
    sender.Table("foo").Column("num", i).AtNow();    
}
await sender.SendAsync(); // buffer is now flushed and empty
sender.Truncate(); // buffer is trimmed back to `init_buf_size`
```

### Clearing the buffer

Users might wish to keep the sender, but clear the internal buffer.

This can be performed using [Clear](xref:QuestDB:Senders:ISender.Clear*).

```csharp
sender.Clear(); // empties the internal buffer
```