/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

using System.Diagnostics;
using System.Globalization;
using System.Text;
using QuestDB.Senders;

namespace QuestDB.StacBench;

/// <summary>
///     STAC benchmark ingestion test client. Port of java-questdb-client's
///     <c>StacBenchmarkClient</c>. Reports sustained ingestion throughput as
///     rows/second and an estimated (pre-compression) MB/s.
///
///     Tests ingestion for a STAC-like quotes table with this schema:
///     <code>
///     CREATE TABLE q (
///         s SYMBOL,     -- 4-letter ticker symbol (8512 unique)
///         x CHAR,       -- exchange code
///         b FLOAT,      -- bid price
///         a FLOAT,      -- ask price
///         v SHORT,      -- bid volume
///         w SHORT,      -- ask volume
///         m BOOLEAN,    -- market flag
///         T TIMESTAMP   -- designated timestamp
///     ) timestamp(T) PARTITION BY DAY WAL;
///     </code>
///
///     Pre-create the table so the server uses the narrow column types
///     (FLOAT, SHORT, CHAR); otherwise QWP/ILP auto-creation widens b/a to
///     DOUBLE and v/w to LONG (x stays CHAR, sent natively over QWP).
/// </summary>
internal static class Program
{
    private const int DefaultBatchSize = 10_000;
    private const int DefaultFlushBytes = 0;
    private const long DefaultFlushIntervalMs = 0;
    private const string DefaultHost = "localhost";
    private const int DefaultReportInterval = 1_000_000;
    private const int DefaultRows = 80_000_000;
    private const string DefaultTable = "q";
    private const int DefaultWarmupRows = 100_000;

    // Estimated row size for the (pre-compression) throughput figure: 1 symbol
    // (~6) + 1 char (2) + 2 floats (8) + 2 shorts (4) + 1 bool (1) + 1 timestamp
    // (8) + overhead (~10) = ~39 bytes. Fixed constant, same as the Java client.
    private const int EstimatedRowSize = 39;

    private const string ProtocolIlpHttp = "ilp-http";
    private const string ProtocolIlpTcp = "ilp-tcp";
    private const string ProtocolQwpWebSocket = "qwp-websocket";

    // 8512 unique 4-letter symbols, as per the STAC NYSE benchmark.
    private const int SymbolCount = 8512;

    // Exchange codes (single characters).
    private static readonly char[] Exchanges = { 'N', 'Q', 'A', 'B', 'C', 'D', 'P', 'Z' };

    private static readonly string[] Symbols = GenerateSymbols();
    private static readonly double[] BasePrices = GenerateBasePrices();

    private static async Task<int> Main(string[] args)
    {
        var protocol = ProtocolQwpWebSocket;
        var host = DefaultHost;
        var port = -1;
        var totalRows = DefaultRows;
        var batchSize = DefaultBatchSize;
        var flushBytes = DefaultFlushBytes;
        var flushIntervalMs = DefaultFlushIntervalMs;
        var warmupRows = DefaultWarmupRows;
        var reportInterval = DefaultReportInterval;
        var table = DefaultTable;

        foreach (var arg in args)
        {
            if (arg is "--help" or "-h")
            {
                PrintUsage();
                return 0;
            }

            if (arg.StartsWith("--protocol=", StringComparison.Ordinal))
                protocol = arg["--protocol=".Length..].ToLowerInvariant();
            else if (arg.StartsWith("--host=", StringComparison.Ordinal))
                host = arg["--host=".Length..];
            else if (arg.StartsWith("--port=", StringComparison.Ordinal))
                port = int.Parse(arg["--port=".Length..], CultureInfo.InvariantCulture);
            else if (arg.StartsWith("--rows=", StringComparison.Ordinal))
                totalRows = int.Parse(arg["--rows=".Length..], CultureInfo.InvariantCulture);
            else if (arg.StartsWith("--batch=", StringComparison.Ordinal))
                batchSize = int.Parse(arg["--batch=".Length..], CultureInfo.InvariantCulture);
            else if (arg.StartsWith("--flush-bytes=", StringComparison.Ordinal))
                flushBytes = int.Parse(arg["--flush-bytes=".Length..], CultureInfo.InvariantCulture);
            else if (arg.StartsWith("--flush-interval-ms=", StringComparison.Ordinal))
                flushIntervalMs = long.Parse(arg["--flush-interval-ms=".Length..], CultureInfo.InvariantCulture);
            else if (arg.StartsWith("--warmup=", StringComparison.Ordinal))
                warmupRows = int.Parse(arg["--warmup=".Length..], CultureInfo.InvariantCulture);
            else if (arg.StartsWith("--report=", StringComparison.Ordinal))
                reportInterval = int.Parse(arg["--report=".Length..], CultureInfo.InvariantCulture);
            else if (arg.StartsWith("--table=", StringComparison.Ordinal))
                table = arg["--table=".Length..];
            else if (arg == "--no-warmup")
                warmupRows = 0;
            else
            {
                Console.Error.WriteLine("Unknown option: " + arg);
                PrintUsage();
                return 1;
            }
        }

        if (port == -1)
            port = GetDefaultPort(protocol);

        Console.WriteLine("STAC Benchmark Ingestion Client");
        Console.WriteLine("================================");
        Console.WriteLine("Protocol: " + protocol);
        Console.WriteLine("Host: " + host);
        Console.WriteLine("Port: " + port);
        Console.WriteLine("Table: " + table);
        Console.WriteLine("Total rows: " + totalRows.ToString("N0", CultureInfo.InvariantCulture));
        Console.WriteLine("Batch size (rows): " + batchSize.ToString("N0", CultureInfo.InvariantCulture) +
                          (batchSize == 0 ? " (default)" : ""));
        Console.WriteLine("Flush bytes: " + (flushBytes == 0
            ? "(default)"
            : flushBytes.ToString("N0", CultureInfo.InvariantCulture)));
        Console.WriteLine("Flush interval: " + (flushIntervalMs == 0 ? "(default)" : flushIntervalMs + " ms"));
        Console.WriteLine("Warmup rows: " + warmupRows.ToString("N0", CultureInfo.InvariantCulture));
        Console.WriteLine("Report interval: " + reportInterval.ToString("N0", CultureInfo.InvariantCulture));
        Console.WriteLine("Symbols: " + SymbolCount.ToString("N0", CultureInfo.InvariantCulture) +
                          " unique 4-letter tickers");
        Console.WriteLine();

        try
        {
            await RunTest(protocol, host, port, table, totalRows, batchSize, flushBytes, flushIntervalMs,
                warmupRows, reportInterval);
            return 0;
        }
        catch (Exception e)
        {
            Console.Error.WriteLine("Error: " + e.Message);
            Console.Error.WriteLine(e);
            return 1;
        }
    }

    private static ISender CreateSender(string protocol, string host, int port,
        int batchSize, int flushBytes, long flushIntervalMs)
    {
        switch (protocol)
        {
            case ProtocolIlpTcp:
                return Sender.New($"tcp::addr={host}:{port};");

            case ProtocolIlpHttp:
            {
                var conf = new StringBuilder($"http::addr={host}:{port};");
                if (batchSize > 0)
                    conf.Append($"auto_flush_rows={batchSize};auto_flush_interval=off;auto_flush_bytes=off;");
                return Sender.New(conf.ToString());
            }

            case ProtocolQwpWebSocket:
            {
                var conf = new StringBuilder($"ws::addr={host}:{port};");
                // Java only sets autoFlushRows; drive flushing purely by the knobs supplied so the
                // batch size is the sole trigger unless --flush-bytes / --flush-interval-ms override.
                if (batchSize > 0)
                    conf.Append($"auto_flush_rows={batchSize};");
                conf.Append(flushBytes > 0 ? $"auto_flush_bytes={flushBytes};" : "auto_flush_bytes=off;");
                conf.Append(flushIntervalMs > 0 ? $"auto_flush_interval={flushIntervalMs};" : "auto_flush_interval=off;");
                // Match Java's StacBenchmarkClient timing: its flush() only publishes rows to the ring
                // (no awaitPendingAcks) and the ACK drain happens untimed in close(). close_flush_timeout_millis=0
                // makes Send()/SendAsync() a publish-to-ring (no ACK wait), so the timed region mirrors Java's;
                // the bench drains explicitly (untimed) after stopping the clock.
                conf.Append("close_flush_timeout_millis=0;");
                return Sender.New(conf.ToString());
            }

            default:
                throw new ArgumentException("Unknown protocol: " + protocol +
                                            ". Use one of: ilp-tcp, ilp-http, qwp-websocket");
        }
    }

    private static double[] GenerateBasePrices()
    {
        var prices = new double[SymbolCount];
        var rng    = new Random(42); // fixed seed for reproducibility
        for (var i = 0; i < SymbolCount; i++)
            prices[i] = 1.0f + (float)rng.NextDouble() * 499.0f;
        return prices;
    }

    private static string[] GenerateSymbols()
    {
        var symbols = new string[SymbolCount];
        var idx = 0;
        // 26^4 = 456,976 possible 4-letter combinations, far more than 8512.
        for (var a = 'A'; a <= 'Z'; a++)
        for (var b = 'A'; b <= 'Z'; b++)
        for (var c = 'A'; c <= 'Z'; c++)
        for (var d = 'A'; d <= 'Z'; d++)
        {
            symbols[idx++] = new string(new[] { a, b, c, d });
            if (idx >= SymbolCount)
                return symbols;
        }

        return symbols;
    }

    private static int GetDefaultPort(string protocol)
    {
        if (protocol is ProtocolIlpHttp or ProtocolQwpWebSocket)
            return 9000;
        return 9009;
    }

    private static void PrintUsage()
    {
        Console.WriteLine("STAC Benchmark Ingestion Client");
        Console.WriteLine();
        Console.WriteLine("Tests ingestion performance for a STAC-like quotes table.");
        Console.WriteLine("The table must be pre-created with the correct schema.");
        Console.WriteLine();
        Console.WriteLine("Usage: net-questdb-client-stac-bench [options]");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  --protocol=PROTOCOL      Protocol to use (default: qwp-websocket)");
        Console.WriteLine("  --host=HOST              Server host (default: localhost)");
        Console.WriteLine("  --port=PORT              Server port (default: 9009 for TCP, 9000 for HTTP/WebSocket)");
        Console.WriteLine("  --table=TABLE            Table name (default: q)");
        Console.WriteLine("  --rows=N                 Total rows to send (default: 80000000)");
        Console.WriteLine("  --batch=N                Auto-flush after N rows (default: 10000)");
        Console.WriteLine("  --flush-bytes=N          Auto-flush after N bytes (default: protocol default)");
        Console.WriteLine("  --flush-interval-ms=N    Auto-flush after N ms (default: protocol default)");
        Console.WriteLine("  --warmup=N               Warmup rows (default: 100000)");
        Console.WriteLine("  --report=N               Report progress every N rows (default: 1000000)");
        Console.WriteLine("  --no-warmup              Skip warmup phase");
        Console.WriteLine("  --help                   Show this help");
        Console.WriteLine();
        Console.WriteLine("Protocols:");
        Console.WriteLine("  ilp-tcp          Old ILP text protocol over TCP (default port: 9009)");
        Console.WriteLine("  ilp-http         Old ILP text protocol over HTTP (default port: 9000)");
        Console.WriteLine("  qwp-websocket    New QWP binary protocol over WebSocket (default port: 9000)");
        Console.WriteLine();
        Console.WriteLine("Table schema (must be pre-created):");
        Console.WriteLine("  CREATE TABLE q (");
        Console.WriteLine("      s SYMBOL, x CHAR, b FLOAT, a FLOAT,");
        Console.WriteLine("      v SHORT, w SHORT, m BOOLEAN, T TIMESTAMP");
        Console.WriteLine("  ) timestamp(T) PARTITION BY DAY WAL;");
    }

    private static async Task RunTest(string protocol, string host, int port, string table,
        int totalRows, int batchSize, int flushBytes, long flushIntervalMs,
        int warmupRows, int reportInterval)
    {
        Console.WriteLine($"Connecting to {host}:{port}...");

        await using var sender = CreateSender(protocol, host, port, batchSize, flushBytes, flushIntervalMs);
        Console.WriteLine("Connected! Protocol: " + protocol);
        Console.WriteLine();

        // Warmup phase.
        if (warmupRows > 0)
        {
            Console.WriteLine("Warming up (" + warmupRows.ToString("N0", CultureInfo.InvariantCulture) + " rows)...");
            var warmupSw = Stopwatch.StartNew();
            for (var i = 0; i < warmupRows; i++)
                SendQuoteRow(sender, table, i);
            await sender.SendAsync();
            // Fully drain the warmup (untimed) so the main test starts with an empty ring rather than
            // inheriting warmup frames still in flight (close_flush_timeout_millis=0 makes Send() no-wait).
            await sender.FlushAsync(TimeSpan.FromMinutes(1));
            warmupSw.Stop();
            var warmupRowsPerSec = warmupRows / warmupSw.Elapsed.TotalSeconds;
            Console.WriteLine($"Warmup complete in {warmupSw.ElapsedMilliseconds} ms " +
                              $"({warmupRowsPerSec.ToString("N0", CultureInfo.InvariantCulture)} rows/sec)");
            Console.WriteLine();

            GC.Collect();
            GC.WaitForPendingFinalizers();
            await Task.Delay(100);
        }

        // Main test phase.
        Console.WriteLine("Starting main test (" + totalRows.ToString("N0", CultureInfo.InvariantCulture) + " rows)...");
        if (reportInterval > 0 && reportInterval <= totalRows)
            Console.WriteLine("Progress will be reported every " +
                              reportInterval.ToString("N0", CultureInfo.InvariantCulture) + " rows");
        Console.WriteLine();

        var sw = Stopwatch.StartNew();
        var lastReportElapsed = TimeSpan.Zero;
        var lastReportRows = 0;

        for (var i = 0; i < totalRows; i++)
        {
            SendQuoteRow(sender, table, i);

            if (reportInterval > 0 && (i + 1) % reportInterval == 0)
            {
                var now = sw.Elapsed;
                var elapsedSinceReport = (now - lastReportElapsed).TotalSeconds;
                var rowsSinceReport = i + 1 - lastReportRows;
                var rowsPerSec = rowsSinceReport / elapsedSinceReport;
                var overallRowsPerSec = (i + 1) / now.TotalSeconds;

                Console.WriteLine(string.Format(CultureInfo.InvariantCulture,
                    "Progress: {0:N0} / {1:N0} rows ({2:F1}%) - {3:N0} rows/sec (interval) - {4:N0} rows/sec (overall)",
                    i + 1, totalRows, (i + 1) * 100.0 / totalRows, rowsPerSec, overallRowsPerSec));

                lastReportElapsed = now;
                lastReportRows = i + 1;
            }
        }

        // Publish-to-ring (no ACK wait) — mirrors Java's flush(). Timer stops here, as Java's does.
        // await sender.SendAsync();
        sw.Stop();

        var totalSeconds = sw.Elapsed.TotalSeconds;
        var rowsPerSecond = totalRows / totalSeconds;
        var mbPerSec = (long)totalRows * EstimatedRowSize / (1024.0 * 1024.0 * totalSeconds);

        // Untimed drain to server ACK — mirrors Java's close(), which delivers the buffered rows after
        // the timer has stopped. Measured separately so the end-to-end cost is still visible.
        var drainSw = Stopwatch.StartNew();
        await sender.FlushAsync(TimeSpan.FromMinutes(5));
        drainSw.Stop();

        var withDrainSeconds = totalSeconds + drainSw.Elapsed.TotalSeconds;

        Console.WriteLine();
        Console.WriteLine("Test Complete!");
        Console.WriteLine("==============");
        Console.WriteLine("Protocol: " + protocol);
        Console.WriteLine("Table: " + table);
        Console.WriteLine("Total rows: " + totalRows.ToString("N0", CultureInfo.InvariantCulture));
        Console.WriteLine("Batch size: " + batchSize.ToString("N0", CultureInfo.InvariantCulture));
        Console.WriteLine("Total time (publish-to-ring, matches Java): " +
                          totalSeconds.ToString("F2", CultureInfo.InvariantCulture) + " seconds");
        Console.WriteLine("Throughput: " + rowsPerSecond.ToString("N0", CultureInfo.InvariantCulture) + " rows/second");
        Console.WriteLine("Data rate (before compression): " +
                          mbPerSec.ToString("F2", CultureInfo.InvariantCulture) + " MB/s (estimated)");
        Console.WriteLine("Drain to server ACK (untimed, like Java close()): " +
                          drainSw.Elapsed.TotalSeconds.ToString("F2", CultureInfo.InvariantCulture) + " seconds");
        Console.WriteLine("Throughput incl. drain: " +
                          ((long)(totalRows / withDrainSeconds)).ToString("N0", CultureInfo.InvariantCulture) +
                          " rows/second");
    }

    /// <summary>
    ///     Sends a single quote row matching the STAC schema
    ///     (s SYMBOL, x CHAR, b FLOAT, a FLOAT, v SHORT, w SHORT, m BOOLEAN, T TIMESTAMP).
    ///     The server downcasts double->FLOAT and long->SHORT when the table is pre-created
    ///     with the narrow schema; x is sent as a native QWP CHAR.
    /// </summary>
    private static void SendQuoteRow(ISender sender, string table, int rowIndex)
    {
        var symbolIdx = rowIndex % SymbolCount;
        var exchangeIdx = rowIndex % Exchanges.Length;

        var basePrice = BasePrices[symbolIdx];
        // rowIndex bits give fast pseudo-random variation without a Random object.
        var variation = ((rowIndex * 7 + symbolIdx * 13) % 200 - 100) * 0.01f;
        double bid = basePrice + variation;
        double ask = bid + 0.01f + rowIndex % 10 * 0.01f; // spread: 1-10 cents

        // Volumes: 100-32000 range fits SHORT.
        long bidVol = (short)(100 + (rowIndex * 3 + symbolIdx) % 31901);
        long askVol = (short)(100 + (rowIndex * 7 + symbolIdx * 5) % 31901);

        // Timestamp: microsecond precision, one day of data spread across the run.
        const long baseTimestamp = 1704067200000000L; // 2024-01-01 00:00:00 UTC in micros
        var timestamp = baseTimestamp + rowIndex * 10L + rowIndex % 7;

        sender.Table(table)
            .Symbol("s", Symbols[symbolIdx])
            .Column("x", Exchanges[exchangeIdx])
            .Column("b", bid)
            .Column("a", ask)
            .Column("v", bidVol)
            .Column("w", askVol)
            .Column("m", (rowIndex & 1) == 0)
            .At(timestamp);
    }
}
