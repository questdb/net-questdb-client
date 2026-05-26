using System.Globalization;
using QuestDB;
using QuestDB.Senders;

// Line-protocol sidecar for the Enterprise e2e pytest harness.
// Mirrors the Java QwpSidecarMain: reads commands from stdin,
// writes "OK ..." or "ERR ..." replies to stdout.

Console.OutputEncoding = System.Text.Encoding.UTF8;
Console.InputEncoding = System.Text.Encoding.UTF8;

ISender? sender = null;

Console.Out.WriteLine("READY");
Console.Out.Flush();

string? line;
while ((line = Console.In.ReadLine()) != null)
{
    line = line.Trim();
    if (line.Length == 0) continue;

    var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
    var verb = parts[0].ToUpperInvariant();

    try
    {
        switch (verb)
        {
            case "CONNECT":
            {
                var connectString = line[parts[0].Length..].Trim();
                sender?.Dispose();
                sender = Sender.New(connectString);
                Reply("OK");
                break;
            }

            case "SEND":
            {
                EnsureSender(sender);
                var table = parts[1];
                var count = int.Parse(parts[2], CultureInfo.InvariantCulture);
                var startIndex = parts.Length > 3
                    ? int.Parse(parts[3], CultureInfo.InvariantCulture)
                    : 0;

                for (var i = 0; i < count; i++)
                {
                    var idx = startIndex + i;
                    sender!.Table(table)
                        .Symbol("tag", $"test_{idx}")
                        .Column("value", (long)idx)
                        .At(DateTime.UtcNow);
                }

                Reply("OK");
                break;
            }

            case "FLUSH":
            {
                EnsureSender(sender);
                if (sender is IQwpWebSocketSender wsSender)
                {
                    var fsn = await wsSender.FlushAndGetSequenceAsync();
                    Reply($"OK {fsn}");
                }
                else
                {
                    await sender!.SendAsync();
                    Reply("OK -1");
                }

                break;
            }

            case "AWAIT_ACKED":
            {
                EnsureSender(sender);
                var fsn = long.Parse(parts[1], CultureInfo.InvariantCulture);
                var timeoutMs = int.Parse(parts[2], CultureInfo.InvariantCulture);
                if (sender is IQwpWebSocketSender wsSender)
                {
                    var reached = await wsSender.AwaitAckedFsnAsync(
                        fsn, TimeSpan.FromMilliseconds(timeoutMs));
                    Reply($"OK {(reached ? "true" : "false")}");
                }
                else
                {
                    Reply("OK true");
                }

                break;
            }

            case "STATS":
            {
                EnsureSender(sender);
                if (sender is IQwpWebSocketSender wsSender)
                {
                    Reply(
                        $"OK acked={wsSender.AckedFsn} " +
                        $"sent={wsSender.TotalFramesSent} " +
                        $"acks={wsSender.TotalAcks} " +
                        $"reconnAttempts={wsSender.TotalReconnectAttempts} " +
                        $"reconnSucc={wsSender.TotalReconnectsSucceeded} " +
                        $"serverErrors={wsSender.TotalServerErrors}");
                }
                else
                {
                    Reply("OK acked=-1 sent=0 acks=0 reconnAttempts=0 reconnSucc=0 serverErrors=0");
                }

                break;
            }

            case "CLOSE":
            {
                sender?.Dispose();
                sender = null;
                Reply("OK");
                break;
            }

            case "EXIT":
            {
                sender?.Dispose();
                sender = null;
                Reply("OK");
                return;
            }

            default:
                Reply($"ERR unknown verb: {verb}");
                break;
        }
    }
    catch (Exception ex)
    {
        Reply($"ERR {ex.GetType().Name}: {ex.Message}");
    }
}

sender?.Dispose();
return;

static void Reply(string msg)
{
    Console.Out.WriteLine(msg);
    Console.Out.Flush();
}

static void EnsureSender(ISender? s)
{
    if (s == null) throw new InvalidOperationException("no active sender; call CONNECT first");
}