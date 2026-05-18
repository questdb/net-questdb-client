using System.Diagnostics;
using System.Net.Sockets;
using System.Text;

namespace net_questdb_client_tests;

/// <summary>
///     Manages a QuestDB server for integration tests by launching it as a local process,
///     the same way the Rust client's <c>system_test/fixture.py</c> does — no Docker.
/// </summary>
/// <remarks>
///     The QuestDB build is located via environment variables (checked in order):
///     <list type="bullet">
///         <item><c>QDB_LIVE_HTTP</c> (+ optional <c>QDB_LIVE_ILP</c>) — skip launching and
///         point the tests at an already-running instance.</item>
///         <item><c>QUESTDB_JAR</c> — absolute path to a built <c>questdb.jar</c>.</item>
///         <item><c>QUESTDB_REPO</c> — path to a built QuestDB repo; the fixture finds
///         <c>core/target/**/questdb*-SNAPSHOT.jar</c>.</item>
///     </list>
///     <c>java</c> is resolved from <c>JAVA_HOME</c> or <c>PATH</c>. The WS (<c>/write/v4</c>)
///     and egress (<c>/read/v1</c>) endpoints only exist on QuestDB master, so the jar must be
///     built from master (CI clones + builds it; see <c>ci/azure-pipelines.yml</c>).
/// </remarks>
public class QuestDbManager : IAsyncDisposable
{
    private readonly int _ilpPort;
    private readonly int _httpPort;
    private readonly int _pgPort;
    private readonly string? _liveHttp;
    private readonly string? _liveIlp;
    private readonly string _dataDir;
    private readonly string[] _extraConf;
    private readonly HttpClient _httpClient;
    private readonly StringBuilder _serverLog = new();
    private readonly object _logLock = new();

    private Process? _process;

    /// <summary>Initializes a new instance of the QuestDbManager.</summary>
    /// <param name="port">ILP port (default: 9009).</param>
    /// <param name="httpPort">HTTP port (default: 9000); also the WebSocket/QWP port.</param>
    /// <param name="extraConf">
    ///     Extra <c>server.conf</c> lines (e.g. debug fragmentation knobs). Appended verbatim.
    /// </param>
    public QuestDbManager(int port = 9009, int httpPort = 9000, IEnumerable<string>? extraConf = null)
    {
        _ilpPort = port;
        _httpPort = httpPort;
        _extraConf = extraConf?.ToArray() ?? Array.Empty<string>();
        _pgPort = FindFreeTcpPort();
        _liveHttp = NormalizeEndpoint(Environment.GetEnvironmentVariable("QDB_LIVE_HTTP"));
        _liveIlp = NormalizeEndpoint(Environment.GetEnvironmentVariable("QDB_LIVE_ILP"));
        _dataDir = Path.Combine(
            Path.GetTempPath(),
            $"qdb-fixture-{httpPort}-{port}-{Guid.NewGuid().ToString("N").Substring(0, 8)}");
        _httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
    }

    public bool UseLiveServer => !string.IsNullOrEmpty(_liveHttp);

    public bool IsRunning { get; private set; }

    /// <summary>
    ///     No-op retained for source compatibility. With the process-launch fixture the
    ///     per-instance data directory persists across <see cref="StopAsync" /> /
    ///     <see cref="StartAsync" />, so data survives a restart without a Docker volume.
    /// </summary>
    public void SetVolume(string volumeName)
    {
    }

    /// <summary>Starts the QuestDB server (or connects to the live instance).</summary>
    public async Task StartAsync()
    {
        if (IsRunning)
        {
            return;
        }

        if (UseLiveServer)
        {
            Console.WriteLine($"Using live QuestDB at {_liveHttp}");
            await WaitForQuestDbAsync().ConfigureAwait(false);
            IsRunning = true;
            return;
        }

        var java = ResolveJava();
        var jar = ResolveQuestDbJar();
        EnsureDataDir();

        var logDir = Path.Combine(_dataDir, "log");
        Directory.CreateDirectory(logDir);

        var startInfo = new ProcessStartInfo
        {
            FileName = java,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
            WorkingDirectory = _dataDir,
        };
        // Module-path launch, mirroring the Rust fixture's `java -p questdb.jar -m
        // io.questdb/io.questdb.ServerMain -d <dataDir>`.
        startInfo.ArgumentList.Add("-DQuestDB-Runtime-0");
        startInfo.ArgumentList.Add("-Dnoebug");
        startInfo.ArgumentList.Add("-XX:+UnlockExperimentalVMOptions");
        startInfo.ArgumentList.Add("-XX:+AlwaysPreTouch");
        startInfo.ArgumentList.Add("-p");
        startInfo.ArgumentList.Add(jar);
        startInfo.ArgumentList.Add("-m");
        startInfo.ArgumentList.Add("io.questdb/io.questdb.ServerMain");
        startInfo.ArgumentList.Add("-d");
        startInfo.ArgumentList.Add(_dataDir);

        Console.WriteLine($"Starting QuestDB: {java} (http={_httpPort}, ilp={_ilpPort}, data={_dataDir})");

        var process = new Process { StartInfo = startInfo, EnableRaisingEvents = true };
        process.OutputDataReceived += (_, e) => AppendLog(e.Data);
        process.ErrorDataReceived += (_, e) => AppendLog(e.Data);

        if (!process.Start())
        {
            throw new InvalidOperationException("Failed to start the QuestDB java process");
        }

        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        _process = process;

        try
        {
            await WaitForQuestDbAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await StopAsync().ConfigureAwait(false);
            throw new InvalidOperationException(
                $"QuestDB failed to start. Server log tail:\n{LogTail()}", ex);
        }

        IsRunning = true;
        Console.WriteLine("QuestDB is ready");
    }

    /// <summary>Stops the QuestDB server. The data directory is left intact for a restart.</summary>
    public async Task StopAsync()
    {
        if (UseLiveServer)
        {
            IsRunning = false;
            return;
        }

        var process = _process;
        if (process is null)
        {
            IsRunning = false;
            return;
        }

        Console.WriteLine("Stopping QuestDB");
        try
        {
            if (!process.HasExited)
            {
                // QuestDB is crash-safe (WAL); a hard kill recovers on next start.
                process.Kill(entireProcessTree: true);
            }

            await process.WaitForExitAsync(new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Warning: failed to stop QuestDB cleanly: {ex.Message}");
        }
        finally
        {
            process.Dispose();
            _process = null;
            IsRunning = false;
        }
    }

    [System.Runtime.InteropServices.DllImport("libc", SetLastError = true)]
    private static extern int kill(int pid, int sig);

    /// <summary>
    ///     Gracefully stops QuestDB: SIGTERM so the JVM runs shutdown hooks and closes the WAL
    ///     cleanly (no torn writes, no mid-ack death). Falls back to a hard kill after 30s.
    ///     Use this for restart scenarios; <see cref="StopAsync" /> hard-kills (crash simulation).
    /// </summary>
    public async Task StopGracefulAsync()
    {
        if (UseLiveServer)
        {
            IsRunning = false;
            return;
        }

        var process = _process;
        if (process is null)
        {
            IsRunning = false;
            return;
        }

        Console.WriteLine("Stopping QuestDB (graceful)");
        try
        {
            if (!process.HasExited)
            {
                if (OperatingSystem.IsWindows())
                {
                    process.Kill(entireProcessTree: true);
                }
                else
                {
                    const int sigterm = 15;
                    kill(process.Id, sigterm);
                }
            }

            try
            {
                await process.WaitForExitAsync(new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                if (!process.HasExited)
                {
                    process.Kill(entireProcessTree: true);
                    await process.WaitForExitAsync().ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Warning: failed to stop QuestDB gracefully: {ex.Message}");
        }
        finally
        {
            process.Dispose();
            _process = null;
            IsRunning = false;
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _httpClient.Dispose();

        try
        {
            if (Directory.Exists(_dataDir))
            {
                Directory.Delete(_dataDir, recursive: true);
            }
        }
        catch
        {
            // best-effort cleanup
        }
    }

    /// <summary>Gets the HTTP endpoint (<c>host:port</c>) for QuestDB.</summary>
    public string GetHttpEndpoint() => _liveHttp ?? $"localhost:{_httpPort}";

    /// <summary>Gets the ILP (TCP) endpoint for QuestDB.</summary>
    public string GetIlpEndpoint() => _liveIlp ?? $"localhost:{_ilpPort}";

    /// <summary>Gets the WebSocket (QWP) endpoint for QuestDB. Shares the HTTP port.</summary>
    public string GetWebSocketEndpoint() => _liveHttp ?? $"localhost:{_httpPort}";

    private void EnsureDataDir()
    {
        var confDir = Path.Combine(_dataDir, "conf");
        Directory.CreateDirectory(confDir);

        var confPath = Path.Combine(confDir, "server.conf");
        if (File.Exists(confPath))
        {
            return;
        }

        var lines = new List<string>
        {
            $"http.bind.to=0.0.0.0:{_httpPort}",
            $"line.tcp.net.bind.to=0.0.0.0:{_ilpPort}",
            $"pg.net.bind.to=0.0.0.0:{_pgPort}",
            "http.min.enabled=false",
            "line.udp.enabled=false",
            "telemetry.enabled=false",
            "cairo.commit.lag=100",
            "http.request.header.buffer.size=4194304",
        };
        lines.AddRange(_extraConf);
        lines.Add(string.Empty);
        File.WriteAllText(confPath, string.Join('\n', lines));
    }

    private async Task WaitForQuestDbAsync()
    {
        const int maxAttempts = 120;
        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            if (!UseLiveServer && _process is { HasExited: true })
            {
                throw new InvalidOperationException("QuestDB process exited during startup");
            }

            try
            {
                var response = await _httpClient.GetAsync($"http://{GetHttpEndpoint()}/settings")
                    .ConfigureAwait(false);
                if (response.IsSuccessStatusCode)
                {
                    return;
                }
            }
            catch
            {
                // not up yet — retry
            }

            await Task.Delay(1000).ConfigureAwait(false);
        }

        throw new TimeoutException("QuestDB failed to start within 120 seconds");
    }

    private static string ResolveJava()
    {
        var javaHome = Environment.GetEnvironmentVariable("JAVA_HOME");
        if (!string.IsNullOrWhiteSpace(javaHome))
        {
            var exe = OperatingSystem.IsWindows() ? "java.exe" : "java";
            var candidate = Path.Combine(javaHome.Trim(), "bin", exe);
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        // Fall back to PATH; the process launch surfaces a clear error if java is absent.
        return OperatingSystem.IsWindows() ? "java.exe" : "java";
    }

    private static string ResolveQuestDbJar()
    {
        var jar = Environment.GetEnvironmentVariable("QUESTDB_JAR");
        if (!string.IsNullOrWhiteSpace(jar))
        {
            jar = jar.Trim();
            if (!File.Exists(jar))
            {
                throw new FileNotFoundException($"QUESTDB_JAR points at a missing file: {jar}");
            }
            return jar;
        }

        var repo = Environment.GetEnvironmentVariable("QUESTDB_REPO");
        if (!string.IsNullOrWhiteSpace(repo))
        {
            var targetDir = Path.Combine(repo.Trim(), "core", "target");
            if (Directory.Exists(targetDir))
            {
                var match = Directory.GetFiles(targetDir, "questdb*.jar", SearchOption.AllDirectories)
                    .Where(p =>
                    {
                        var n = Path.GetFileName(p);
                        return n.Contains("SNAPSHOT", StringComparison.Ordinal)
                               && !n.Contains("-sources", StringComparison.Ordinal)
                               && !n.Contains("-javadoc", StringComparison.Ordinal)
                               && !n.Contains("-tests", StringComparison.Ordinal);
                    })
                    .OrderBy(p => p)
                    .FirstOrDefault();
                if (match is not null)
                {
                    return match;
                }
            }
            throw new FileNotFoundException(
                $"Could not find questdb*-SNAPSHOT.jar under {targetDir}. Build QuestDB first " +
                "(mvn -pl core -am -DskipTests package).");
        }

        throw new InvalidOperationException(
            "No QuestDB build configured. Set QUESTDB_JAR (a built questdb.jar), QUESTDB_REPO " +
            "(a built QuestDB master repo), or QDB_LIVE_HTTP (an already-running instance).");
    }

    private static int FindFreeTcpPort()
    {
        // TcpListener only implements IDisposable on net8.0+, so release it explicitly.
        var listener = new TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static string? NormalizeEndpoint(string? raw)
        => string.IsNullOrWhiteSpace(raw) ? null : raw.Trim();

    private void AppendLog(string? line)
    {
        if (line is null)
        {
            return;
        }

        lock (_logLock)
        {
            _serverLog.AppendLine(line);
            // Cap the buffer so a long-running fixture doesn't grow unbounded.
            if (_serverLog.Length > 256 * 1024)
            {
                _serverLog.Remove(0, _serverLog.Length - 128 * 1024);
            }
        }
    }

    private string LogTail()
    {
        lock (_logLock)
        {
            var text = _serverLog.ToString();
            return text.Length <= 8192 ? text : text.Substring(text.Length - 8192);
        }
    }
}
