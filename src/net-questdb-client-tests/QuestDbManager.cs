using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace QuestDB.Client.Tests;

/// <summary>
/// Manages QuestDB server lifecycle for integration tests.
/// Handles downloading, starting, and stopping QuestDB instances.
/// </summary>
public class QuestDbManager : IAsyncDisposable
{
    private readonly string _projectRoot;
    private readonly string _questdbDir;
    private readonly int _port;
    private readonly int _httpPort;
    private Process? _process;
    private readonly HttpClient _httpClient;

    public string QuestDbPath { get; private set; } = string.Empty;
    public bool IsRunning { get; private set; }

    /// <summary>
    /// Initializes a new instance of the QuestDbManager.
    /// </summary>
    /// <param name="port">ILP port (default: 9009)</param>
    /// <param name="httpPort">HTTP port (default: 9000)</param>
    public QuestDbManager(int port = 9009, int httpPort = 9000)
    {
        _port = port;
        _httpPort = httpPort;
        _projectRoot = FindProjectRoot();
        _questdbDir = Path.Combine(_projectRoot, ".questdb");
        _httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
    }

    /// <summary>
    /// Downloads QuestDB binary if not already present.
    /// </summary>
    public async Task EnsureDownloadedAsync()
    {
        if (IsQuestDbDownloaded())
        {
            QuestDbPath = GetLatestQuestDbPath();
            return;
        }

        var platform = DetectPlatform();
        var version = await GetLatestVersionAsync();

        Console.WriteLine($"Platform: {platform}");
        Console.WriteLine($"Latest version: {version}");
        Console.WriteLine("Downloading QuestDB...");

        await DownloadAndExtractAsync(platform, version);

        QuestDbPath = GetLatestQuestDbPath();
        if (!Directory.Exists(QuestDbPath))
        {
            throw new InvalidOperationException($"QuestDB path does not exist: {QuestDbPath}");
        }

        Console.WriteLine($"QuestDB extracted to: {QuestDbPath}");
    }

    /// <summary>
    /// Starts the QuestDB server.
    /// </summary>
    public async Task StartAsync()
    {
        if (IsRunning)
        {
            Console.WriteLine("QuestDB is already running");
            return;
        }

        await EnsureDownloadedAsync();

        Console.WriteLine($"Starting QuestDB from {QuestDbPath}");

        var questdbExe = Path.Combine(QuestDbPath, "bin", GetExecutableName("questdb"));
        if (!File.Exists(questdbExe))
        {
            throw new FileNotFoundException($"QuestDB executable not found at {questdbExe}");
        }

        var dataDir = Path.Combine(_questdbDir, "data");
        Directory.CreateDirectory(dataDir);

        var startInfo = new ProcessStartInfo
        {
            FileName = questdbExe,
            Arguments = $"-d \"{dataDir}\"",
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            EnvironmentVariables =
            {
                { "QDB_ROOT", dataDir }
            }
        };

        _process = Process.Start(startInfo);
        if (_process == null)
        {
            throw new InvalidOperationException("Failed to start QuestDB process");
        }

        Console.WriteLine($"QuestDB started with PID {_process.Id}");
        IsRunning = true;

        // Wait for QuestDB to be ready
        await WaitForQuestDbAsync();
    }

    /// <summary>
    /// Stops the QuestDB server.
    /// </summary>
    public async Task StopAsync()
    {
        if (!IsRunning || _process == null)
        {
            return;
        }

        Console.WriteLine($"Stopping QuestDB (PID: {_process.Id})");

        try
        {
            _process.Kill();
            await _process.WaitForExitAsync().ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            // Process already exited
        }

        _process?.Dispose();
        _process = null;
        IsRunning = false;
        Console.WriteLine("QuestDB stopped");
    }

    /// <summary>
    /// Gets the HTTP endpoint for QuestDB.
    /// </summary>
    public string GetHttpEndpoint() => $"http://localhost:{_httpPort}";

    /// <summary>
    /// Gets the ILP endpoint for QuestDB.
    /// </summary>
    public string GetIlpEndpoint() => $"localhost:{_port}";

    /// <summary>
    /// Waits for QuestDB to be ready.
    /// </summary>
    private async Task WaitForQuestDbAsync()
    {
        const int maxAttempts = 30;
        var attempts = 0;

        while (attempts < maxAttempts)
        {
            try
            {
                var response = await _httpClient.GetAsync($"{GetHttpEndpoint()}/api/v1/health");
                if (response.IsSuccessStatusCode)
                {
                    Console.WriteLine("QuestDB is ready");
                    return;
                }
            }
            catch
            {
                // Ignore and retry
            }

            await Task.Delay(1000);
            attempts++;
        }

        throw new TimeoutException($"QuestDB failed to start within {maxAttempts} seconds");
    }

    /// <summary>
    /// Cleanup resources.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _httpClient?.Dispose();
        _process?.Dispose();
    }

    private bool IsQuestDbDownloaded()
    {
        if (!Directory.Exists(_questdbDir))
        {
            return false;
        }

        var questdbDirs = Directory.GetDirectories(_questdbDir, "questdb-*");
        return questdbDirs.Length > 0;
    }

    private string GetLatestQuestDbPath()
    {
        var questdbDirs = Directory.GetDirectories(_questdbDir, "questdb-*");
        if (questdbDirs.Length == 0)
        {
            throw new InvalidOperationException("No QuestDB installation found");
        }

        // Return the most recently modified directory
        var latest = questdbDirs[0];
        var latestTime = Directory.GetCreationTime(latest);

        foreach (var dir in questdbDirs)
        {
            var time = Directory.GetCreationTime(dir);
            if (time > latestTime)
            {
                latest = dir;
                latestTime = time;
            }
        }

        return latest;
    }

    private string FindProjectRoot()
    {
        var current = Directory.GetCurrentDirectory();
        while (current != null)
        {
            if (File.Exists(Path.Combine(current, "net-questdb-client.sln")))
            {
                return current;
            }

            current = Directory.GetParent(current)?.FullName;
        }

        throw new InvalidOperationException("Could not find project root (net-questdb-client.sln)");
    }

    private string DetectPlatform()
    {
        var os = OperatingSystem.IsWindows() ? "windows" :
                 OperatingSystem.IsMacOS() ? "macos" :
                 OperatingSystem.IsLinux() ? "linux" :
                 throw new NotSupportedException($"Unsupported OS: {RuntimeInformation.OSDescription}");

        var architecture = RuntimeInformation.ProcessArchitecture switch
        {
            System.Runtime.InteropServices.Architecture.X64 => "amd64",
            System.Runtime.InteropServices.Architecture.Arm64 => "arm64",
            _ => throw new NotSupportedException($"Unsupported architecture: {RuntimeInformation.ProcessArchitecture}")
        };

        return $"{os}-{architecture}";
    }

    private async Task<string> GetLatestVersionAsync()
    {
        var latestUrl = "https://api.github.com/repos/questdb/questdb/releases/latest";
        var response = await _httpClient.GetAsync(latestUrl);

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"Failed to fetch latest QuestDB version from {latestUrl}");
        }

        var content = await response.Content.ReadAsStringAsync();
        var json = JsonDocument.Parse(content);
        var tagName = json.RootElement.GetProperty("tag_name").GetString();

        if (string.IsNullOrEmpty(tagName))
        {
            throw new InvalidOperationException("Could not parse version from GitHub API response");
        }

        // Remove 'v' prefix if present
        return tagName.StartsWith("v") ? tagName.Substring(1) : tagName;
    }

    private async Task DownloadAndExtractAsync(string platform, string version)
    {
        var downloadUrl = $"https://github.com/questdb/questdb/releases/download/v{version}/questdb-{version}-{platform}.tar.gz";
        var tarFile = Path.Combine(_questdbDir, $"questdb-{version}-{platform}.tar.gz");
        var extractDir = Path.Combine(_questdbDir, $"questdb-{version}");

        // Check if already extracted
        if (Directory.Exists(extractDir))
        {
            Console.WriteLine($"QuestDB {version} already extracted");
            return;
        }

        try
        {
            // Create directory
            Directory.CreateDirectory(_questdbDir);

            // Download
            Console.WriteLine($"Downloading from {downloadUrl}...");
            var response = await _httpClient.GetAsync(downloadUrl);
            if (!response.IsSuccessStatusCode)
            {
                throw new InvalidOperationException(
                    $"Failed to download QuestDB from {downloadUrl}: HTTP {response.StatusCode}");
            }

            await using var downloadStream = await response.Content.ReadAsStreamAsync();
            await using var fileStream = File.Create(tarFile);
            await downloadStream.CopyToAsync(fileStream);

            // Extract using tar command (available on Unix and Windows 10+)
            Console.WriteLine("Extracting archive...");
            await ExtractTarGzAsync(tarFile, _questdbDir);

            // Make binaries executable
            if (!OperatingSystem.IsWindows())
            {
                MakeExecutable(Path.Combine(extractDir, "bin"));
            }
        }
        finally
        {
            // Clean up tar file
            if (File.Exists(tarFile))
            {
                File.Delete(tarFile);
            }
        }
    }

    private async Task ExtractTarGzAsync(string tarGzFile, string extractPath)
    {
        // Use system tar command for reliable extraction across platforms
        var startInfo = new ProcessStartInfo
        {
            FileName = OperatingSystem.IsWindows() ? "tar" : "/usr/bin/tar",
            Arguments = $"-xzf \"{tarGzFile}\" -C \"{extractPath}\"",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };

        var process = Process.Start(startInfo);
        if (process == null)
        {
            throw new InvalidOperationException("Failed to start tar extraction process");
        }

        var error = await process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();

        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException($"tar extraction failed: {error}");
        }
    }

    private void MakeExecutable(string directoryPath)
    {
        if (!Directory.Exists(directoryPath))
        {
            return;
        }

        var startInfo = new ProcessStartInfo
        {
            FileName = "/bin/chmod",
            Arguments = $"+x \"{directoryPath}\"/*",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };

        var process = Process.Start(startInfo);
        process?.WaitForExit();
    }

    private static string GetExecutableName(string baseName)
    {
        return OperatingSystem.IsWindows() ? $"{baseName}.exe" : baseName;
    }
}
