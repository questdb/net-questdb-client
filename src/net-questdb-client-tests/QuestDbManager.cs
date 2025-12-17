using System.Diagnostics;

namespace net_questdb_client_tests;

/// <summary>
///     Manages QuestDB server lifecycle for integration tests using Docker.
///     Handles pulling, starting, and stopping QuestDB container instances.
/// </summary>
public class QuestDbManager : IAsyncDisposable
{
    private const string DockerImage = "questdb/questdb:latest";
    private const string ContainerNamePrefix = "questdb-test-";
    private readonly string _containerName;
    private readonly HttpClient _httpClient;
    private readonly int _httpPort;

    private readonly int _port;
    private string? _containerId;
    private string? _volumeName;

    /// <summary>
    ///     Initializes a new instance of the QuestDbManager.
    /// </summary>
    /// <param name="port">ILP port (default: 9009)</param>
    /// <summary>
    /// Initializes a QuestDbManager configured to manage a QuestDB Docker container for integration tests.
    /// </summary>
    /// <param name="port">Local ILP port to expose (container port 9009).</param>
    /// <param name="httpPort">Local HTTP port to expose (container port 9000).</param>
    public QuestDbManager(int port = 9009, int httpPort = 9000)
    {
        _port          = port;
        _httpPort      = httpPort;
        _containerName = $"{ContainerNamePrefix}{port}-{httpPort}-{Guid.NewGuid().ToString().Substring(0, 8)}";
        _httpClient    = new HttpClient { Timeout = TimeSpan.FromSeconds(5), };
    }

    public bool IsRunning { get; private set; }

    /// <summary>
    ///     Cleanup resources.
    /// <summary>
    /// Stops the QuestDB container, removes the configured Docker volume if set, and disposes the internal HTTP client.
    /// </summary>
    /// <returns>A ValueTask that completes when disposal has finished.</returns>
    public async ValueTask DisposeAsync()
    {
        await StopAsync();

        // Clean up Docker volume if one was used
        if (!string.IsNullOrEmpty(_volumeName))
        {
            await RunDockerCommandAsync($"volume rm {_volumeName}");
        }

        _httpClient?.Dispose();
    }

    /// <summary>
    ///     Sets a Docker volume to be used for persistent storage.
    /// <summary>
    /// Sets the Docker volume name to mount into the QuestDB container for persistent storage.
    /// </summary>
    /// <param name="volumeName">Docker volume name to be mounted at /var/lib/questdb inside the container.</param>
    public void SetVolume(string volumeName)
    {
        _volumeName = volumeName;
    }

    /// <summary>
    ///     Ensures Docker is available.
    /// <summary>
    /// Verifies that the Docker CLI is available and functioning on the host.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown if Docker is not available or the availability check fails. If the failure is caused by an unexpected error, the original exception is provided as the InnerException.
    /// </exception>
    public async Task EnsureDockerAvailableAsync()
    {
        try
        {
            var (exitCode, output) = await RunDockerCommandAsync("--version");
            if (exitCode != 0)
            {
                throw new InvalidOperationException("Docker is not available or not working properly");
            }

            Console.WriteLine($"Docker is available: {output.Trim()}");
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                "Docker is required to run integration tests. " +
                "Please install Docker from https://docs.docker.com/get-docker/",
                ex);
        }
    }

    /// <summary>
    ///     Ensures QuestDB Docker image is available (uses local if exists, otherwise pulls latest).
    /// <summary>
    /// Ensures the QuestDB Docker image is available locally, pulling it if necessary.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if the docker pull command fails; the exception message contains the command output.</exception>
    public async Task PullImageAsync()
    {
        // Check if image already exists locally
        if (await ImageExistsAsync())
        {
            Console.WriteLine($"Docker image already exists locally: {DockerImage}");
            return;
        }

        Console.WriteLine($"Pulling Docker image {DockerImage}...");
        var (exitCode, output) = await RunDockerCommandAsync($"pull {DockerImage}");
        if (exitCode != 0)
        {
            throw new InvalidOperationException($"Failed to pull Docker image: {output}");
        }

        Console.WriteLine("Docker image pulled successfully");
    }

    /// <summary>
    ///     Checks if the QuestDB Docker image exists locally.
    /// <summary>
    /// Checks whether the QuestDB Docker image is available locally.
    /// </summary>
    /// <returns>`true` if the image exists locally (Docker returns a non-empty image ID), `false` otherwise.</returns>
    private async Task<bool> ImageExistsAsync()
    {
        // Use 'docker images' to check if image exists
        // Format: docker images --filter "reference=questdb/questdb:latest" --quiet
        var (exitCode, output) = await RunDockerCommandAsync($"images --filter \"reference={DockerImage}\" --quiet");

        // If the image exists, output will contain the image ID
        // If it doesn't exist, output will be empty
        return exitCode == 0 && !string.IsNullOrWhiteSpace(output);
    }

    /// <summary>
    ///     Starts the QuestDB container.
    /// <summary>
    /// Starts a QuestDB Docker container configured with the manager's ports and optional mounted volume, waits until the server responds to its HTTP /settings endpoint, and marks the manager as running.
    /// </summary>
    /// <returns>A task that completes when the container has been started and QuestDB is responsive.</returns>
    public async Task StartAsync()
    {
        if (IsRunning)
        {
            Console.WriteLine("QuestDB is already running");
            return;
        }

        await EnsureDockerAvailableAsync();

        // Clean up any existing containers using these ports
        await CleanupExistingContainersAsync();

        await PullImageAsync();

        Console.WriteLine($"Starting QuestDB container: {_containerName}");
        Console.WriteLine($"HTTP port: {_httpPort}, ILP port: {_port}");

        // Run container with port mappings
        // -d: detached mode
        // -p: port mappings
        // --name: container name
        // -v: volume mount (if specified)
        var volumeArg = string.IsNullOrEmpty(_volumeName)
                            ? string.Empty
                            : $"-v {_volumeName}:/var/lib/questdb ";

        var runArgs = $"run -d " +
                      $"-p {_httpPort}:9000 " +
                      $"-p {_port}:9009 " +
                      $"--name {_containerName} " +
                      volumeArg +
                      DockerImage;

        var (exitCode, output) = await RunDockerCommandAsync(runArgs);
        if (exitCode != 0)
        {
            throw new InvalidOperationException($"Failed to start QuestDB container: {output}");
        }

        _containerId = output.Trim();
        Console.WriteLine($"QuestDB container started: {_containerId}");
   

        // Wait for QuestDB to be ready
        await WaitForQuestDbAsync();
        IsRunning = true;
    }

    /// <summary>
    ///     Stops the QuestDB container.
    /// <summary>
    /// Stops the managed QuestDB Docker container if it is running.
    /// </summary>
    /// <remarks>
    /// Attempts a graceful stop with a 10-second timeout; if that fails, attempts a force removal.
    /// On success or failure, clears the manager's running state and stored container ID.
    /// </remarks>
    /// <returns>A task that completes after the container has been stopped or force-removed and internal state has been updated.</returns>
    public async Task StopAsync()
    {
        if (!IsRunning || string.IsNullOrEmpty(_containerId))
        {
            return;
        }

        Console.WriteLine($"Stopping QuestDB container: {_containerName}");

        // Stop the container (with 10 second timeout)
        var (exitCode, output) = await RunDockerCommandAsync($"stop -t 10 {_containerName}");
        if (exitCode != 0)
        {
            Console.WriteLine($"Warning: Failed to stop container gracefully: {output}");
            // Try force remove
            await RunDockerCommandAsync($"rm -f {_containerName}");
        }

        IsRunning    = false;
        _containerId = null;
        Console.WriteLine("QuestDB container stopped");
    }

    /// <summary>
    ///     Gets the HTTP endpoint for QuestDB.
    /// <summary>
    /// Gets the HTTP endpoint host and port for the managed QuestDB instance.
    /// </summary>
    /// <returns>The HTTP endpoint in the form "localhost:{port}".</returns>
    public string GetHttpEndpoint()
    {
        return $"localhost:{_httpPort}";
    }

    /// <summary>
    ///     Gets the ILP endpoint for QuestDB.
    /// <summary>
    /// ILP endpoint host and port for the QuestDB instance.
    /// </summary>
    /// <returns>The ILP endpoint string in the form "localhost:{port}".</returns>
    public string GetIlpEndpoint()
    {
        return $"localhost:{_port}";
    }

    /// <summary>
    ///     Waits for QuestDB to be ready.
    /// <summary>
    /// Waits until QuestDB responds successfully to its /settings HTTP endpoint or fails after a timeout.
    /// </summary>
    /// <exception cref="TimeoutException">Thrown if QuestDB does not respond successfully within 120 seconds.</exception>
    private async Task WaitForQuestDbAsync()
    {
        const int maxAttempts = 120;
        var       attempts    = 0;

        while (attempts < maxAttempts)
        {
            try
            {
                var response = await _httpClient.GetAsync($"http://{GetHttpEndpoint()}/settings");
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

        throw new TimeoutException("QuestDB failed to start within 120 seconds");
    }

    /// <summary>
    /// Finds and removes local QuestDB test containers that match the instance's port and name pattern.
    /// </summary>
    /// <remarks>
    /// Inspects all containers (running and stopped) and, for any whose name contains the configured container prefix and the HTTP/ILP port combination, attempts to stop (with a short timeout) and remove the container. If listing containers fails, the method returns without throwing.
    /// </remarks>
    private async Task CleanupExistingContainersAsync()
    {
        Console.WriteLine($"Checking for existing containers on ports {_httpPort}/{_port}...");

        // Get list of all containers (running and stopped)
        var (exitCode, output) = await RunDockerCommandAsync("ps -a --format \"{{.Names}}\"");
        if (exitCode != 0)
        {
            return; // Silently ignore errors listing containers
        }

        var containerNames = output.Split('\n', StringSplitOptions.RemoveEmptyEntries);

        // Stop and remove any QuestDB test containers
        foreach (var rawName in containerNames)
        {
            // Trim the name to remove trailing \r or whitespace
            var name = rawName.Trim();

            // Look for containers with matching port pattern: questdb-test-{port}-{httpPort}-*
            if (name.Contains(ContainerNamePrefix, StringComparison.Ordinal) &&
                (name.Contains($"-{_port}-{_httpPort}-", StringComparison.Ordinal) ||
                 name.Contains($"-{_httpPort}-{_port}-", StringComparison.Ordinal)))
            {
                Console.WriteLine($"Cleaning up existing container: {name}");

                // Stop the container
                await RunDockerCommandAsync($"stop -t 5 {name}");

                // Remove the container
                await RunDockerCommandAsync($"rm {name}");
            }
        }
    }

    /// <summary>
    /// Executes a Docker CLI command with the provided arguments and captures its output.
    /// </summary>
    /// <param name="arguments">Command-line arguments passed to the `docker` executable (e.g., "ps -a").</param>
    /// <returns>
    /// A tuple where `ExitCode` is the process exit code and `Output` is the combined standard output and standard error text produced by the command.
    /// </returns>
    /// <exception cref="InvalidOperationException">Thrown if the `docker` process could not be started.</exception>
    private async Task<(int ExitCode, string Output)> RunDockerCommandAsync(string arguments)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName               = "docker",
            Arguments              = arguments,
            UseShellExecute        = false,
            RedirectStandardOutput = true,
            RedirectStandardError  = true,
            CreateNoWindow         = true,
        };

        var process = Process.Start(startInfo);
        if (process == null)
        {
            throw new InvalidOperationException("Failed to start docker command");
        }

        var outputTask = process.StandardOutput.ReadToEndAsync();
        var errorTask  = process.StandardError.ReadToEndAsync();
        await Task.WhenAll(outputTask, errorTask);
        var output = await outputTask;
        var error  = await errorTask;
        await process.WaitForExitAsync();

        return (process.ExitCode, output + error);
    }
}