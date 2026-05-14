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

using System.Net;
using System.Net.Sockets;

namespace net_questdb_client_tests;

public sealed class TcpProxy : IDisposable
{
    private readonly string _backendHost;
    private readonly int _backendPort;
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _cts = new();
    private readonly object _connectionsLock = new();
    private readonly List<(TcpClient Client, TcpClient Backend)> _connections = new();
    private Task? _acceptTask;
    private bool _disposed;

    public TcpProxy(string backendEndpoint)
    {
        var parts = backendEndpoint.Split(':', 2);
        if (parts.Length != 2 || !int.TryParse(parts[1], out var port))
        {
            throw new ArgumentException($"backend endpoint must be host:port, got `{backendEndpoint}`",
                nameof(backendEndpoint));
        }
        _backendHost = parts[0];
        _backendPort = port;
        _listener = new TcpListener(IPAddress.Loopback, 0);
    }

    public string LocalEndpoint { get; private set; } = string.Empty;

    public Task StartAsync()
    {
        if (_acceptTask != null) return Task.CompletedTask;
        _listener.Start();
        var port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        LocalEndpoint = $"127.0.0.1:{port}";
        _acceptTask = AcceptLoopAsync(_cts.Token);
        return Task.CompletedTask;
    }

    public void KillAllConnections()
    {
        List<(TcpClient Client, TcpClient Backend)> snapshot;
        lock (_connectionsLock)
        {
            snapshot = new List<(TcpClient, TcpClient)>(_connections);
            _connections.Clear();
        }

        foreach (var (c, b) in snapshot)
        {
            try { c.Close(); } catch { }
            try { b.Close(); } catch { }
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        try { _cts.Cancel(); } catch { }
        KillAllConnections();
        try { _listener.Stop(); } catch { }
        _cts.Dispose();
    }

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            TcpClient client;
            try { client = await _listener.AcceptTcpClientAsync(ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { return; }
            catch (ObjectDisposedException) { return; }
            catch { continue; }

            _ = HandleClientAsync(client, ct);
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken ct)
    {
        TcpClient? backend = null;
        var registered = false;
        try
        {
            backend = new TcpClient();
            await backend.ConnectAsync(_backendHost, _backendPort, ct).ConfigureAwait(false);

            lock (_connectionsLock)
            {
                _connections.Add((client, backend));
                registered = true;
            }

            var clientToBackend = PipeAsync(client.GetStream(), backend.GetStream(), ct);
            var backendToClient = PipeAsync(backend.GetStream(), client.GetStream(), ct);
            await Task.WhenAny(clientToBackend, backendToClient).ConfigureAwait(false);
        }
        catch
        {
        }
        finally
        {
            if (registered)
            {
                lock (_connectionsLock) { _connections.Remove((client, backend!)); }
            }
            try { client.Close(); } catch { }
            try { backend?.Close(); } catch { }
        }
    }

    private static async Task PipeAsync(NetworkStream src, NetworkStream dst, CancellationToken ct)
    {
        var buf = new byte[8192];
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var n = await src.ReadAsync(buf.AsMemory(), ct).ConfigureAwait(false);
                if (n <= 0) return;
                await dst.WriteAsync(buf.AsMemory(0, n), ct).ConfigureAwait(false);
            }
        }
        catch { }
    }
}
