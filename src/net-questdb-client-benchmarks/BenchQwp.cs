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
 ******************************************************************************/

using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using BenchmarkDotNet.Attributes;
using QuestDB;

namespace net_questdb_client_benchmarks;

/// <summary>
///     §4.1 — Microbenchmarks comparing QWP transports against the legacy ILP
///     transports for ingestion. Targets local fake servers (UDP socket sink + a
///     minimal WebSocket echo) — relative numbers are meaningful, absolute numbers
///     reflect localhost+kernel overhead, not real-network behaviour.
/// </summary>
[MarkdownExporterAttribute.GitHub]
[MemoryDiagnoser]
public class BenchQwp
{
    private UdpEchoSink? _udpSink;
    private MinimalEchoWebSocketServer? _wsServer;

    [Params(1_000, 10_000)]
    public int RowsPerIteration;

    [GlobalSetup]
    public void Setup()
    {
        _udpSink = new UdpEchoSink();
        _wsServer = MinimalEchoWebSocketServer.StartAsync().GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _udpSink?.Dispose();
        _wsServer?.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    [Benchmark(Baseline = true)]
    public async Task QwpUdp()
    {
        using var sender = Sender.New($"udp::addr=127.0.0.1:{_udpSink!.Port};auto_flush=off;");
        for (var i = 0; i < RowsPerIteration; i++)
        {
            sender.Table("qwp_bench").Symbol("k", "v").Column("number", i).At(DateTime.UtcNow);
        }
        await sender.SendAsync();
    }

    [Benchmark]
    public async Task QwpWebSocket()
    {
        using var sender = Sender.New($"ws::addr=127.0.0.1:{_wsServer!.Port};auto_flush=off;");
        for (var i = 0; i < RowsPerIteration; i++)
        {
            sender.Table("qwp_bench").Symbol("k", "v").Column("number", i).At(DateTime.UtcNow);
        }
        await sender.SendAsync();
    }

    /// <summary>
    ///     UDP socket bound to 127.0.0.1:0 that drops every received datagram on the
    ///     floor. Equivalent to a /dev/null sink — measures the sender's encode +
    ///     send path without server-side processing overhead.
    /// </summary>
    private sealed class UdpEchoSink : IDisposable
    {
        private readonly Socket _socket;
        private readonly CancellationTokenSource _cts = new();
        private readonly Task _drain;

        public UdpEchoSink()
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            _socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            Port = ((IPEndPoint)_socket.LocalEndPoint!).Port;
            _drain = Task.Run(DrainLoop);
        }

        public int Port { get; }

        private async Task DrainLoop()
        {
            var buf = new byte[64 * 1024];
            while (!_cts.IsCancellationRequested)
            {
                try
                {
                    await _socket.ReceiveAsync(buf, SocketFlags.None, _cts.Token).ConfigureAwait(false);
                }
                catch { return; }
            }
        }

        public void Dispose()
        {
            _cts.Cancel();
            _socket.Close();
            try { _drain.GetAwaiter().GetResult(); } catch { /* swallow */ }
            _cts.Dispose();
        }
    }

    /// <summary>
    ///     HttpListener-backed WebSocket server. Drains every received frame; emits
    ///     no responses. Measures sender-side encode + WS framing — auto_flush=off
    ///     batches everything into a single Send call.
    /// </summary>
    private sealed class MinimalEchoWebSocketServer : IAsyncDisposable
    {
        private readonly HttpListener _listener;
        private readonly CancellationTokenSource _cts = new();
        private Task? _acceptLoop;

        private MinimalEchoWebSocketServer(HttpListener listener) => _listener = listener;

        public int Port { get; private set; }

        public static async Task<MinimalEchoWebSocketServer> StartAsync()
        {
            using var probe = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            var port = ((IPEndPoint)probe.LocalEndPoint!).Port;
            probe.Close();

            var listener = new HttpListener();
            listener.Prefixes.Add($"http://127.0.0.1:{port}/");
            listener.Start();

            var server = new MinimalEchoWebSocketServer(listener) { Port = port };
            server._acceptLoop = Task.Run(server.AcceptLoop);
            await Task.Yield();
            return server;
        }

        private async Task AcceptLoop()
        {
            while (!_cts.IsCancellationRequested)
            {
                HttpListenerContext ctx;
                try { ctx = await _listener.GetContextAsync().ConfigureAwait(false); }
                catch { return; }

                _ = Task.Run(async () =>
                {
                    try
                    {
                        var wsCtx = await ctx.AcceptWebSocketAsync(null).ConfigureAwait(false);
                        using var ws = wsCtx.WebSocket;
                        var buf = new byte[64 * 1024];
                        while (!_cts.IsCancellationRequested && ws.State == WebSocketState.Open)
                        {
                            try
                            {
                                var result = await ws.ReceiveAsync(buf, _cts.Token).ConfigureAwait(false);
                                if (result.MessageType == WebSocketMessageType.Close) break;
                            }
                            catch { return; }
                        }
                    }
                    catch { /* swallow — server shutting down */ }
                });
            }
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();
            try { _listener.Stop(); } catch { /* swallow */ }
            try { _listener.Close(); } catch { /* swallow */ }
            if (_acceptLoop is not null)
            {
                try { await _acceptLoop.ConfigureAwait(false); } catch { /* swallow */ }
            }
            _cts.Dispose();
        }
    }
}
