using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SharedMeta.Transport.SignalR.RateLimiting;
using Xunit;

namespace SharedMeta.Transport.SignalR.Tests;

/// <summary>
/// The rate limit over a real SignalR connection: a hub, a socket, and a client that keeps calling.
/// A limiter that is never reached by a live call is worth nothing, so the test drives the same
/// path a game client does instead of constructing invocation contexts by hand.
/// </summary>
public class MetaHubRateLimitFilterTests : IAsyncLifetime
{
    public class CountingHub : Hub
    {
        public static int Served;

        public Task<int> RpcCall()
        {
            return Task.FromResult(Interlocked.Increment(ref Served));
        }

        public Task<int> SessionConnect()
        {
            return Task.FromResult(Interlocked.Increment(ref Served));
        }
    }

    private const int PERMITS = 3;
    private const int PORT = 5211;
    private const int LOOPBACK_EXEMPT_PORT = 5212;

    private WebApplication _app = null!;

    public async Task InitializeAsync()
    {
        CountingHub.Served = 0;

        _app = await StartHostAsync(PORT, exemptLoopback: false);
    }

    public async Task DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }

    [Fact(Timeout = 60_000)]
    public async Task ClientSpendsItsBudget_TheNextCallIsRefused()
    {
        await using var client = await ConnectAsync();

        for (var i = 0; i < PERMITS; i++)
        {
            await client.InvokeAsync<int>(nameof(CountingHub.RpcCall));
        }

        var refused = await Assert.ThrowsAsync<HubException>(() => client.InvokeAsync<int>(nameof(CountingHub.RpcCall)));

        Assert.Equal(PERMITS, CountingHub.Served);
        Assert.Contains("Rate limit exceeded", refused.Message);
    }

    [Fact(Timeout = 60_000)]
    public async Task SecondClientArrivesAfterTheFirstFlooded_ItStillGetsItsOwnBudget()
    {
        await using var flooder = await ConnectAsync();
        for (var i = 0; i < PERMITS; i++)
        {
            await flooder.InvokeAsync<int>(nameof(CountingHub.RpcCall));
        }

        await Assert.ThrowsAsync<HubException>(() => flooder.InvokeAsync<int>(nameof(CountingHub.RpcCall)));

        await using var newcomer = await ConnectAsync();
        var servedBefore = CountingHub.Served;

        for (var i = 0; i < PERMITS; i++)
        {
            await newcomer.InvokeAsync<int>(nameof(CountingHub.RpcCall));
        }

        Assert.Equal(servedBefore + PERMITS, CountingHub.Served);
    }

    [Fact(Timeout = 60_000)]
    public async Task HandshakeIsExempt_AReconnectStormIsNotThrottled()
    {
        await using var client = await ConnectAsync();

        for (var i = 0; i < PERMITS * 5; i++)
        {
            await client.InvokeAsync<int>(nameof(CountingHub.SessionConnect));
        }

        Assert.Equal(PERMITS * 5, CountingHub.Served);
    }

    [Fact(Timeout = 60_000)]
    public async Task LoopbackIsExempt_ALocalToolKeepsCallingPastTheBudget()
    {
        var app = await StartHostAsync(LOOPBACK_EXEMPT_PORT, exemptLoopback: true);
        try
        {
            await using var local = await ConnectAsync(LOOPBACK_EXEMPT_PORT);

            for (var i = 0; i < PERMITS * 5; i++)
            {
                await local.InvokeAsync<int>(nameof(CountingHub.RpcCall));
            }

            Assert.Equal(PERMITS * 5, CountingHub.Served);
        }
        finally
        {
            await app.StopAsync();
            await app.DisposeAsync();
        }
    }

    private static async Task<WebApplication> StartHostAsync(int port, bool exemptLoopback)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
        builder.Logging.ClearProviders();
        builder.Services.AddSignalR().AddMetaHubRateLimiting(o =>
        {
            o.PermitLimit = PERMITS;
            o.Window = TimeSpan.FromMinutes(5);
            o.ExemptLoopback = exemptLoopback;
        });

        var app = builder.Build();
        app.MapHub<CountingHub>("/counting");

        await app.StartAsync();

        return app;
    }

    private static async Task<HubConnection> ConnectAsync(int port = PORT)
    {
        var connection = new HubConnectionBuilder().WithUrl($"http://127.0.0.1:{port}/counting").Build();
        await connection.StartAsync();

        return connection;
    }
}
