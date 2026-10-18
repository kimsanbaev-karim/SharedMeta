using SharedMeta.Client;
using SharedMeta.Core;
using SharedMeta.Debug.InProcess;
using SharedMeta.IntegrationTests.Infrastructure;
using SharedMeta.Test.Meta1;
using SharedMeta.Test.Meta1.Client;
using Xunit;

namespace SharedMeta.IntegrationTests;

/// <summary>
/// Concurrent <c>GetServiceAsync</c> calls against one (entityId, StateType).
///
/// Each call used to subscribe on its own. The loser disposed its network, yet the API client
/// built on that network still overwrote the winner's in the cache — every later resolve handed
/// back a client that threw ObjectDisposedException on every call and never received a broadcast.
///
/// The subscribe is held open by gating CounterConfig resolution (it runs after the network call),
/// so both resolves are guaranteed to be inside the window at once.
/// </summary>
[Collection(TestClusterCollection.Name)]
public class ConcurrentResolveTests
{
    private readonly TestClusterFixture _fixture;

    public ConcurrentResolveTests(TestClusterFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact(Timeout = 60_000)]
    public async Task ConcurrentResolves_OfOneClientType_ShareOneLiveClient()
    {
        var server = new InProcessServer(_fixture.CreateHandlerFactory());
        await using var client = new TestClientSetup(server, "race-same-" + Guid.NewGuid().ToString("N")[..8]);
        await using var observer = new TestClientSetup(server, "race-obs-" + Guid.NewGuid().ToString("N")[..8]);
        await client.ConnectAsync();
        await observer.ConnectAsync();
        var resolver = client.CreateResolver();
        var entityId = $"counter_{Guid.NewGuid():N}";

        var gate = new GatedConfigProvider<CounterConfig>();
        resolver.RegisterConfigProvider(gate);

        var a = resolver.GetServiceAsync<CounterServiceApiClient>(entityId);
        var b = resolver.GetServiceAsync<CounterServiceApiClient>(entityId);
        await gate.WaitUntilRequestedAsync();
        await Task.Delay(100);
        gate.Release(new CounterConfig());

        var apiA = await a;
        var apiB = await b;
        var apiC = await resolver.GetServiceAsync<CounterServiceApiClient>(entityId);

        Assert.Same(apiA, apiB);
        Assert.Same(apiA, apiC);

        await apiA.AddValueAsync(1, 1);
        await apiB.AddValueAsync(2, 2);
        await apiC.AddValueAsync(3, 3);

        // A broadcast from another player must reach the cached client.
        var replayed = new List<(int value, int seq)>();
        apiC.OnAddValue_Replayed += args => replayed.Add(args);
        var other = await observer.CreateResolver().GetServiceAsync<CounterServiceApiClient>(entityId);
        await other.AddValueAsync(10, 1);
        await Task.Delay(100);

        Assert.Contains((10, 1), replayed);
        Assert.Equal(16, resolver.GetState<CounterState>(entityId).Sum);
    }

    [Fact(Timeout = 60_000)]
    public async Task ConcurrentResolves_OfTwoClientTypesOnOneState_BothStayLive()
    {
        var server = new InProcessServer(_fixture.CreateHandlerFactory());
        await using var client = new TestClientSetup(server, "race-mixed-" + Guid.NewGuid().ToString("N")[..8]);
        await client.ConnectAsync();
        var resolver = client.CreateResolver();
        var entityId = $"counter_{Guid.NewGuid():N}";

        var gate = new GatedConfigProvider<CounterConfig>();
        resolver.RegisterConfigProvider(gate);

        var counter = resolver.GetServiceAsync<CounterServiceApiClient>(entityId);
        var aux = resolver.GetServiceAsync<CounterAuxServiceApiClient>(entityId);
        await gate.WaitUntilRequestedAsync();
        await Task.Delay(100);
        gate.Release(new CounterConfig());

        var counterApi = await counter;
        var auxApi = await aux;

        Assert.Same(counterApi, await resolver.GetServiceAsync<CounterServiceApiClient>(entityId));
        Assert.Same(auxApi, await resolver.GetServiceAsync<CounterAuxServiceApiClient>(entityId));

        await counterApi.AddValueAsync(4, 1);
        await auxApi.AuxAddAsync(5);

        Assert.Equal(9, resolver.GetState<CounterState>(entityId).Sum);
    }

    [Fact(Timeout = 60_000)]
    public async Task DisconnectDuringSubscribe_FailsTheResolveAndLeavesNothingCached()
    {
        var server = new InProcessServer(_fixture.CreateHandlerFactory());
        await using var client = new TestClientSetup(server, "race-cancel-" + Guid.NewGuid().ToString("N")[..8]);
        await client.ConnectAsync();
        var resolver = client.CreateResolver();
        var entityId = $"counter_{Guid.NewGuid():N}";

        var gate = new GatedConfigProvider<CounterConfig>();
        resolver.RegisterConfigProvider(gate);

        var pending = resolver.GetServiceAsync<CounterServiceApiClient>(entityId);
        await gate.WaitUntilRequestedAsync();

        await resolver.DisconnectAsync(entityId);

        // Fails right away — not once the abandoned network call returns.
        await Assert.ThrowsAsync<OperationCanceledException>(() => pending);

        gate.Release(new CounterConfig());
        await Task.Delay(100);

        // The abandoned subscribe must not resurrect the connection once it completes.
        Assert.False(resolver.TryGetState<CounterState>(entityId, out _));

        // And the entity resolves cleanly afterwards.
        var api = await resolver.GetServiceAsync<CounterServiceApiClient>(entityId);
        await api.AddValueAsync(7, 1);
        Assert.Equal(7, resolver.GetState<CounterState>(entityId).Sum);
    }

    [Fact(Timeout = 60_000)]
    public async Task FailedSubscribe_IsRetriedByTheNextResolve()
    {
        var server = new InProcessServer(_fixture.CreateHandlerFactory());
        await using var client = new TestClientSetup(server, "race-fail-" + Guid.NewGuid().ToString("N")[..8]);
        await client.ConnectAsync();
        var resolver = client.CreateResolver();
        var entityId = $"counter_{Guid.NewGuid():N}";

        int calls = 0;
        resolver.RegisterConfigProvider(new DelegateConfigProvider<CounterConfig>(_ =>
            Interlocked.Increment(ref calls) == 1
                ? Task.FromException<CounterConfig>(new InvalidOperationException("config download failed"))
                : Task.FromResult(new CounterConfig())));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => resolver.GetServiceAsync<CounterServiceApiClient>(entityId));
        Assert.Contains("config download failed", ex.Message);

        var api = await resolver.GetServiceAsync<CounterServiceApiClient>(entityId);
        await api.AddValueAsync(3, 1);
        Assert.Equal(3, resolver.GetState<CounterState>(entityId).Sum);
    }

    /// <summary>Holds config resolution — and with it the subscribe — until released.</summary>
    private sealed class GatedConfigProvider<T> : IClientMetaConfigProvider<T> where T : class
    {
        private readonly TaskCompletionSource<T> _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _requested = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<T> GetConfigAsync(MetaConfigVersion version)
        {
            _requested.TrySetResult();
            return _gate.Task;
        }

        public Task WaitUntilRequestedAsync() => _requested.Task;

        public void Release(T config) => _gate.TrySetResult(config);
    }

    private sealed class DelegateConfigProvider<T> : IClientMetaConfigProvider<T> where T : class
    {
        private readonly Func<MetaConfigVersion, Task<T>> _get;
        public DelegateConfigProvider(Func<MetaConfigVersion, Task<T>> get) => _get = get;
        public Task<T> GetConfigAsync(MetaConfigVersion version) => _get(version);
    }
}
