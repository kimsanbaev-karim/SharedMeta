using System;
using System.Net;
using System.Security.Claims;
using System.Threading.RateLimiting;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;

namespace SharedMeta.Transport.SignalR.RateLimiting
{
    /// <summary>
    /// Caps how fast one caller may invoke hub methods, and refuses the excess with a
    /// <see cref="HubException"/> — the way a hub filter is meant to reject a call.
    /// <para>
    /// The partition key is the authenticated identity when there is one, and the connection id
    /// otherwise. The <c>PlayerId</c> a client sends in the handshake is deliberately NOT used: it
    /// is caller-supplied, so a flooder would simply rotate it and get a fresh budget each time.
    /// Both keys that are used are bounded by things the server issues, which also keeps the
    /// partition table from growing without limit — unbounded keys are a denial-of-service vector
    /// of their own.
    /// </para>
    /// </summary>
    public sealed class MetaHubRateLimitFilter : IHubFilter, IDisposable
    {
        private readonly MetaHubRateLimitOptions _options;
        private readonly PartitionedRateLimiter<HubInvocationContext> _limiter;

        public MetaHubRateLimitFilter(IOptions<MetaHubRateLimitOptions> options)
        {
            _options = options.Value;
            _limiter = PartitionedRateLimiter.Create<HubInvocationContext, string>(CreatePartition);
        }

        public async ValueTask<object?> InvokeMethodAsync(
            HubInvocationContext invocationContext,
            Func<HubInvocationContext, ValueTask<object?>> next)
        {
            if (_options.ExemptMethods.Contains(invocationContext.HubMethodName))
                return await next(invocationContext);

            if (_options.ExemptLoopback && IsLoopback(invocationContext))
                return await next(invocationContext);

            using var lease = await _limiter.AcquireAsync(invocationContext, 1);
            if (lease.IsAcquired) return await next(invocationContext);

            var method = invocationContext.HubMethodName;
            var seconds = _options.Window.TotalSeconds;

            throw new HubException($"Rate limit exceeded for '{method}': more than {_options.PermitLimit} calls per {seconds:0.##}s.");
        }

        public void Dispose()
        {
            _limiter.Dispose();
        }

        private static bool IsLoopback(HubInvocationContext context)
        {
            var address = context.Context.GetHttpContext()?.Connection.RemoteIpAddress;
            if (address == null) return false;

            // A client on the same machine often arrives as ::ffff:127.0.0.1, and IPAddress.IsLoopback
            // says no to that form, so unwrap it first.
            if (address.IsIPv4MappedToIPv6) address = address.MapToIPv4();

            return IPAddress.IsLoopback(address);
        }

        private RateLimitPartition<string> CreatePartition(HubInvocationContext context)
        {
            return RateLimitPartition.GetFixedWindowLimiter(CallerKey(context), BuildLimiterOptions);
        }

        private FixedWindowRateLimiterOptions BuildLimiterOptions(string key)
        {
            return new FixedWindowRateLimiterOptions
            {
                AutoReplenishment = true,
                PermitLimit = _options.PermitLimit,
                Window = _options.Window,
                QueueLimit = _options.QueueLimit,
                QueueProcessingOrder = QueueProcessingOrder.OldestFirst
            };
        }

        private string CallerKey(HubInvocationContext context)
        {
            var identity = context.Context.User?.FindFirst("sub")?.Value
                           ?? context.Context.User?.FindFirst(ClaimTypes.NameIdentifier)?.Value;

            return string.IsNullOrEmpty(identity) ? context.Context.ConnectionId : identity;
        }
    }
}
