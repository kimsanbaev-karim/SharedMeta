using System;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;

namespace SharedMeta.Transport.SignalR.RateLimiting
{
    /// <summary>
    /// Registration for <see cref="MetaHubRateLimitFilter"/>.
    /// </summary>
    public static class MetaHubRateLimitExtensions
    {
        /// <summary>
        /// Adds per-caller rate limiting to every hub in the app. The filter is a singleton because
        /// it owns the partition table: a per-invocation instance would hand every call a fresh
        /// budget and limit nothing.
        /// </summary>
        public static ISignalRServerBuilder AddMetaHubRateLimiting(
            this ISignalRServerBuilder builder,
            Action<MetaHubRateLimitOptions>? configure = null)
        {
            if (configure != null) builder.Services.Configure(configure);

            builder.Services.AddSingleton<MetaHubRateLimitFilter>();
            builder.Services.Configure<HubOptions>(o => o.AddFilter<MetaHubRateLimitFilter>());

            return builder;
        }
    }
}
