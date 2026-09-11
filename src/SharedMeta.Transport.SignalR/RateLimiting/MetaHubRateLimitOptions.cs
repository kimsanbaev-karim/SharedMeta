using System;
using System.Collections.Generic;

namespace SharedMeta.Transport.SignalR.RateLimiting
{
    /// <summary>
    /// Per-caller limits for hub method invocations. The ASP.NET Core rate limiting middleware does
    /// not reach these: it runs on HTTP requests, and once the transport has upgraded, every RPC
    /// travels inside one long-lived connection that the middleware never sees again. A hub filter
    /// is the layer that does see them, so the limits live here.
    /// </summary>
    public class MetaHubRateLimitOptions
    {
        /// <summary>
        /// Calls a single caller may make inside <see cref="Window"/>. Sized for a game client that
        /// batches its own input, not for a UI that fires one RPC per keystroke - measure your own
        /// traffic before lowering it.
        /// </summary>
        public int PermitLimit { get; set; } = 60;

        /// <summary>Length of the window <see cref="PermitLimit"/> applies to.</summary>
        public TimeSpan Window { get; set; } = TimeSpan.FromSeconds(1);

        /// <summary>
        /// Calls held back to wait for a permit instead of being rejected outright. Zero by default:
        /// a queued game command arrives late, and late is usually worse than refused, because the
        /// client has already predicted the outcome and moved on.
        /// </summary>
        public int QueueLimit { get; set; }

        /// <summary>
        /// Let calls arriving from a loopback address (127.0.0.0/8, ::1) through without spending a
        /// permit — for admin tooling, health probes and integration tests that share the machine
        /// with the silo.
        /// <para>
        /// Off by default, and that default is deliberate: behind a reverse proxy on the same host
        /// every player's connection also arrives from loopback, so switching this on there disables
        /// the limit for the entire internet. Turn it on only when the silo is reached directly, or
        /// after the host is configured to restore the real client address (forwarded headers).
        /// </para>
        /// </summary>
        public bool ExemptLoopback { get; set; }

        /// <summary>
        /// Methods that bypass the limit entirely. The handshake and the acknowledgement path are
        /// exempt by default: throttling them turns a reconnect storm - exactly what happens after a
        /// server restart - into a queue of clients that cannot get back in.
        /// </summary>
        public HashSet<string> ExemptMethods { get; } = new HashSet<string>(StringComparer.Ordinal)
        {
            "SessionConnect",
            "RegisterClientSignature",
            "AcknowledgeSequence",
            "GracefulDisconnect"
        };
    }
}
