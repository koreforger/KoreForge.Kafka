using System;
using KoreForge.Time;

namespace KoreForge.Kafka.Core.Diagnostics;

/// <summary>
/// Legacy alias kept for backwards compatibility. Prefer <see cref="KoreForge.Time.ISystemClock"/>.
/// </summary>
[Obsolete("Use KoreForge.Time.ISystemClock instead.")]
public interface ISystemClock : KoreForge.Time.ISystemClock
{
}

/// <summary>
/// Legacy alias that maps to the shared <see cref="KoreForge.Time.SystemClock"/> implementation.
/// </summary>
[Obsolete("Use KoreForge.Time.SystemClock instead.")]
public static class SystemClock
{
    public static KoreForge.Time.ISystemClock Instance => KoreForge.Time.SystemClock.Instance;
}
