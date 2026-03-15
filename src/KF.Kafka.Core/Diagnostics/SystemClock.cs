using System;
using KF.Time;

namespace KF.Kafka.Core.Diagnostics;

/// <summary>
/// Legacy alias kept for backwards compatibility. Prefer <see cref="KF.Time.ISystemClock"/>.
/// </summary>
[Obsolete("Use KF.Time.ISystemClock instead.")]
public interface ISystemClock : KF.Time.ISystemClock
{
}

/// <summary>
/// Legacy alias that maps to the shared <see cref="KF.Time.SystemClock"/> implementation.
/// </summary>
[Obsolete("Use KF.Time.SystemClock instead.")]
public static class SystemClock
{
    public static KF.Time.ISystemClock Instance => KF.Time.SystemClock.Instance;
}
