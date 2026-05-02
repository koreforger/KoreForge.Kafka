using System;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace KoreForge.Kafka.Producer.Tests.Support;

internal sealed class RecordingLoggerFactory : ILoggerFactory
{
    private readonly ConcurrentQueue<LogEntry> _entries = new();

    public ConcurrentQueue<LogEntry> Entries => _entries;

    public void AddProvider(ILoggerProvider provider)
    {
    }

    public ILogger CreateLogger(string categoryName) => new RecordingLogger(categoryName, _entries);

    public void Dispose()
    {
    }

    private sealed class RecordingLogger : ILogger
    {
        private readonly string _categoryName;
        private readonly ConcurrentQueue<LogEntry> _entries;

        public RecordingLogger(string categoryName, ConcurrentQueue<LogEntry> entries)
        {
            _categoryName = categoryName;
            _entries = entries;
        }

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var message = formatter?.Invoke(state, exception) ?? state?.ToString() ?? string.Empty;
            _entries.Enqueue(new LogEntry(_categoryName, logLevel, eventId, message, exception));
        }
    }

    private sealed class NullScope : IDisposable
    {
        public static NullScope Instance { get; } = new();
        public void Dispose()
        {
        }
    }
}

internal sealed record LogEntry(string Category, LogLevel Level, EventId EventId, string Message, Exception? Exception);
