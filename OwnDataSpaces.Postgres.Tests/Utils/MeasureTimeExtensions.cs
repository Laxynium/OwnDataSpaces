using System.Diagnostics;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace OwnDataSpaces.Postgres.Tests.Utils;

public static class MeasureTimeExtensions
{
    public static async Task Measure(this IMessageSink sink, string context, Func<Task> action)
    {
        var before = Stopwatch.GetTimestamp();

        await action();

        var elapsed = Stopwatch.GetElapsedTime(before);

        sink.OnMessage(new DiagnosticMessage("[{0}] time={1}ms", context, elapsed.TotalMilliseconds));
    }

    public static async Task<T> Measure<T>(this IMessageSink sink, string context, Func<Task<T>> action)
    {
        var before = Stopwatch.GetTimestamp();

        var result = await action();

        var elapsed = Stopwatch.GetElapsedTime(before);

        sink.OnMessage(new DiagnosticMessage("[{0}] time={1}ms", context, elapsed.TotalMilliseconds));

        return result;
    }

    public static void Measure(this IMessageSink sink, string context, Action action)
    {
        var before = Stopwatch.GetTimestamp();

        action();

        var elapsed = Stopwatch.GetElapsedTime(before);

        sink.OnMessage(new DiagnosticMessage("[{0}] time={1}ms", context, elapsed.TotalMilliseconds));
    }
}