using System;

namespace Vostok.Metrics.Aggregations.Helpers
{
    internal static class DateTimeOffsetExtensions
    {
        public static bool InInterval(this DateTimeOffset timestamp, DateTimeOffset start, DateTimeOffset end)
        {
            return start <= timestamp && timestamp < end;
        }
    }
}