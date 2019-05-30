using System;
using FluentAssertions.Extensions;

namespace Vostok.Metrics.Aggregations.Tests
{
    internal static class TestsHelpers
    {
        public static DateTimeOffset TimestampWithSeconds(int seconds)
        {
            return new DateTimeOffset(2019, 01, 01, 00, 00, 00, TimeSpan.Zero) + seconds.Seconds();
        }
    }
}