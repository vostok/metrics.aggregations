using FluentAssertions;
using NUnit.Framework;
using Vostok.Metrics.Aggregations.Helpers;

namespace Vostok.Metrics.Aggregations.Tests.Helpers
{
    [TestFixture]
    internal class DateTimeOffsetExtensions_Tests
    {
        [TestCase(40, 42, 50, true)]
        [TestCase(41, 42, 50, true)]
        [TestCase(42, 42, 50, true)]
        [TestCase(43, 42, 50, false)]
        [TestCase(40, 42, 42, false)]
        [TestCase(40, 42, 41, false)]
        public void InInterval_should_check_from_start_inclusive_to_end_exclusive(
            int secondsStart,
            int secondsTimestamp,
            int secondsEnd,
            bool expected)
        {
            var start = TestsHelpers.TimestampWithSeconds(secondsStart);
            var timestamp = TestsHelpers.TimestampWithSeconds(secondsTimestamp);
            var end = TestsHelpers.TimestampWithSeconds(secondsEnd);

            timestamp.InInterval(start, end).Should().Be(expected);
        }
    }
}