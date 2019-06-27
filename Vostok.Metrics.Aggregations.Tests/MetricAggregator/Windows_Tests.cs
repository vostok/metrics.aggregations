using System;
using System.Linq;
using System.Threading;
using FluentAssertions;
using FluentAssertions.Extensions;
using NUnit.Framework;
using Vostok.Hercules.Client.Abstractions.Models;
using Vostok.Metrics.Aggregations.MetricAggregator;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.Tests.MetricAggregator
{
    [TestFixture]
    internal class Windows_Tests
    {
        private readonly TimeSpan period = 1.Seconds();
        private readonly TimeSpan lag = 0.3.Seconds();

        [Test]
        public void Aggregate_should_close_windows_after_lag_elapsed_using_max_observed_timestamp()
        {
            var windows = FilledWindows();

            windows.Aggregate().AggregatedEvents.Single().Value.Should().Be(45);
        }

        [Test]
        public void Aggregate_should_close_windows_after_lag_plus_period_elapsed_since_last_event_add()
        {
            var windows = FilledWindows();

            windows.Aggregate().AggregatedEvents.Single().Value.Should().Be(45);
            windows.Aggregate().AggregatedEvents.Should().BeEmpty();

            Thread.Sleep(period + lag);

            windows.Aggregate().AggregatedEvents.Single().Value.Should().Be(46);
        }

        [Test]
        public void Aggregate_should_calculate_statistic()
        {
            var windows = FilledWindows();

            var result = windows.Aggregate();
            result.AggregatedEvents.Single().Value.Should().Be(45);
            result.ActiveEventsCount.Should().Be(4);
            result.ActiveWindowsCount.Should().Be(1);
            result.FirstActiveEventCoordinates.Should()
                .BeEquivalentTo(
                    new StreamCoordinates(
                        new[]
                        {
                            new StreamPosition
                            {
                                Offset = 10,
                                Partition = 42
                            }
                        }));
        }

        [Test]
        public void AddEvent_should_reject_all_before_first_open_window()
        {
            var windows = FilledWindows();

            windows.Aggregate().AggregatedEvents.Single().Value.Should().Be(45);

            windows.AddEvent(
                    new MetricEvent(
                        100,
                        new MetricTags(1).Append("key", "value"),
                        TestsHelpers.TimestampWithSeconds(0.9),
                        null,
                        null,
                        null),
                    StreamCoordinates.Empty)
                .Should()
                .BeFalse();

            windows.AddEvent(
                    new MetricEvent(
                        100,
                        new MetricTags(1).Append("key", "value"),
                        TestsHelpers.TimestampWithSeconds(1),
                        null,
                        null,
                        null),
                    StreamCoordinates.Empty)
                .Should()
                .BeTrue();
        }

        private Windows FilledWindows()
        {
            var windows = new Windows(() => new TestsHelpers.SumValues(), period, lag);

            for (var seconds = 0; seconds <= 13; seconds++)
            {
                windows.AddEvent(
                        new MetricEvent(
                            seconds,
                            new MetricTags(1).Append("key", "value"),
                            TestsHelpers.TimestampWithSeconds(seconds / 10.0),
                            null,
                            null,
                            null),
                        new StreamCoordinates(
                            new[]
                            {
                                new StreamPosition
                                {
                                    Offset = seconds,
                                    Partition = 42
                                }
                            }))
                    .Should()
                    .BeTrue();
            }

            return windows;
        }
    }
}