using System;
using System.Linq;
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
        private readonly TimeSpan windowSize = 10.Seconds();
        private readonly TimeSpan windowLag = 3.Seconds();

        [Test]
        public void AddEvent_should_build_windows()
        {
            var windows = FilledWindows();

            var aggregate = new TestsHelpers.SumValues();

            var result = windows.Aggregate(aggregate, TestsHelpers.TimestampWithSeconds(60)).AggregatedEvents.ToList();
            result.Count.Should().Be(2);
            result[0].Value.Should().Be(45);
            result[1].Value.Should().Be(46);
        }

        [Test]
        public void AddEvent_should_reject_all_before_first_open_window()
        {
            var windows = FilledWindows();

            var aggregate = new TestsHelpers.SumValues();
            
            windows.Aggregate(aggregate, TestsHelpers.TimestampWithSeconds(-100)).AggregatedEvents.Single().Value.Should().Be(45);

            windows.AddEvent(
                    new MetricEvent(
                        100,
                        new MetricTags(1).Append("key", "value"),
                        TestsHelpers.TimestampWithSeconds(9),
                        null,
                        null,
                        null),
                    StreamCoordinates.Empty,
                    windowSize,
                    windowLag)
                .Should()
                .BeFalse();

            windows.AddEvent(
                    new MetricEvent(
                        100,
                        new MetricTags(1).Append("key", "value"),
                        TestsHelpers.TimestampWithSeconds(10),
                        null,
                        null,
                        null),
                    StreamCoordinates.Empty,
                    windowSize,
                    windowLag)
                .Should()
                .BeTrue();

            windows.Aggregate(aggregate, TestsHelpers.TimestampWithSeconds(60)).AggregatedEvents.Single().Value.Should().Be(146);
        }

        [Test]
        public void AddEvent_should_use_period_and_lag_from_aggregation_parameters()
        {
            var windows = new Windows();

            var aggregate = new TestsHelpers.SumValues();

            windows.AddEvent(
                    new MetricEvent(
                        1,
                        new MetricTags(1).Append("key", "value"),
                        TestsHelpers.TimestampWithSeconds(0),
                        null,
                        null,
                        null),
                    StreamCoordinates.Empty,
                    1.Seconds(),
                    2.Seconds())
                .Should()
                .BeTrue();

            windows.AddEvent(
                    new MetricEvent(
                        2,
                        new MetricTags(1).Append("key", "value"),
                        TestsHelpers.TimestampWithSeconds(1),
                        null,
                        null,
                        null),
                    StreamCoordinates.Empty,
                    5.Seconds(),
                    4.Seconds())
                .Should()
                .BeTrue();

            windows.Aggregate(aggregate, TestsHelpers.TimestampWithSeconds(2)).AggregatedEvents.Should().BeEmpty();
            windows.Aggregate(aggregate, TestsHelpers.TimestampWithSeconds(3)).AggregatedEvents.Single().Value.Should().Be(1);
            windows.Aggregate(aggregate, TestsHelpers.TimestampWithSeconds(8)).AggregatedEvents.Should().BeEmpty();
            windows.Aggregate(aggregate, TestsHelpers.TimestampWithSeconds(9)).AggregatedEvents.Single().Value.Should().Be(2);
        }

        [Test]
        public void Aggregate_should_close_windows_after_lag_elapsed_using_max_observed_timestamp()
        {
            var windows = FilledWindows();

            var aggregate = new TestsHelpers.SumValues();

            windows.Aggregate(aggregate, TestsHelpers.TimestampWithSeconds(-100)).AggregatedEvents.Single().Value.Should().Be(45);
        }

        [Test]
        public void Aggregate_should_close_windows_after_lag_elapsed_using_now_timestamp()
        {
            var windows = FilledWindows();

            var aggregate = new TestsHelpers.SumValues();

            windows.Aggregate(aggregate, TestsHelpers.TimestampWithSeconds(-100)).AggregatedEvents.Single().Value.Should().Be(45);
            windows.Aggregate(aggregate, TestsHelpers.TimestampWithSeconds(22)).AggregatedEvents.Should().BeEmpty();
            windows.Aggregate(aggregate, TestsHelpers.TimestampWithSeconds(23)).AggregatedEvents.Single().Value.Should().Be(46);
        }

        [Test]
        public void Aggregate_should_calculate_statistic()
        {
            var windows = FilledWindows();

            var aggregate = new TestsHelpers.SumValues();

            var result = windows.Aggregate(aggregate, TestsHelpers.TimestampWithSeconds(-100));
            result.AggregatedEvents.Single().Value.Should().Be(45);
            result.ActiveEventsCount.Should().Be(4);
            result.ActiveWindowsCount.Should().Be(1);
            result.FirstActiveEventCoordinates.Should().BeEquivalentTo(new StreamCoordinates(new[] {new StreamPosition
            {
                Offset = 10,
                Partition = 42
            }}));
        }

        [Test]
        public void Aggregate_should_close_windows_only_once()
        {
            var windows = FilledWindows();

            var aggregate = new TestsHelpers.SumValues();

            windows.Aggregate(aggregate, TestsHelpers.TimestampWithSeconds(60)).AggregatedEvents.Count.Should().Be(2);
            windows.Aggregate(aggregate, TestsHelpers.TimestampWithSeconds(60)).AggregatedEvents.Count.Should().Be(0);
        }

        private Windows FilledWindows()
        {
            var windows = new Windows();

            for (var seconds = 0; seconds <= 13; seconds++)
            {
                windows.AddEvent(
                        new MetricEvent(
                            seconds,
                            new MetricTags(1).Append("key", "value"),
                            TestsHelpers.TimestampWithSeconds(seconds),
                            null,
                            null,
                            null),
                        new StreamCoordinates(new[] {new StreamPosition
                        {
                            Offset = seconds,
                            Partition = 42
                        }}), 
                        windowSize,
                        windowLag)
                    .Should()
                    .BeTrue();
            }

            return windows;
        }
    }
}