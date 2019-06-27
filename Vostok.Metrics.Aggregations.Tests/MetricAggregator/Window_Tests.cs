using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FluentAssertions;
using FluentAssertions.Extensions;
using NUnit.Framework;
using Vostok.Hercules.Client.Abstractions.Models;
using Vostok.Metrics.Aggregations.Helpers;
using Vostok.Metrics.Aggregations.MetricAggregator;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.Tests.MetricAggregator
{
    [TestFixture]
    internal class Window_Tests
    {
        private readonly TimeSpan period = 10.Seconds();
        private readonly TimeSpan lag = 3.Seconds();

        [Test]
        public void Create_should_build_good_interval()
        {
            for (var seconds = 0; seconds < 120; seconds++)
            {
                var timestamp = TestsHelpers.TimestampWithSeconds(seconds);

                var window = Window.Create(new TestsHelpers.ReturnEvents(), StreamCoordinates.Empty, timestamp, period, lag);

                timestamp.InInterval(window.Start, window.End).Should().BeTrue();
                window.Lag.Should().Be(lag);
                window.Period.Should().Be(period);
            }
        }

        [Test]
        public void AddEvent_should_check_in_interval_and_AggregateEvents_after()
        {
            var window = new Window(
                new TestsHelpers.ReturnEvents(),
                StreamCoordinates.Empty,
                TestsHelpers.TimestampWithSeconds(40),
                TestsHelpers.TimestampWithSeconds(50),
                period,
                lag);

            window.AddEvent(
                    new MetricEvent(
                        0,
                        new MetricTags(1).Append("key", "value"),
                        TestsHelpers.TimestampWithSeconds(39),
                        null,
                        null,
                        null))
                .Should()
                .BeFalse();

            window.AddEvent(
                    new MetricEvent(
                        1,
                        new MetricTags(1).Append("key", "value"),
                        TestsHelpers.TimestampWithSeconds(42),
                        null,
                        null,
                        null))
                .Should()
                .BeTrue();

            window.AddEvent(
                    new MetricEvent(
                        2,
                        new MetricTags(1).Append("key", "value"),
                        TestsHelpers.TimestampWithSeconds(43),
                        null,
                        null,
                        null))
                .Should()
                .BeTrue();

            window.AddEvent(
                    new MetricEvent(
                        3,
                        new MetricTags(1).Append("key", "value"),
                        TestsHelpers.TimestampWithSeconds(50),
                        null,
                        null,
                        null))
                .Should()
                .BeFalse();

            window.EventsCount.Should().Be(2);

            var aggregated = window.AggregateEvents();
            aggregated.Select(e => e.Value).Should().BeEquivalentTo(new List<double> {1, 2});
        }

        [Test]
        public void AddEvent_AggregateEvents_should_work_correctly()
        {
            var window = new Window(
                new TestsHelpers.ReturnEvents(),
                StreamCoordinates.Empty,
                TestsHelpers.TimestampWithSeconds(40),
                TestsHelpers.TimestampWithSeconds(50),
                period,
                lag);

            window.AddEvent(
                    new MetricEvent(
                        0,
                        new MetricTags(1).Append("key", "value"),
                        TestsHelpers.TimestampWithSeconds(39),
                        null,
                        null,
                        null))
                .Should()
                .BeFalse();

            window.AddEvent(
                    new MetricEvent(
                        1,
                        new MetricTags(1).Append("key", "value"),
                        TestsHelpers.TimestampWithSeconds(42),
                        null,
                        null,
                        null))
                .Should()
                .BeTrue();

            window.AddEvent(
                    new MetricEvent(
                        2,
                        new MetricTags(1).Append("key", "value"),
                        TestsHelpers.TimestampWithSeconds(50),
                        null,
                        null,
                        null))
                .Should()
                .BeFalse();

            window.AggregateEvents().Single().Value.Should().Be(1);
        }

        [Test]
        public void ShouldBeClosedBefore_should_be_true_if_lag_elapsed()
        {
            var window = new Window(
                new TestsHelpers.ReturnEvents(),
                StreamCoordinates.Empty,
                TestsHelpers.TimestampWithSeconds(40),
                TestsHelpers.TimestampWithSeconds(50),
                period,
                lag);

            window.ShouldBeClosedBefore(TestsHelpers.TimestampWithSeconds(52)).Should().BeFalse();
            window.ShouldBeClosedBefore(TestsHelpers.TimestampWithSeconds(53)).Should().BeTrue();
            window.ShouldBeClosedBefore(TestsHelpers.TimestampWithSeconds(54)).Should().BeTrue();
        }

        [Test]
        public void TooLongExists_should_be_true_if_time_elapsed()
        {
            var window = new Window(
                new TestsHelpers.ReturnEvents(),
                StreamCoordinates.Empty,
                TestsHelpers.TimestampWithSeconds(40),
                TestsHelpers.TimestampWithSeconds(50),
                0.1.Seconds(),
                0.1.Seconds());

            window.ExistsForTooLong().Should().BeFalse();
            Thread.Sleep(0.1.Seconds());
            window.ExistsForTooLong().Should().BeFalse();
            Thread.Sleep(0.1.Seconds());
            window.ExistsForTooLong().Should().BeTrue();
        }

        [Test]
        public void TooLongExists_should_be_false_if_time_elapsed_on_restart_phase()
        {
            var window = new Window(
                new TestsHelpers.ReturnEvents(),
                StreamCoordinates.Empty,
                TestsHelpers.TimestampWithSeconds(40),
                TestsHelpers.TimestampWithSeconds(50),
                0.1.Seconds(),
                0.1.Seconds());

            window.ExistsForTooLong(true).Should().BeFalse();
            Thread.Sleep(0.1.Seconds());
            window.ExistsForTooLong(true).Should().BeFalse();
            Thread.Sleep(0.1.Seconds());
            window.ExistsForTooLong(true).Should().BeFalse();
        }
    }
}