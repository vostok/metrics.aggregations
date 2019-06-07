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
using Vostok.Metrics.Primitives.Timer;

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

                var window = Window.Create(StreamCoordinates.Empty, timestamp, period, lag);

                timestamp.InInterval(window.Start, window.End).Should().BeTrue();
                window.Lag.Should().Be(lag);
                window.Period.Should().Be(period);
            }
        }

        [Test]
        public void AddEvent_should_check_in_interval()
        {
            // [40, 50)
            var window = Window.Create(StreamCoordinates.Empty, TestsHelpers.TimestampWithSeconds(42), period, lag);

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

            window.AggregateEvents(new TestsHelpers.ReturnEvents()).Single().Value.Should().Be(1);
        }

        [Test]
        public void ShouldBeClosedBefore_should_be_true_if_lag_elapsed()
        {
            // [40, 50)
            var window = Window.Create(StreamCoordinates.Empty, TestsHelpers.TimestampWithSeconds(42), period, lag);

            window.ShouldBeClosedBefore(TestsHelpers.TimestampWithSeconds(52)).Should().BeFalse();
            window.ShouldBeClosedBefore(TestsHelpers.TimestampWithSeconds(53)).Should().BeTrue();
            window.ShouldBeClosedBefore(TestsHelpers.TimestampWithSeconds(54)).Should().BeTrue();
        }

        [Test]
        public void TooLongExists_should_be_true_if_time_elapsed()
        {
            // [40, 50)
            var window = Window.Create(StreamCoordinates.Empty, TestsHelpers.TimestampWithSeconds(42), 0.1.Seconds(), 0.1.Seconds());

            window.TooLongExists().Should().BeFalse();
            Thread.Sleep(0.1.Seconds());
            window.TooLongExists().Should().BeFalse();
            Thread.Sleep(0.1.Seconds());
            window.TooLongExists().Should().BeTrue();
        }

        [Test]
        public void TooLongExists_should_be_false_if_time_elapsed_on_restart_phase()
        {
            // [40, 50)
            var window = Window.Create(StreamCoordinates.Empty, TestsHelpers.TimestampWithSeconds(42), 0.1.Seconds(), 0.1.Seconds());

            window.TooLongExists(true).Should().BeFalse();
            Thread.Sleep(0.1.Seconds());
            window.TooLongExists(true).Should().BeFalse();
            Thread.Sleep(0.1.Seconds());
            window.TooLongExists(true).Should().BeFalse();
        }

        [Test]
        public void AggregateEvents_should_pass_unit_and_quantiles_from_last_event()
        {
            // [40, 50)
            var window = Window.Create(StreamCoordinates.Empty, TestsHelpers.TimestampWithSeconds(42), period, lag);

            window.AddEvent(
                    new MetricEvent(
                        0,
                        new MetricTags(1).Append("key", "value"),
                        TestsHelpers.TimestampWithSeconds(40),
                        "unit1",
                        null,
                        new Dictionary<string, string>().SetQuantiles(new[] {0.33}))
                )
                .Should()
                .BeTrue();

            window.AddEvent(
                    new MetricEvent(
                        1,
                        new MetricTags(1).Append("key", "value"),
                        TestsHelpers.TimestampWithSeconds(41),
                        "unit2",
                        null,
                        new Dictionary<string, string>().SetQuantiles(new[] {0.34}))
                )
                .Should()
                .BeTrue();

            var aggregate = new TestsHelpers.ReturnEvents();
            window.AggregateEvents(aggregate).Count().Should().Be(2);
            aggregate.Unit.Should().Be("unit2");
            aggregate.Quantiles.Should().BeEquivalentTo(new[] {0.34});
        }

        [Test]
        public void AggregateEvents_should_pass_null_unit_and_quantiles()
        {
            // [40, 50)
            var window = Window.Create(StreamCoordinates.Empty, TestsHelpers.TimestampWithSeconds(42), period, lag);

            window.AddEvent(
                    new MetricEvent(
                        0,
                        new MetricTags(1).Append("key", "value"),
                        TestsHelpers.TimestampWithSeconds(40),
                        null,
                        null,
                        null)
                )
                .Should()
                .BeTrue();

            var aggregate = new TestsHelpers.ReturnEvents {Unit = "unit", Quantiles = new[] {0.33}};
            window.AggregateEvents(aggregate).Count().Should().Be(1);
            aggregate.Unit.Should().BeNull();
            aggregate.Quantiles.Should().BeNull();
        }
    }
}