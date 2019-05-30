﻿using System;
using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using FluentAssertions.Extensions;
using NUnit.Framework;
using Vostok.Metrics.Aggregations.Helpers;
using Vostok.Metrics.Aggregations.MetricAggregator;
using Vostok.Metrics.Models;
using Vostok.Metrics.Primitives.Timer;

namespace Vostok.Metrics.Aggregations.Tests.MetricAggregator
{
    [TestFixture]
    internal class Window_Tests
    {
        private readonly TimeSpan windowSize = 10.Seconds();
        private readonly TimeSpan windowLag = 3.Seconds();

        [Test]
        public void CreateForTimestamp_should_build_good_interval()
        {
            for (var seconds = 0; seconds < 120; seconds++)
            {
                var timestamp = TestsHelpers.TimestampWithSeconds(seconds);

                var window = Window.CreateForTimestamp(timestamp, windowSize, windowLag);

                timestamp.InInterval(window.Start, window.End).Should().BeTrue();
                window.Lag.Should().Be(windowLag);
            }
        }

        [Test]
        public void AddEvent_should_check_in_interval()
        {
            // [40, 50)
            var window = Window.CreateForTimestamp(TestsHelpers.TimestampWithSeconds(42), windowSize, windowLag);

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
        public void ShouldBeClosedBefore_should_return_if_lag_elapsed()
        {
            // [40, 50)
            var window = Window.CreateForTimestamp(TestsHelpers.TimestampWithSeconds(42), windowSize, windowLag);

            window.ShouldBeClosedBefore(TestsHelpers.TimestampWithSeconds(52)).Should().BeFalse();
            window.ShouldBeClosedBefore(TestsHelpers.TimestampWithSeconds(53)).Should().BeTrue();
            window.ShouldBeClosedBefore(TestsHelpers.TimestampWithSeconds(54)).Should().BeTrue();
        }

        [Test]
        public void AggregateEvents_should_pass_unit_and_quantiles_from_first_event()
        {
            // [40, 50)
            var window = Window.CreateForTimestamp(TestsHelpers.TimestampWithSeconds(42), windowSize, windowLag);

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
                        null))
                .Should()
                .BeTrue();

            var aggregate = new TestsHelpers.ReturnEvents();
            window.AggregateEvents(aggregate).Count().Should().Be(2);
            aggregate.Unit.Should().Be("unit1");
            aggregate.Quantiles.Should().BeEquivalentTo(new[] {0.33});
        }

        [Test]
        public void AggregateEvents_should_pass_null_unit_and_quantiles()
        {
            // [40, 50)
            var window = Window.CreateForTimestamp(TestsHelpers.TimestampWithSeconds(42), windowSize, windowLag);

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