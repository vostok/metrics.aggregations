using System;
using System.Collections.Generic;
using FluentAssertions;
using FluentAssertions.Extensions;
using NUnit.Framework;
using Vostok.Metrics.Aggregations.AggregateFunctions;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.Tests.AggregateFunctions
{
    [TestFixture]
    internal class CounterAggregateFunction_Tests
    {
        [Test]
        public void Aggregate_should_summarize_count()
        {
            var function = new CountersAggregateFunction();

            var timestamp = DateTimeOffset.Now;

            var aggregationParameters = new Dictionary<string, string>
            {
                {"a", "aa"},
                {"b", "bb"}
            };

            var tags = MetricTags.Empty
                .Append("team", "vostok")
                .Append("project", "metrics-aggregators")
                .Append(WellKnownTagKeys.Name, "magic");

            function.Add(new MetricEvent(3, tags, timestamp, "unicorns", WellKnownAggregationTypes.Counter, aggregationParameters));
            function.Add(new MetricEvent(100, tags, timestamp, "unicorns", WellKnownAggregationTypes.Counter, aggregationParameters));

            function.Aggregate(timestamp + 1.Minutes())
                .Should()
                .BeEquivalentTo(
                    new MetricEvent(103, tags, timestamp + 1.Minutes(), "unicorns", null, null));
        }
    }
}