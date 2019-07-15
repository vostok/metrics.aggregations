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
    internal class TimersAggregateFunction_Tests
    {
        [Test]
        public void Aggregate_should_calculate_quantiles()
        {
            var function = new TimersAggregateFunction();

            var timestamp = DateTimeOffset.Now;

            var aggregationParameters = new Dictionary<string, string>
            {
                {"a", "aa"},
                {"b", "bb"}
            }.SetQuantiles(new[] {0.7});

            var tags = MetricTags.Empty
                .Append("team", "vostok")
                .Append("project", "metrics-aggregators")
                .Append(WellKnownTagKeys.Name, "magic");

            for (var value = 0; value < 10; value++)
                function.AddEvent(new MetricEvent(value, tags, timestamp, "unicorns", WellKnownAggregationTypes.Counter, aggregationParameters));

            function.Aggregate(timestamp + 1.Minutes())
                .Should()
                .BeEquivalentTo(
                    new MetricEvent(10, tags.Append(WellKnownTagKeys.Aggregate, "count"), timestamp + 1.Minutes(), null, null, null),
                    new MetricEvent(4.5, tags.Append(WellKnownTagKeys.Aggregate, "avg"), timestamp + 1.Minutes(), "unicorns", null, null),
                    new MetricEvent(7, tags.Append(WellKnownTagKeys.Aggregate, "p70"), timestamp + 1.Minutes(), "unicorns", null, null)
                );
        }
    }
}