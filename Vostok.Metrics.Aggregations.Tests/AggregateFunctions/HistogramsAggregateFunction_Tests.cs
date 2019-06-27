using System;
using System.Collections.Generic;
using NUnit.Framework;
using Vostok.Metrics.Aggregations.AggregateFunctions;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.Tests.AggregateFunctions
{
    [TestFixture]
    internal class HistogramsAggregateFunction_Tests
    {
        [Test]
        public void Aggregate_should_summarize_count_in_buckets()
        {
            var function = new HistogramsAggregateFunction();

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

            for (var i = 0; i < 10; i++)
                function.AddEvent(new MetricEvent(i, tags, timestamp, "unicorns", WellKnownAggregationTypes.Counter, aggregationParameters));

            //TODO
            //function.Aggregate(timestamp + 1.Minutes())
            //    .Should()
            //    .BeEquivalentTo(
            //        new MetricEvent(103, tags, timestamp + 1.Minutes(), "unicorns", null, null));
        }
    }
}