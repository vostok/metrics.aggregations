using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using FluentAssertions;
using FluentAssertions.Extensions;
using NUnit.Framework;
using Vostok.Metrics.Aggregations.AggregateFunctions;
using Vostok.Metrics.Models;
using Vostok.Metrics.Primitives.Timer;

namespace Vostok.Metrics.Aggregations.Tests.AggregateFunctions
{
    [TestFixture]
    internal class HistogramsAggregateFunction_Tests
    {
        [Test]
        public void Aggregate_should_calculate_quantiles()
        {
            var function = new HistogramsAggregateFunction();

            var timestamp = DateTimeOffset.Now;

            var tags = MetricTags.Empty
                .Append("team", "vostok")
                .Append("project", "metrics-aggregators")
                .Append(WellKnownTagKeys.Name, "magic");

            for (var i = 0; i < 10; i++)
            {
                var lower = i % 3 == 0 ? double.NegativeInfinity : i % 3;
                var upper = i % 3 + 1 == 3 ? double.PositiveInfinity : i % 3 + 1;

                var aggregationParameters = new Dictionary<string, string>
                {
                    {"a", "aa"},
                    {"b", "bb"},
                    {"_lowerBound", lower.ToString(CultureInfo.InvariantCulture)},
                    {"_upperBound", upper.ToString(CultureInfo.InvariantCulture)}
                };

                aggregationParameters.SetQuantiles(new[] {0.1, 0.5, 0.75});

                function.AddEvent(new MetricEvent(i, tags, timestamp, "unicorns", WellKnownAggregationTypes.Counter, aggregationParameters));
            }

            //[-Inf..1]=18, [1..2]=12, [2..Inf]=15

            var aggregated = function.Aggregate(timestamp + 1.Minutes()).ToList();

            aggregated
                .Should()
                .BeEquivalentTo(
                    new MetricEvent(1, tags.Append(WellKnownTagKeys.Aggregate, "p10"), timestamp + 1.Minutes(), "unicorns", null, null),
                    new MetricEvent(1.375, tags.Append(WellKnownTagKeys.Aggregate, "p50"), timestamp + 1.Minutes(), "unicorns", null, null),
                    new MetricEvent(2, tags.Append(WellKnownTagKeys.Aggregate, "p75"), timestamp + 1.Minutes(), "unicorns", null, null),
                    new MetricEvent(45, tags.Append(WellKnownTagKeys.Aggregate, WellKnownTagValues.AggregateCount), timestamp + 1.Minutes(), null, null, null)
                );
        }

        [TestCase(0, 10)]
        [TestCase(5.0 / 18, 15)]
        [TestCase(8.0 / 18, 24)]
        [TestCase(12.0 / 18, 40)]
        [TestCase(13.0 / 18, 40)]
        public void GetQuantile_should_works_correctly(double quantile, double? expected)
        {
            var buckets = new List<KeyValuePair<HistogramBucket, double>>
            {
                new KeyValuePair<HistogramBucket, double>(new HistogramBucket(double.NegativeInfinity, 10), 3),
                new KeyValuePair<HistogramBucket, double>(new HistogramBucket(10, 20), 4),
                new KeyValuePair<HistogramBucket, double>(new HistogramBucket(20, 40), 5),
                new KeyValuePair<HistogramBucket, double>(new HistogramBucket(40, double.PositiveInfinity), 6)
            };

            HistogramsAggregateFunction.GetQuantile(buckets, quantile).Should().Be(expected);
        }
    }
}