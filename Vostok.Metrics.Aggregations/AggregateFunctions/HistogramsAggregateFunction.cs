using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Vostok.Metrics.Models;
using Vostok.Metrics.Primitives.Timer;

namespace Vostok.Metrics.Aggregations.AggregateFunctions
{
    [PublicAPI]
    public class HistogramsAggregateFunction : IAggregateFunction
    {
        private MetricEvent lastEvent;
        private Dictionary<HistogramBucket, double> buckets = new Dictionary<HistogramBucket, double>();

        public void AddEvent(MetricEvent @event)
        {
            lastEvent = @event;

            var bucket = @event.AggregationParameters.GetHistogramBucket();
            if (bucket == null)
                return;

            if (!buckets.ContainsKey(bucket.Value))
                buckets[bucket.Value] = 0;
            buckets[bucket.Value] += @event.Value;
        }

        public IEnumerable<MetricEvent> Aggregate(DateTimeOffset timestamp)
        {
            if (lastEvent == null || !buckets.Any())
                return Enumerable.Empty<MetricEvent>();

            var result = new List<MetricEvent>();
            var tags = lastEvent.Tags;

            var quantiles = lastEvent.AggregationParameters.GetQuantiles() ?? Quantiles.DefaultQuantiles;
            var sortedBuckets = buckets.OrderBy(b => b.Key.LowerBound).ToList();

            var quantileTags = Quantiles.QuantileTags(quantiles, tags);
            for (var i = 0; i < quantiles.Length; i++)
            {
                var value = GetQuantile(sortedBuckets, quantiles[i]);
                result.Add(new MetricEvent(value, quantileTags[i], timestamp, lastEvent.Unit, null, null));
            }

            var totalCount = sortedBuckets.Sum(x => x.Value);
            var countTags = tags.Append(WellKnownTagKeys.Aggregate, WellKnownTagValues.AggregateCount);
            result.Add(new MetricEvent(totalCount, countTags, timestamp, null, null, null));

            return result;
        }

        internal static double GetQuantile(List<KeyValuePair<HistogramBucket, double>> sortedBuckets, double quantile)
        {
            var totalCount = sortedBuckets.Sum(x => x.Value);
            var skip = quantile * totalCount;

            var i = 0;
            while (i + 1 < sortedBuckets.Count && sortedBuckets[i].Value < skip)
            {
                skip -= sortedBuckets[i].Value;
                i++;
            }

            var bucket = sortedBuckets[i].Key;
            var value = sortedBuckets[i].Value;

            if (double.IsPositiveInfinity(bucket.UpperBound))
                return bucket.LowerBound;

            if (double.IsNegativeInfinity(bucket.LowerBound))
                return bucket.UpperBound;

            var length = bucket.UpperBound - bucket.LowerBound;
            var result = bucket.LowerBound + skip / value * length;

            return result;
        }
    }
}