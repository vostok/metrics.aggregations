using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.AggregateFunctions
{
    [PublicAPI]
    public class CountersAggregateFunction : IAggregateFunction
    {
        private MetricEvent lastEvent;
        private double count;

        public void Add(MetricEvent @event)
        {
            lastEvent = @event;
            count += @event.Value;
        }

        public IEnumerable<MetricEvent> Aggregate(DateTimeOffset timestamp)
        {
            if (lastEvent == null)
                return Enumerable.Empty<MetricEvent>();

            var result = new MetricEvent(
                count,
                lastEvent.Tags,
                timestamp,
                lastEvent.Unit,
                null,
                null);

            return new[] {result};
        }
    }
}