using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.AggregateFunctions
{
    [PublicAPI]
    public class HistogramsAggregateFunction : IAggregateFunction
    {
        private MetricEvent lastEvent;

        public void AddEvent(MetricEvent @event)
        {
            lastEvent = @event;
            // TODO
        }

        public IEnumerable<MetricEvent> Aggregate(DateTimeOffset timestamp)
        {
            if (lastEvent == null)
                return Enumerable.Empty<MetricEvent>();

            // TODO
            return Enumerable.Empty<MetricEvent>();
        }
    }
}