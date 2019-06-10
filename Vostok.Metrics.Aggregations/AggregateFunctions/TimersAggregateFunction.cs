using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Vostok.Metrics.Models;
using Vostok.Metrics.Primitives.Timer;

namespace Vostok.Metrics.Aggregations.AggregateFunctions
{
    [PublicAPI]
    public class TimersAggregateFunction : IAggregateFunction
    {
        private MetricEvent lastEvent;
        private List<double> values = new List<double>();

        public void AddEvent(MetricEvent @event)
        {
            lastEvent = @event;
            values.Add(@event.Value);
        }

        public IEnumerable<MetricEvent> Aggregate(DateTimeOffset timestamp)
        {
            if (lastEvent == null)
                return new List<MetricEvent>();

            var quantileMetricsBuilder = new QuantileMetricsBuilder(
                lastEvent.AggregationParameters.GetQuantiles(), 
                lastEvent.Tags, 
                lastEvent.Unit);

            return quantileMetricsBuilder.Build(values.ToArray(), timestamp);
        }
    }
}