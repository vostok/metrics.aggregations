using System;
using Vostok.Hercules.Consumers;
using Vostok.Metrics.Aggregations.AggregateFunctions;
using Vostok.Metrics.Hercules;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations
{
    internal class AggregatorWindow : WindowedStreamConsumerSettings<MetricEvent, MetricTags>.IWindow
    {
        private readonly IAggregateFunction aggregateFunction;
        private readonly StreamBinaryEventsWriter writer;

        public AggregatorWindow(IAggregateFunction aggregateFunction, StreamBinaryEventsWriter writer)
        {
            this.aggregateFunction = aggregateFunction;
            this.writer = writer;
        }

        public void Add(MetricEvent @event) =>
            aggregateFunction.Add(@event);

        public void Flush(DateTimeOffset timestamp)
        {
            foreach (var @event in aggregateFunction.Aggregate(timestamp))
                writer.Put(b => HerculesEventMetricBuilder.Build(@event, b));
        }
    }
}