using System;
using System.Collections.Generic;
using Vostok.Logging.Abstractions;
using Vostok.Metrics.Aggregations.AggregateFunctions;
using Vostok.Metrics.Aggregations.Helpers;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.MetricAggregator
{
    internal class OneMetricAggregator
    {
        private readonly AggregatorSettings settings;
        private readonly ILog log;
        private readonly Windows windows = new Windows();
        private readonly IAggregateFunction aggregateFunction;

        public OneMetricAggregator(MetricTags tags, AggregatorSettings settings, ILog log)
        {
            this.settings = settings;
            this.log = log;
            aggregateFunction = settings.AggregateFunctionFactory(tags);
        }

        public bool AddEvent(MetricEvent @event)
        {
            if (!@event.Timestamp.InInterval(DateTimeOffset.Now - settings.MaximumEventBeforeNow, DateTimeOffset.Now + settings.MaximumEventAfterNow))
                return false;

            return windows.AddEvent(@event, settings.DefaultPeriod, settings.DefaultLag);
        }

        public IEnumerable<MetricEvent> GetAggregatedMetrics()
        {
            return windows.TryCloseWindows(aggregateFunction);
        }
    }
}