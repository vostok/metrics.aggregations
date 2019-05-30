using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Vostok.Logging.Abstractions;
using Vostok.Metrics.Aggregations.AggregateFunctions;
using Vostok.Metrics.Aggregations.Helpers;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.MetricAggregator
{
    internal class OneMetricAggregator
    {
        private readonly MetricTags tags;
        private readonly AggregatorSettings settings;
        private readonly ILog log;
        private readonly Windows windows;
        private readonly IAggregateFunction aggregateFunction;

        public OneMetricAggregator(MetricTags tags, AggregatorSettings settings, ILog log)
        {
            this.tags = tags;
            this.settings = settings;
            this.log = log;
            windows = new Windows();
            aggregateFunction = settings.AggregateFunctionFactory(tags);
        }

        public bool AddEvent([NotNull] MetricEvent @event)
        {
            try
            {
                if (!@event.Timestamp.InInterval(DateTimeOffset.Now - settings.MaximumEventBeforeNow, DateTimeOffset.Now + settings.MaximumEventAfterNow))
                    return false;

                return windows.AddEvent(@event, settings.DefaultPeriod, settings.DefaultLag);
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to add event {Event}.", @event);
                return false;
            }
        }

        [NotNull]
        [ItemNotNull]
        public IEnumerable<MetricEvent> GetAggregatedMetrics()
        {
            try
            {
                return windows.TryCloseWindows(aggregateFunction, DateTimeOffset.Now);
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to aggregate {Metric} metric.", tags);
                return new List<MetricEvent>();
            }
        }
    }
}