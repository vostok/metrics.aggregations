using System;
using JetBrains.Annotations;
using Vostok.Hercules.Client.Abstractions.Models;
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
        public DateTimeOffset LastEventAdded;

        public OneMetricAggregator(MetricTags tags, AggregatorSettings settings, ILog log)
        {
            this.tags = tags;
            this.settings = settings;
            this.log = log;
            windows = new Windows();
            aggregateFunction = settings.AggregateFunctionFactory(tags);
            LastEventAdded = DateTimeOffset.Now;
        }

        public bool AddEvent([NotNull] MetricEvent @event, [NotNull] StreamCoordinates coordinates)
        {
            try
            {
                if (!@event.Timestamp.InInterval(DateTimeOffset.Now - settings.MaximumEventBeforeNow, DateTimeOffset.Now + settings.MaximumEventAfterNow))
                    return false;

                if (windows.AddEvent(@event, coordinates, settings.DefaultPeriod, settings.DefaultLag))
                {
                    LastEventAdded = DateTimeOffset.Now;
                    return true;
                }

                return false;
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to add event {Event}.", @event);
                return false;
            }
        }

        [NotNull]
        public AggregateResult Aggregate()
        {
            try
            {
                return windows.Aggregate(aggregateFunction);
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to aggregate {Metric} metric.", tags);
                return new AggregateResult();
            }
        }
    }
}