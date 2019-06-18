using System;
using JetBrains.Annotations;
using Vostok.Hercules.Client.Abstractions.Models;
using Vostok.Logging.Abstractions;
using Vostok.Metrics.Aggregations.Helpers;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.MetricAggregator
{
    internal class OneMetricAggregator
    {
        public DateTimeOffset LastEventAdded;
        private readonly MetricTags tags;
        private readonly AggregatorSettings settings;
        private readonly ILog log;
        private readonly Windows windows;

        public OneMetricAggregator(MetricTags tags, AggregatorSettings settings, ILog log)
        {
            this.tags = tags;
            this.settings = settings;
            this.log = log;
            windows = new Windows(settings.AggregateFunctionFactory, settings.DefaultPeriod, settings.DefaultLag);

            LastEventAdded = DateTimeOffset.UtcNow;
        }

        public bool AddEvent([NotNull] MetricEvent @event, [NotNull] StreamCoordinates coordinates)
        {
            try
            {
                // CR(iloktionov): Call Now only once.
                if (!@event.Timestamp.InInterval(DateTimeOffset.Now - settings.MaximumEventBeforeNow, DateTimeOffset.Now + settings.MaximumEventAfterNow))
                    return false;

                if (windows.AddEvent(@event, coordinates))
                {
                    LastEventAdded = DateTimeOffset.UtcNow;
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
        public AggregateResult Aggregate(bool restartPhase = false)
        {
            try
            {
                return windows.Aggregate(restartPhase);
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to aggregate {Metric} metric.", tags);
                return new AggregateResult();
            }
        }
    }
}