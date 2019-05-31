using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Vostok.Hercules.Client.Abstractions.Models;
using Vostok.Metrics.Aggregations.AggregateFunctions;
using Vostok.Metrics.Models;
using Vostok.Metrics.Primitives.Timer;

namespace Vostok.Metrics.Aggregations.MetricAggregator
{
    internal class Windows
    {
        private readonly List<Window> windows = new List<Window>();
        private DateTimeOffset minimumAllowedTimestamp = DateTimeOffset.MinValue;
        private DateTimeOffset maximumObservedTimestamp = DateTimeOffset.MinValue;

        public bool AddEvent([NotNull] MetricEvent @event, [NotNull] StreamCoordinates coordinates, TimeSpan defaultPeriod, TimeSpan defaultLag)
        {
            if (@event.Timestamp < minimumAllowedTimestamp)
                return false;

            if (maximumObservedTimestamp < @event.Timestamp)
                maximumObservedTimestamp = @event.Timestamp;

            foreach (var window in windows)
            {
                if (window.AddEvent(@event))
                    return true;
            }

            var newWindow = Window.Create(
                coordinates,
                @event.Timestamp,
                @event.AggregationParameters.GetAggregatePeriod() ?? defaultPeriod,
                @event.AggregationParameters.GetAggregateLag() ?? defaultLag);

            newWindow.AddEvent(@event);
            windows.Add(newWindow);

            return true;
        }

        [NotNull]
        public AggregateResult Aggregate([NotNull] IAggregateFunction aggregateFunction, DateTimeOffset now)
        {
            if (maximumObservedTimestamp < now)
                maximumObservedTimestamp = now;

            var result = new AggregateResult();

            for (var i = 0; i < windows.Count; i++)
            {
                var window = windows[i];

                if (window.ShouldBeClosedBefore(maximumObservedTimestamp) || window.TooLongExists())
                {
                    windows.RemoveAt(i--);

                    result.AggregatedEvents.AddRange(window.AggregateEvents(aggregateFunction));
                   
                    if (minimumAllowedTimestamp < window.End)
                        minimumAllowedTimestamp = window.End;
                }
                else
                {
                    result.AddActiveWindow(window);
                }
            }

            return result;
        }
    }
}