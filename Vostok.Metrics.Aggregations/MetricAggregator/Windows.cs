using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Vostok.Hercules.Client.Abstractions.Models;
using Vostok.Metrics.Aggregations.AggregateFunctions;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.MetricAggregator
{
    internal class Windows
    {
        private readonly Func<IAggregateFunction> aggregateFunctionFactory;
        private readonly TimeSpan defaultPeriod;
        private readonly TimeSpan defaultLag;
        private readonly List<Window> windows = new List<Window>();
        private DateTimeOffset minimumAllowedTimestamp = DateTimeOffset.MinValue;
        private DateTimeOffset maximumObservedTimestamp = DateTimeOffset.MinValue;

        public Windows(Func<IAggregateFunction> aggregateFunctionFactory, TimeSpan defaultPeriod, TimeSpan defaultLag)
        {
            this.aggregateFunctionFactory = aggregateFunctionFactory;
            this.defaultPeriod = defaultPeriod;
            this.defaultLag = defaultLag;
        }

        public bool AddEvent([NotNull] MetricEvent @event, [NotNull] StreamCoordinates coordinates)
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
                aggregateFunctionFactory(),
                coordinates,
                @event.Timestamp,
                @event.AggregationParameters.GetAggregationPeriod() ?? defaultPeriod,
                @event.AggregationParameters.GetAggregationLag() ?? defaultLag);

            newWindow.AddEvent(@event);
            windows.Add(newWindow);

            return true;
        }

        [NotNull]
        public AggregateResult Aggregate(bool restartPhase = false)
        {
            var result = new AggregateResult();

            for (var i = 0; i < windows.Count; i++)
            {
                var window = windows[i];

                if (window.ShouldBeClosedBefore(maximumObservedTimestamp) || window.ExistsForTooLong(restartPhase))
                {
                    windows.RemoveAt(i--);

                    result.AggregatedEvents.AddRange(window.AggregateEvents());

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