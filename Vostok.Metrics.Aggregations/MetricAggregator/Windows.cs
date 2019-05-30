using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Vostok.Metrics.Aggregations.AggregateFunctions;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.MetricAggregator
{
    internal class Windows
    {
        private readonly List<Window> windows = new List<Window>();
        private DateTimeOffset minimumAllowedTimestamp = DateTimeOffset.MinValue;
        private DateTimeOffset maximumObservedTimestamp = DateTimeOffset.MinValue;

        public bool AddEvent([NotNull] MetricEvent @event, TimeSpan defaultPeriod, TimeSpan defaultLag)
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

            // TODO(kungurtsev): try read period & lag from event
            var newWindow = Window.CreateForTimestamp(@event.Timestamp, defaultPeriod, defaultLag);
            newWindow.AddEvent(@event);
            windows.Add(newWindow);

            return true;
        }

        [NotNull]
        [ItemNotNull]
        public IEnumerable<MetricEvent> TryCloseWindows([NotNull] IAggregateFunction aggregateFunction)
        {
            var result = new List<MetricEvent>();

            for (var i = windows.Count - 1; i >= 0; i--)
            {
                var window = windows[i];

                if (window.ShouldBeClosedBefore(maximumObservedTimestamp))
                {
                    windows.RemoveAt(i);

                    result.AddRange(window.AggregateEvents(aggregateFunction));
                   
                    if (minimumAllowedTimestamp < window.End)
                        minimumAllowedTimestamp = window.End;
                }
            }

            return result;
        }
    }
}