using System;
using System.Collections.Generic;
using Vostok.Metrics.Aggregations.AggregateFunctions;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.MetricAggregator
{
    internal class Windows
    {
        private readonly List<Window> windows = new List<Window>();
        private DateTimeOffset minimumAllowedTimestamp = DateTimeOffset.MinValue;
        private DateTimeOffset maximumObservedTimestamp = DateTimeOffset.MinValue;

        public bool AddEvent(MetricEvent @event, TimeSpan defaultPeriod, TimeSpan defaultLag)
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

        public IEnumerable<MetricEvent> TryCloseWindows(IAggregateFunction aggregateFunction)
        {
            var result = new List<MetricEvent>();

            for (var i = windows.Count - 1; i >= 0; i--)
            {
                if (windows[i].ShouldBeClosedBefore(maximumObservedTimestamp))
                {
                    result.AddRange(windows[i].AggregateEvents(aggregateFunction));
                   
                    if (minimumAllowedTimestamp < windows[i].End)
                        minimumAllowedTimestamp = windows[i].End;

                    windows.RemoveAt(i);
                }
            }

            return result;
        }
    }
}