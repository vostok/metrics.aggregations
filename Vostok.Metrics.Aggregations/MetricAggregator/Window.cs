using System;
using System.Collections.Generic;
using System.Linq;
using Vostok.Metrics.Aggregations.AggregateFunctions;
using Vostok.Metrics.Aggregations.Helpers;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.MetricAggregator
{
    internal class Window
    {
        public readonly DateTimeOffset Start;
        public readonly DateTimeOffset End;
        public readonly TimeSpan Lag;
        private readonly List<MetricEvent> events = new List<MetricEvent>();

        private Window(DateTimeOffset start, DateTimeOffset end, TimeSpan lag)
        {
            Start = start;
            End = end;
            Lag = lag;
        }

        public bool AddEvent(MetricEvent @event)
        {
            if (!@event.Timestamp.InInterval(Start, End))
                return false;

            events.Add(@event);

            return true;
        }

        public static Window CreateWithEvent(MetricEvent @event, TimeSpan windowSize, TimeSpan lag)
        {
            var timestamp = @event.Timestamp;
            var start = timestamp.AddTicks(-timestamp.Ticks % windowSize.Ticks);
            var result = new Window(start, start + windowSize, lag);
            result.AddEvent(@event);
            return result;
        }

        public bool ShouldBeClosedBefore(DateTimeOffset timestamp)
        {
            return End + Lag <= timestamp;
        }

        public IEnumerable<MetricEvent> AggregateEvents(IAggregateFunction aggregateFunction)
        {
            var someEvent = events.FirstOrDefault();
            if (someEvent != null)
            {
                aggregateFunction.SetUnit(someEvent.Unit);
                // TODO(kungurtsev): parse quantiles
                //aggregateFunction.SetQuantiles(someEvent.AggregationParameters);
            }

            return aggregateFunction.Aggregate(events, End);
        }
    }
}