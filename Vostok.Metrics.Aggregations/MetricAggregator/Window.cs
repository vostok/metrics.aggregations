using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Vostok.Hercules.Client.Abstractions.Models;
using Vostok.Metrics.Aggregations.AggregateFunctions;
using Vostok.Metrics.Aggregations.Helpers;
using Vostok.Metrics.Models;
using Vostok.Metrics.Primitives.Timer;

namespace Vostok.Metrics.Aggregations.MetricAggregator
{
    internal class Window
    {
        public readonly StreamCoordinates FirstEventCoordinates;
        public readonly DateTimeOffset Start;
        public readonly DateTimeOffset End;
        public readonly TimeSpan Period;
        public readonly TimeSpan Lag;
        private readonly DateTimeOffset createdAt;
        private readonly List<MetricEvent> events = new List<MetricEvent>();

        private Window(StreamCoordinates firstEventCoordinates, DateTimeOffset start, DateTimeOffset end, TimeSpan period, TimeSpan lag)
        {
            FirstEventCoordinates = firstEventCoordinates;
            Start = start;
            End = end;
            Period = period;
            Lag = lag;
            createdAt = DateTimeOffset.Now;
        }

        [NotNull]
        public static Window Create(StreamCoordinates firstEventCoordinates, DateTimeOffset timestamp, TimeSpan period, TimeSpan lag)
        {
            var start = timestamp.AddTicks(-timestamp.Ticks % period.Ticks);
            var result = new Window(firstEventCoordinates, start, start + period, period, lag);
            return result;
        }

        public int EventsCount => events.Count;

        public bool AddEvent([NotNull] MetricEvent @event)
        {
            if (!@event.Timestamp.InInterval(Start, End))
                return false;

            events.Add(@event);

            return true;
        }

        public bool ShouldBeClosedBefore(DateTimeOffset timestamp)
        {
            return End + Lag <= timestamp;
        }

        public bool TooLongExists()
        {
            return DateTimeOffset.Now - createdAt > Period + Lag;
        }

        [NotNull]
        [ItemNotNull]
        public IEnumerable<MetricEvent> AggregateEvents([NotNull] IAggregateFunction aggregateFunction)
        {
            var firstEvent = events.FirstOrDefault();
            if (firstEvent != null)
            {
                aggregateFunction.SetUnit(firstEvent.Unit);
                aggregateFunction.SetQuantiles(firstEvent.AggregationParameters.GetQuantiles());
            }

            return aggregateFunction.Aggregate(events, End);
        }
    }
}