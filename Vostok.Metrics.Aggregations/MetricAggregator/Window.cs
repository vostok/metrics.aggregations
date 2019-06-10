using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Vostok.Commons.Time;
using Vostok.Hercules.Client.Abstractions.Models;
using Vostok.Metrics.Aggregations.AggregateFunctions;
using Vostok.Metrics.Aggregations.Helpers;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.MetricAggregator
{
    internal class Window
    {
        private static readonly TimeSpan MaximumAllowedPeriod = 1.Minutes();
        private static readonly TimeSpan MaximumAllowedLag = 1.Minutes();

        private readonly IAggregateFunction aggregateFunction;
        public readonly StreamCoordinates FirstEventCoordinates;
        public readonly DateTimeOffset Start;
        public readonly DateTimeOffset End;
        public readonly TimeSpan Period;
        public readonly TimeSpan Lag;

        private DateTimeOffset lastEventAdded;

        public int EventsCount { get; private set; }

        internal Window(IAggregateFunction aggregateFunction, StreamCoordinates firstEventCoordinates, DateTimeOffset start, DateTimeOffset end, TimeSpan period, TimeSpan lag)
        {
            this.aggregateFunction = aggregateFunction;
            FirstEventCoordinates = firstEventCoordinates;
            Start = start;
            End = end;
            Period = period;
            Lag = lag;
            lastEventAdded = DateTimeOffset.Now;
        }

        [NotNull]
        public static Window Create(IAggregateFunction aggregateFunction, StreamCoordinates firstEventCoordinates, DateTimeOffset timestamp, TimeSpan period, TimeSpan lag)
        {
            if (period > MaximumAllowedPeriod)
                period = MaximumAllowedPeriod;
            if (lag > MaximumAllowedLag)
                lag = MaximumAllowedLag;

            var start = timestamp.AddTicks(-timestamp.Ticks % period.Ticks);
            var result = new Window(aggregateFunction, firstEventCoordinates, start, start + period, period, lag);
            return result;
        }

        public bool AddEvent([NotNull] MetricEvent @event)
        {
            if (!@event.Timestamp.InInterval(Start, End))
                return false;

            lastEventAdded = DateTimeOffset.Now;
            EventsCount++;
            aggregateFunction.AddEvent(@event);

            return true;
        }

        public bool ShouldBeClosedBefore(DateTimeOffset timestamp)
        {
            return End + Lag <= timestamp;
        }

        public bool TooLongExists(bool restartPhase = false)
        {
            if (restartPhase)
            {
                lastEventAdded = DateTimeOffset.Now;
                return false;
            }

            return DateTimeOffset.Now - lastEventAdded > Period + Lag;
        }

        [NotNull]
        [ItemNotNull]
        public IEnumerable<MetricEvent> AggregateEvents()
        {
            return aggregateFunction.Aggregate(End);
        }
    }
}