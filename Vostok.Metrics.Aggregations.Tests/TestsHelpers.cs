using System;
using System.Collections.Generic;
using System.Linq;
using FluentAssertions.Extensions;
using Vostok.Metrics.Aggregations.AggregateFunctions;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.Tests
{
    internal static class TestsHelpers
    {
        public static DateTimeOffset TimestampWithSeconds(double seconds)
        {
            return new DateTimeOffset(2019, 01, 01, 00, 00, 00, TimeSpan.Zero) + seconds.Seconds();
        }

        public class ReturnEvents : IAggregateFunction
        {
            private List<MetricEvent> events = new List<MetricEvent>();

            public void Add(MetricEvent @event)
            {
                events.Add(@event);
            }

            public IEnumerable<MetricEvent> Aggregate(DateTimeOffset timestamp) =>
                events;
        }

        public class SumValues : IAggregateFunction
        {
            private List<MetricEvent> events = new List<MetricEvent>();

            public void Add(MetricEvent @event)
            {
                events.Add(@event);
            }

            public IEnumerable<MetricEvent> Aggregate(DateTimeOffset timestamp) =>
                new List<MetricEvent>
                {
                    new MetricEvent(
                        events.Sum(e => e.Value),
                        events.First().Tags,
                        timestamp,
                        events.First().Unit,
                        null,
                        null)
                };
        }

        public class TestMetricSender : IMetricEventSender
        {
            private List<MetricEvent> sent = new List<MetricEvent>();

            public void Send(MetricEvent @event)
            {
                lock (sent)
                {
                    sent.Add(@event);
                }
            }

            public List<MetricEvent> Events()
            {
                lock (sent)
                {
                    var result = sent.ToList();
                    sent.Clear();
                    return result;
                }
            }
        }
    }
}