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
            public string Unit;
            public double[] Quantiles;

            public IEnumerable<MetricEvent> Aggregate(IEnumerable<MetricEvent> events, DateTimeOffset timestamp) =>
                events.ToList();

            public void SetUnit(string newUnit)
            {
                Unit = newUnit;
            }

            public void SetQuantiles(double[] newQuantiles)
            {
                Quantiles = newQuantiles;
            }
        }

        public class SumValues : IAggregateFunction
        {
            public string Unit;
            public double[] Quantiles;

            public IEnumerable<MetricEvent> Aggregate(IEnumerable<MetricEvent> events, DateTimeOffset timestamp) =>
                new List<MetricEvent>
                {
                    new MetricEvent(
                        events.Sum(e => e.Value),
                        new MetricTags(1).Append("key", "value"),
                        timestamp,
                        Unit,
                        null,
                        null)
                };

            public void SetUnit(string newUnit)
            {
                Unit = newUnit;
            }

            public void SetQuantiles(double[] newQuantiles)
            {
                Quantiles = newQuantiles;
            }
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