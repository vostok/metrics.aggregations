using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Vostok.Metrics.Models;
using Vostok.Metrics.Primitives.Timer;

namespace Vostok.Metrics.Aggregations.AggregateFunctions
{
    [PublicAPI]
    public class TimersAggregateFunction : IAggregateFunction
    {
        private readonly QuantileMetricsBuilder quantileMetricsBuilder;

        public TimersAggregateFunction([NotNull] MetricTags tags)
        {
            quantileMetricsBuilder = new QuantileMetricsBuilder(Quantiles.DefaultQuantiles, tags, null);
        }

        public IEnumerable<MetricEvent> Aggregate(IEnumerable<MetricEvent> events, DateTimeOffset timestamp)
            => quantileMetricsBuilder.Build(events.Select(e => e.Value).ToArray(), timestamp);

        public void SetUnit(string newUnit)
            => quantileMetricsBuilder.SetUnit(newUnit);

        public void SetQuantiles(double[] newQuantiles)
            => quantileMetricsBuilder.SetQuantiles(newQuantiles);
    }
}