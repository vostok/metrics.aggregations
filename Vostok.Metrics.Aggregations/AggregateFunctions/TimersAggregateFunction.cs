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
            quantileMetricsBuilder = new QuantileMetricsBuilder(null, tags, null);
        }

        public IEnumerable<MetricEvent> Aggregate(IEnumerable<double> values, DateTimeOffset timestamp)
            => quantileMetricsBuilder.Build(values.ToArray(), timestamp);

        public void SetUnit(string newUnit)
            => quantileMetricsBuilder.SetUnit(newUnit);

        public void SetQuantiles(double[] newQuantiles)
            => quantileMetricsBuilder.SetQuantiles(newQuantiles);
    }
}