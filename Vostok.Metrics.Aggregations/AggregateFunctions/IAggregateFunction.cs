using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.AggregateFunctions
{
    [PublicAPI]
    public interface IAggregateFunction
    {
        [NotNull]
        IEnumerable<MetricEvent> Aggregate([NotNull] IEnumerable<double> values, DateTimeOffset timestamp);

        void SetUnit([CanBeNull] string newUnit);

        void SetQuantiles([CanBeNull] double[] newQuantiles);
    }
}