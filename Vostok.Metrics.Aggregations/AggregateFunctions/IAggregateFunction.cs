using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.AggregateFunctions
{
    [PublicAPI]
    public interface IAggregateFunction
    {
        IEnumerable<MetricEvent> Aggregate(IEnumerable<MetricEvent> events, DateTimeOffset timestamp);

        void SetUnit([CanBeNull] string newUnit);

        void SetQuantiles([CanBeNull] double[] newQuantiles);
    }
}