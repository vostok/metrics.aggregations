using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.AggregateFunctions
{
    [PublicAPI]
    public interface IAggregateFunction
    {
        void Add([NotNull] MetricEvent @event);

        [NotNull]
        IEnumerable<MetricEvent> Aggregate(DateTimeOffset timestamp);
    }
}