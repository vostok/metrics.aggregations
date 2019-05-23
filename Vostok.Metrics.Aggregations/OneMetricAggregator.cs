using System.Collections.Generic;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations
{
    internal class OneMetricAggregator
    {
        public void AddEvent(MetricEvent metric)
        {
            
        }

        public IEnumerable<MetricEvent> GetAggregatedMetrics()
        {
            yield break;
        }
    }
}