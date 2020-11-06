using System;
using JetBrains.Annotations;
using Vostok.Commons.Time;
using Vostok.Configuration.Abstractions.Attributes;

namespace Vostok.Metrics.Aggregations.Configuration
{
    [PublicAPI]
    public class AggregatorSettings
    {
        [Required]
        public string SourceStream { get; set; }

        [Required]
        public string TargetStream { get; set; }

        public TimeSpan Lag { get; set; } = 30.Seconds();
        public TimeSpan Period { get; set; } = 1.Minutes();

        public TimeSpan MaximumDeltaAfterNow { get; set; } = 10.Seconds();

        public int EventsReadBatchSize { get; set; } = 50_000;

        public int EventsWriteBufferCapacityLimit { get; set; } = 32 * 1024 * 1024;

        public int? EventsLimitMetric { get; set; }
    }
}