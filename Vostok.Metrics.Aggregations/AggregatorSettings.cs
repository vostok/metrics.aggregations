using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Vostok.Commons.Time;
using Vostok.Hercules.Client.Abstractions;
using Vostok.Hercules.Consumers;
using Vostok.Metrics.Aggregations.AggregateFunctions;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations
{
    [PublicAPI]
    public class AggregatorSettings
    {
        [NotNull]
        public string SourceStreamName { get; }

        [NotNull]
        public string TargetStreamName { get; }

        [NotNull]
        public Func<MetricTags, IAggregateFunction> AggregateFunctionFactory { get; }

        [NotNull]
        public IHerculesStreamClient StreamClient { get; }

        [NotNull]
        public IHerculesGateClient GateClient { get; }

        [NotNull]
        public IStreamCoordinatesStorage LeftCoordinatesStorage { get; }

        [NotNull]
        public IStreamCoordinatesStorage RightCoordinatesStorage { get; }

        [NotNull]
        public Func<StreamShardingSettings> ShardingSettingsProvider { get; }

        public TimeSpan EventsWriteTimeout { get; set; } = 45.Seconds();

        public int EventsBatchSize { get; set; } = 100_000;

        public TimeSpan EventsReadTimeout { get; set; } = TimeSpan.FromSeconds(45);

        public TimeSpan DelayOnError { get; set; } = TimeSpan.FromSeconds(5);

        public TimeSpan DelayOnNoEvents { get; set; } = TimeSpan.FromSeconds(2);

        public TimeSpan DefaultPeriod { get; set; } = 1.Minutes();

        public TimeSpan DefaultLag { get; set; } = 30.Seconds();

        public TimeSpan MaximumEventBeforeNow { get; set; } = 1.Days();

        public TimeSpan MaximumEventAfterNow { get; set; } = 1.Minutes();

        public TimeSpan MetricTtl { get; set; } = 1.Hours();
        
        public AggregatorSettings(
            [NotNull] string sourceStreamName,
            [NotNull] string targetStreamName,
            [NotNull] Func<MetricTags, IAggregateFunction> aggregateFunctionFactory,
            [NotNull] IHerculesStreamClient streamClient,
            [NotNull] IHerculesGateClient gateClient,
            [NotNull] IStreamCoordinatesStorage leftCoordinatesStorage,
            [NotNull] IStreamCoordinatesStorage rightCoordinatesStorage,
            [NotNull] Func<StreamShardingSettings> shardingSettingsProvider)
        {
            SourceStreamName = sourceStreamName ?? throw new ArgumentNullException(nameof(sourceStreamName));
            TargetStreamName = targetStreamName ?? throw new ArgumentNullException(nameof(targetStreamName));
            AggregateFunctionFactory = aggregateFunctionFactory ?? throw new ArgumentNullException(nameof(aggregateFunctionFactory));
            StreamClient = streamClient ?? throw new ArgumentNullException(nameof(streamClient));
            GateClient = gateClient ?? throw new ArgumentNullException(nameof(gateClient));
            LeftCoordinatesStorage = leftCoordinatesStorage ?? throw new ArgumentNullException(nameof(leftCoordinatesStorage));
            RightCoordinatesStorage = rightCoordinatesStorage ?? throw new ArgumentNullException(nameof(rightCoordinatesStorage));
            ShardingSettingsProvider = shardingSettingsProvider ?? throw new ArgumentNullException(nameof(shardingSettingsProvider));
        }
    }
}