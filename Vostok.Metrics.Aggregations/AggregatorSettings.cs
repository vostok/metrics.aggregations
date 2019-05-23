using System;
using JetBrains.Annotations;
using Vostok.Hercules.Client.Abstractions;
using Vostok.Hercules.Consumers;

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
        public IHerculesStreamClient StreamClient { get; }

        [NotNull]
        public IHerculesGateClient GateClient { get; }

        [NotNull]
        public IStreamCoordinatesStorage CoordinatesStorage { get; }

        [NotNull]
        public Func<StreamShardingSettings> ShardingSettingsProvider { get; }

        public TimeSpan EventsWriteTimeout { get; set; } = TimeSpan.FromSeconds(45);

        public AggregatorSettings(
            [NotNull] string sourceStreamName,
            [NotNull] string targetStreamName,
            [NotNull] IHerculesStreamClient streamClient,
            [NotNull] IHerculesGateClient gateClient,
            [NotNull] IStreamCoordinatesStorage coordinatesStorage,
            [NotNull] Func<StreamShardingSettings> shardingSettingsProvider)
        {
            SourceStreamName = sourceStreamName ?? throw new ArgumentNullException(nameof(sourceStreamName));
            TargetStreamName = targetStreamName ?? throw new ArgumentNullException(nameof(targetStreamName));
            StreamClient = streamClient ?? throw new ArgumentNullException(nameof(streamClient));
            GateClient = gateClient ?? throw new ArgumentNullException(nameof(gateClient));
            CoordinatesStorage = coordinatesStorage ?? throw new ArgumentNullException(nameof(coordinatesStorage));
            ShardingSettingsProvider = shardingSettingsProvider ?? throw new ArgumentNullException(nameof(shardingSettingsProvider));
        }
    }
}