﻿using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Vostok.Hercules.Client.Abstractions.Models;
using Vostok.Hercules.Client.Abstractions.Queries;
using Vostok.Hercules.Client.Abstractions.Results;
using Vostok.Hercules.Consumers;
using Vostok.Logging.Abstractions;

namespace Vostok.Metrics.Aggregations.Helpers
{
    internal class StreamSegmentReader
    {
        private readonly StreamReader streamReader;
        private readonly StreamSegmentReaderSettings settings;
        private readonly ILog log;
        private int? streamPartitionsCount;

        public StreamSegmentReader([NotNull] StreamSegmentReaderSettings settings, [CanBeNull] ILog log)
        {
            this.settings = settings ?? throw new ArgumentNullException(nameof(settings));
            this.log = (log ?? LogProvider.Get()).ForContext<StreamSegmentReader>();

            var streamReaderSettings = new StreamReaderSettings(
                settings.StreamName,
                settings.StreamClient)
            {
                EventsBatchSize = settings.EventsBatchSize,
                EventsReadTimeout = settings.EventsReadTimeout
            };

            streamReader = new StreamReader(streamReaderSettings, log);
        }

        public async Task<(ReadStreamQuery query, ReadStreamResult result)> ReadAsync(
            StreamCoordinates coordinates,
            StreamShardingSettings shardingSettings,
            CancellationToken cancellationToken)
        {
            log.Info(
                "Reading logical shard with index {ClientShard} from {ClientShardCount}.",
                shardingSettings.ClientShardIndex,
                shardingSettings.ClientShardCount);

            log.Debug("Current coordinates: {StreamCoordinates}.", coordinates);

            coordinates = await GetShardCoordinates(coordinates, shardingSettings, cancellationToken).ConfigureAwait(false);
            log.Debug("Current shard coordinates: {StreamCoordinates}.", coordinates);

            streamPartitionsCount = streamPartitionsCount ?? await GetPartitionsCount(cancellationToken).ConfigureAwait(false);

            var current = coordinates.ToDictionary();
            foreach (var partition in coordinates.Positions.Select(p => p.Partition))
            {
                var start = current.ContainsKey(partition) ? current[partition].Offset : 0;
                var end = settings.End.ContainsKey(partition) ? settings.End[partition].Offset : 0;

                if (start < end)
                {
                    var count = end - start;

                    log.Info("Reading {EventsCount} events from {Partition} partition.", count, partition);

                    var (query, result) = await streamReader.ReadAsync(
                            coordinates,
                            // ReSharper disable once PossibleInvalidOperationException
                            new StreamShardingSettings(partition, streamPartitionsCount.Value),
                            count,
                            cancellationToken)
                        .ConfigureAwait(false);

                    result = new ReadStreamResult(
                        result.Status,
                        new ReadStreamPayload(
                            result.Payload.Events,
                            coordinates.SetPosition(result.Payload.Next.Positions.Single())),
                        result.ErrorDetails);

                    query = new ReadStreamQuery(query.Name)
                    {
                        Coordinates = coordinates,
                        ClientShard = shardingSettings.ClientShardIndex,
                        ClientShardCount = shardingSettings.ClientShardCount
                    };

                    return (query, result);
                }
            }

            return (null, null);
        }

        private async Task<int> GetPartitionsCount(CancellationToken cancellationToken)
        {
            var allCoordinates = await GetShardCoordinates(StreamCoordinates.Empty, new StreamShardingSettings(0, 1), cancellationToken).ConfigureAwait(false);
            return allCoordinates.Positions.Length;
        }

        private async Task<StreamCoordinates> GetShardCoordinates(
            StreamCoordinates coordinates,
            StreamShardingSettings shardingSettings,
            CancellationToken cancellationToken)
        {
            var (_, result) = await streamReader.ReadAsync(coordinates, shardingSettings, 0, cancellationToken).ConfigureAwait(false);
            return result.Payload.Next;
        }
    }
}