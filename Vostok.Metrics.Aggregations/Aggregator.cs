using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Vostok.Commons.Helpers.Extensions;
using Vostok.Hercules.Client.Abstractions.Models;
using Vostok.Hercules.Client.Abstractions.Queries;
using Vostok.Hercules.Consumers;
using Vostok.Hercules.Consumers.Helpers;
using Vostok.Logging.Abstractions;
using Vostok.Metrics.Aggregations.Helpers;
using Vostok.Metrics.Aggregations.MetricAggregator;
using Vostok.Metrics.Hercules;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations
{
    [PublicAPI]
    public class Aggregator
    {
        private readonly AggregatorSettings settings;
        private readonly ILog log;
        private readonly Dictionary<MetricTags, OneMetricAggregator> aggregators;
        private readonly StreamReader streamReader;

        private StreamShardingSettings shardingSettings;
        private StreamCoordinates leftCoordinates;
        private StreamCoordinates rightCoordinates;

        private bool restart;

        public Aggregator(AggregatorSettings settings, ILog log)
        {
            this.settings = settings;
            this.log = log;

            var streamReaderSettings = new StreamReaderSettings(
                settings.SourceStreamName,
                settings.StreamClient)
            {
                EventsBatchSize = settings.EventsBatchSize,
                EventsReadTimeout = settings.EventsReadTimeout
            };
            
            streamReader = new StreamReader(streamReaderSettings, log);
            aggregators = new Dictionary<MetricTags, OneMetricAggregator>();
        }

        public async Task RunAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // (iloktionov): Catch-up with state for other shards on any change to our sharding settings:
                    var newShardingSettings = settings.ShardingSettingsProvider();
                    if (shardingSettings == null || !shardingSettings.Equals(newShardingSettings))
                    {
                        log.Info(
                            "Observed new sharding settings: shard with index {ShardIndex} from {ShardCount}. Restarting aggregation.",
                            newShardingSettings.ClientShardIndex,
                            newShardingSettings.ClientShardCount);

                        shardingSettings = newShardingSettings;

                        restart = true;
                    }

                    if (restart)
                    {
                        await Restart(cancellationToken).ConfigureAwait(false);
                        restart = false;
                    }

                    await MakeIteration(cancellationToken).ConfigureAwait(false);
                }
                catch (Exception error)
                {
                    if (cancellationToken.IsCancellationRequested)
                        return;

                    log.Error(error);

                    await Task.Delay(settings.DelayOnError, cancellationToken).SilentlyContinue().ConfigureAwait(false);
                }
            }

        }

        private async Task Restart(CancellationToken cancellationToken)
        {
            leftCoordinates = await settings.LeftCoordinatesStorage.GetCurrentAsync().ConfigureAwait(false);
            rightCoordinates = await settings.RightCoordinatesStorage.GetCurrentAsync().ConfigureAwait(false);

            aggregators.Clear();

            log.Info("Updated coordinates from storage: left: {LeftCoordinates}, right: {RightCoordinates}.", leftCoordinates, rightCoordinates);

            var segmentReaderSettings = new StreamSegmentReaderSettings(
                settings.SourceStreamName,
                settings.StreamClient,
                leftCoordinates,
                rightCoordinates)
            {
                EventsBatchSize = settings.EventsBatchSize,
                EventsReadTimeout = settings.EventsReadTimeout
            };

            var segmentReader = new StreamSegmentReader(segmentReaderSettings, log);

            var coordinates = leftCoordinates;

            while (true)
            {
                var (query, result) = await segmentReader.ReadAsync(coordinates, shardingSettings, cancellationToken).ConfigureAwait(false);
                if (result == null)
                    break;

                foreach (var @event in result.Payload.Events.Select(HerculesMetricEventFactory.CreateFrom))
                {
                    if (!aggregators.ContainsKey(@event.Tags))
                        aggregators[@event.Tags] = new OneMetricAggregator(@event.Tags, settings, log);
                    aggregators[@event.Tags].AddEvent(@event, query.Coordinates);
                }

                foreach (var aggregator in aggregators)
                {
                    aggregator.Value.Aggregate();
                }

                coordinates = result.Payload.Next;
            }
            
            rightCoordinates = coordinates;
            log.Info("Coordinates after restart: left: {LeftCoordinates}, right: {RightCoordinates}.", leftCoordinates, rightCoordinates);
        }

        private async Task MakeIteration(CancellationToken cancellationToken)
        {
            var (query, readResult) = await streamReader.ReadAsync(rightCoordinates, shardingSettings, cancellationToken).ConfigureAwait(false);

            var eventsDropped = 0;

            foreach (var @event in readResult.Payload.Events.Select(HerculesMetricEventFactory.CreateFrom))
            {
                if (!aggregators.ContainsKey(@event.Tags))
                    aggregators[@event.Tags] = new OneMetricAggregator(@event.Tags, settings, log);
                if (!aggregators[@event.Tags].AddEvent(@event, query.Coordinates))
                    eventsDropped++;
            }

            var result = new AggregateResult();
            var staleAggregators = new List<MetricTags>();

            foreach (var aggregator in aggregators)
            {
                var aggregateResult = aggregator.Value.Aggregate();
                result.AddAggregateResult(aggregateResult);

                if (aggregateResult.ActiveEventsCount == 0
                    && DateTimeOffset.Now - aggregator.Value.LastEventAdded > settings.MetricTtl)
                    staleAggregators.Add(aggregator.Key);
            }

            foreach (var aggregator in staleAggregators)
            {
                aggregators.Remove(aggregator);
            }

            leftCoordinates = result.FirstActiveEventCoordinates ?? readResult.Payload.Next;
            rightCoordinates = readResult.Payload.Next;

            if (result.AggregatedEvents.Any())
                await SendAggregatedEvents(result, cancellationToken).ConfigureAwait(false);

            await LogProgress(result, readResult.Payload.Events.Count, eventsDropped, cancellationToken).ConfigureAwait(false);

            await SaveProgress().ConfigureAwait(false);
        }

        private async Task SendAggregatedEvents(AggregateResult result, CancellationToken cancellationToken)
        {
            var insertQuery = new InsertEventsQuery(
                settings.TargetStreamName,
                result.AggregatedEvents.Select(HerculesEventMetricBuilder.Build).ToList());

            var insertResult = await settings.GateClient
                .InsertAsync(insertQuery, settings.EventsWriteTimeout, cancellationToken)
                .ConfigureAwait(false);

            insertResult.EnsureSuccess();
        }

        private async Task SaveProgress()
        {
            try
            {
                await settings.RightCoordinatesStorage.AdvanceAsync(rightCoordinates).ConfigureAwait(false);
                await settings.LeftCoordinatesStorage.AdvanceAsync(leftCoordinates).ConfigureAwait(false);
                log.Info("Saved coordinates: left: {LeftCoordinates}, right: {RightCoordinates}.", leftCoordinates, rightCoordinates);
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to save coordinates: left: {LeftCoordinates}, right: {RightCoordinates}.", leftCoordinates, rightCoordinates);
            }
        }

        private async Task LogProgress(AggregateResult result, int eventsIn, int eventsDropped, CancellationToken cancellationToken)
        {
            log.Info(
                "Global aggregator progress: events in: {EventsIn}, events out: {EventsOut}, events dropped: {EventsDropped}.",
                eventsIn,
                result.AggregatedEvents.Count,
                eventsDropped);

            log.Info(
                "Global aggregator status: aggregators: {AggregatorsCount}, windows: {WindowsCount}, events: {EventsCount}.",
                aggregators.Count,
                result.ActiveWindowsCount,
                result.ActiveEventsCount);

            var remaining = await streamReader.CountStreamRemainingEvents(rightCoordinates, shardingSettings).ConfigureAwait(false);
            log.Info("Global aggregator progress: stream remaining events: {EventsRemaining}.", remaining);
        }
    }
}