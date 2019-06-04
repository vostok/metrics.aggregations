using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Vostok.Hercules.Client.Abstractions.Events;
using Vostok.Hercules.Client.Abstractions.Models;
using Vostok.Hercules.Client.Abstractions.Queries;
using Vostok.Hercules.Client.Abstractions.Results;
using Vostok.Hercules.Consumers;
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
        private readonly StreamConsumer consumer;
        private readonly ILog log;
        private readonly Dictionary<MetricTags, OneMetricAggregator> aggregators;

        public Aggregator(AggregatorSettings settings, ILog log)
        {
            this.settings = settings;
            this.log = log;

            var streamConsumerSettings = new StreamConsumerSettings(
                settings.SourceStreamName,
                settings.StreamClient,
                new AdHocEventsHandler(HandleAsync),
                settings.CoordinatesStorage,
                settings.ShardingSettingsProvider
            )
            {
                AutoSaveCoordinates = false,
                HandleWithoutEvents = true,
                EventsBatchSize = settings.EventsBatchSize,
                DelayOnError = settings.DelayOnError,
                DelayOnNoEvents = settings.DelayOnNoEvents,
                EventsReadTimeout = settings.EventsReadTimeout
            };

            consumer = new StreamConsumer(streamConsumerSettings, log);
            aggregators = new Dictionary<MetricTags, OneMetricAggregator>();
        }

        public Task RunAsync(CancellationToken cancellationToken)
        {
            return consumer.RunAsync(cancellationToken);
        }

        private async Task HandleAsync(ReadStreamQuery query, ReadStreamResult streamResult, CancellationToken cancellationToken)
        {
            var events = streamResult.Payload.Events.Select(HerculesMetricEventFactory.CreateFrom).ToList();
            var droppedEvents = 0;

            foreach (var @event in events)
            {
                if (!aggregators.ContainsKey(@event.Tags))
                    aggregators[@event.Tags] = new OneMetricAggregator(@event.Tags, settings, log);
                if (!aggregators[@event.Tags].AddEvent(@event, streamResult.Payload.Next))
                    droppedEvents++;
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

            if (result.FirstActiveEventCoordinates == null)
                result.AddActiveCoordinates(query.Coordinates);

            if (result.AggregatedEvents.Any())
                await SendAggregatedEvents(result, cancellationToken).ConfigureAwait(false);

            await LogProgress(query, events, result, droppedEvents).ConfigureAwait(false);

            await SaveProgress(result.FirstActiveEventCoordinates).ConfigureAwait(false);
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

        private async Task SaveProgress(StreamCoordinates coordinates)
        {
            try
            {
                await settings.CoordinatesStorage.AdvanceAsync(coordinates).ConfigureAwait(false);
                log.Info("Saved coordinates to storage: {StreamCoordinates}.", coordinates);
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to save {Coordinates} coordinates.", coordinates);
            }
        }

        private async Task LogProgress(ReadStreamQuery query, List<MetricEvent> @in, AggregateResult result, int dropped)
        {
            log.Info(
                "Global aggregator progress: events in: {EventsIn}, events out: {EventsOut}, events dropped: {EventsDropped}.",
                @in.Count,
                result.AggregatedEvents.Count,
                dropped);

            log.Info(
                "Global aggregator status: aggregators: {AggregatorsCount}, windows: {WindowsCount}, events: {EventsCount}.",
                aggregators.Count,
                result.ActiveWindowsCount,
                result.ActiveEventsCount);

            var end = await GetEndCoordinates(query).ConfigureAwait(false);
            if (end != null)
            {
                var remainingEvents = StreamCoordinatesMerger.Distance(query.Coordinates, end) - @in.Count;
                log.Info("Global aggregator progress: stream remaining events: {EventsRemaining}.", remainingEvents);
            }
        }

        private async Task<StreamCoordinates> GetEndCoordinates(ReadStreamQuery query)
        {
            try
            {
                var seekToEndQuery = new SeekToEndStreamQuery(settings.SourceStreamName)
                {
                    ClientShard = query.ClientShard,
                    ClientShardCount = query.ClientShardCount
                };
                var end = await settings.StreamClient.SeekToEndAsync(seekToEndQuery, settings.EventsReadTimeout).ConfigureAwait(false);

                return end.Payload.Next;
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to count remaining events.");
                return null;
            }
        }
    }
}