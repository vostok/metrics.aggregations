using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Vostok.Hercules.Client.Abstractions.Events;
using Vostok.Hercules.Client.Abstractions.Models;
using Vostok.Hercules.Client.Abstractions.Queries;
using Vostok.Hercules.Consumers;
using Vostok.Logging.Abstractions;
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
                EventsBatchSize = settings.EventsBatchSize 
            };

            consumer = new StreamConsumer(streamConsumerSettings, log);
            aggregators = new Dictionary<MetricTags, OneMetricAggregator>();
        }

        public Task RunAsync(CancellationToken cancellationToken)
        {
            return consumer.RunAsync(cancellationToken);
        }

        private async Task HandleAsync(StreamCoordinates coordinates, IList<HerculesEvent> herculesEvents, CancellationToken cancellationToken)
        {
            var events = herculesEvents.Select(HerculesMetricEventFactory.CreateFrom).ToList();
            var droppedEvents = 0;

            foreach (var @event in events)
            {
                if (!aggregators.ContainsKey(@event.Tags))
                    aggregators[@event.Tags] = new OneMetricAggregator(@event.Tags, settings, log);
                if (!aggregators[@event.Tags].AddEvent(@event, coordinates))
                    droppedEvents++;
            }

            var result = new AggregateResult();

            foreach (var aggregator in aggregators)
            {
                result.AddAggregateResult(aggregator.Value.Aggregate());
            }

            // TODO(kungurtsev): do something with old aggregators.

            var insertQuery = new InsertEventsQuery(
                settings.TargetStreamName, 
                result.AggregatedEvents.Select(HerculesEventMetricBuilder.Build).ToList());

            var insertResult = await settings.GateClient
                .InsertAsync(insertQuery, settings.EventsWriteTimeout, cancellationToken)
                .ConfigureAwait(false);

            insertResult.EnsureSuccess();

            LogProgress(events, result, droppedEvents);

#pragma warning disable 4014
            Task.Run(() => settings.CoordinatesStorage.AdvanceAsync(coordinates), cancellationToken);
#pragma warning restore 4014
        }

        private void LogProgress(List<MetricEvent> @in, AggregateResult result, int dropped)
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
        }
    }
}