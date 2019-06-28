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
using Vostok.Logging.Abstractions;
using Vostok.Metrics.Aggregations.Helpers;
using Vostok.Metrics.Aggregations.MetricAggregator;
using Vostok.Metrics.Grouping;
using Vostok.Metrics.Hercules;
using Vostok.Metrics.Models;
using Vostok.Metrics.Primitives.Gauge;
using Vostok.Metrics.Primitives.Timer;

namespace Vostok.Metrics.Aggregations
{
    [PublicAPI]
    public class Aggregator
    {
        private readonly AggregatorSettings settings;
        private readonly ILog log;
        private readonly Dictionary<MetricTags, OneMetricAggregator> aggregators;
        private readonly StreamReader<MetricEvent> streamReader;

        private readonly IMetricGroup1<IIntegerGauge> eventsMetric;
        private readonly IMetricGroup1<IIntegerGauge> stateMetric;
        private readonly IMetricGroup1<ITimer> iterationMetric;

        private volatile StreamShardingSettings shardingSettings;
        private volatile StreamCoordinates leftCoordinates;
        private volatile StreamCoordinates rightCoordinates;

        private volatile bool restart;

        public Aggregator(AggregatorSettings settings, ILog log)
        {
            this.settings = settings;
            this.log = log;

            var streamReaderSettings = new StreamReaderSettings<MetricEvent>(
                settings.SourceStreamName,
                settings.StreamClient)
            {
                EventsBatchSize = settings.EventsReadBatchSize,
                EventsReadTimeout = settings.EventsReadTimeout
            };

            streamReader = new StreamReader<MetricEvent>(streamReaderSettings, log);
            aggregators = new Dictionary<MetricTags, OneMetricAggregator>();

            eventsMetric = settings.MetricContext.CreateIntegerGauge("events", "type", new IntegerGaugeConfig {ResetOnScrape = true});
            stateMetric = settings.MetricContext.CreateIntegerGauge("state", "type");
            settings.MetricContext.CreateFuncGauge("events", "type").For("remaining").SetValueProvider(CountStreamRemainingEvents);
            iterationMetric = settings.MetricContext.CreateSummary("iteration", "type", new SummaryConfig {Quantiles = new []{0.5, 0.75, 1}});
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

                    using (iterationMetric.For("iteration").Measure())
                    {
                        await MakeIteration(cancellationToken).ConfigureAwait(false);
                    }
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

            var segmentReaderSettings = new StreamSegmentReaderSettings<MetricEvent>(
                settings.SourceStreamName,
                settings.StreamClient,
                leftCoordinates,
                rightCoordinates)
            {
                EventsBatchSize = settings.EventsReadBatchSize,
                EventsReadTimeout = settings.EventsReadTimeout
            };

            var segmentReader = new StreamSegmentReader<MetricEvent>(segmentReaderSettings, log);

            var coordinates = leftCoordinates;

            while (true)
            {
                var (query, result) = await segmentReader.ReadAsync(coordinates, shardingSettings, cancellationToken).ConfigureAwait(false);
                if (result == null)
                    break;

                foreach (var @event in result.Payload.Events)
                {
                    if (!aggregators.ContainsKey(@event.Tags))
                        aggregators[@event.Tags] = new OneMetricAggregator(@event.Tags, settings, log);
                    aggregators[@event.Tags].AddEvent(@event, query.Coordinates);
                }

                foreach (var aggregator in aggregators)
                {
                    aggregator.Value.Aggregate(true);
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

            foreach (var @event in readResult.Payload.Events)
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
                    && DateTimeOffset.UtcNow - aggregator.Value.LastEventAdded > settings.MetricTtl)
                    staleAggregators.Add(aggregator.Key);
            }

            foreach (var aggregator in staleAggregators)
            {
                aggregators.Remove(aggregator);
            }

            leftCoordinates = result.FirstActiveEventCoordinates ?? readResult.Payload.Next;
            rightCoordinates = readResult.Payload.Next;

            if (result.AggregatedEvents.Any())
            {
                await SendAggregatedEvents(result, cancellationToken).ConfigureAwait(false);
                await SaveProgress().ConfigureAwait(false);
            }

            LogProgress(result, readResult.Payload.Events.Count, eventsDropped);

            if (readResult.Payload.Events.Count == 0)
            {
                await Task.Delay(settings.DelayOnNoEvents, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task SendAggregatedEvents(AggregateResult result, CancellationToken cancellationToken)
        {
            var events = result.AggregatedEvents.Select(HerculesEventMetricBuilder.Build).ToList();

            while (events.Any())
            {
                try
                {
                    var insertQuery = new InsertEventsQuery(
                        settings.TargetStreamName,
                        events.Take(settings.EventsSendBatchSize).ToList());

                    var insertResult = await settings.GateClient
                        .InsertAsync(insertQuery, settings.EventsWriteTimeout, cancellationToken)
                        .ConfigureAwait(false);

                    insertResult.EnsureSuccess();

                    events = events.Skip(settings.EventsSendBatchSize).ToList();

                    break;
                }
                catch (Exception e)
                {
                    log.Error(e, "Failed to send aggregated events.");
                    await Task.Delay(settings.DelayOnError, cancellationToken).ConfigureAwait(false);
                }
            }
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

        private void LogProgress(AggregateResult result, int eventsIn, int eventsDropped)
        {
            log.Info(
                "Global aggregator progress: events in: {EventsIn}, events out: {EventsOut}, events dropped: {EventsDropped}.",
                eventsIn,
                result.AggregatedEvents.Count,
                eventsDropped);

            iterationMetric.For("in").Report(eventsIn);
            eventsMetric.For("in").Add(eventsIn);
            eventsMetric.For("out").Add(result.AggregatedEvents.Count);
            eventsMetric.For("dropped").Add(eventsDropped);

            log.Info(
                "Global aggregator status: aggregators: {AggregatorsCount}, windows: {WindowsCount}, events: {EventsCount}.",
                aggregators.Count,
                result.ActiveWindowsCount,
                result.ActiveEventsCount);

            stateMetric.For("aggregators").Set(aggregators.Count);
            stateMetric.For("windows").Set(result.ActiveWindowsCount);
            stateMetric.For("events").Set(result.ActiveEventsCount);
        }

        private double CountStreamRemainingEvents()
        {
            var remaining = streamReader.CountStreamRemainingEvents(rightCoordinates, shardingSettings).GetAwaiter().GetResult();
            log.Info("Global aggregator progress: stream remaining events: {EventsRemaining}.", remaining);
            return remaining;
        }
    }
}