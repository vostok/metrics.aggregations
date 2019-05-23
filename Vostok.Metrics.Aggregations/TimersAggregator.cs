using System;
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
using Vostok.Metrics.Hercules;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations
{
    [PublicAPI]
    public class TimersAggregator
    {
        private readonly AggregatorSettings settings;
        private readonly StreamConsumer consumer;
        private readonly ILog log;
        private readonly Dictionary<MetricTags, OneMetricAggregator> aggregators;

        public TimersAggregator(AggregatorSettings settings, ILog log)
        {
            this.settings = settings;
            this.log = log;

            var streamConsumerSettings = new StreamConsumerSettings(
                settings.SourceStreamName,
                settings.StreamClient,
                new AdHocEventsHandler(HandleAsync), 
                settings.CoordinatesStorage,
                settings.ShardingSettingsProvider
            );

            consumer = new StreamConsumer(streamConsumerSettings, log);
            aggregators = new Dictionary<MetricTags, OneMetricAggregator>();
        }

        public Task RunAsync(CancellationToken cancellationToken)
        {
            return consumer.RunAsync(cancellationToken);
        }

        private async Task HandleAsync(StreamCoordinates coordinates, IList<HerculesEvent> events, CancellationToken cancellationToken)
        {
            var metrics = events.Select(HerculesMetricEventFactory.CreateFrom).ToList();

            foreach (var metric in metrics)
            {
                if (!aggregators.ContainsKey(metric.Tags))
                    aggregators[metric.Tags] = new OneMetricAggregator();
                aggregators[metric.Tags].AddEvent(metric);
            }

            var aggregatedMetrics = new List<HerculesEvent>();

            foreach (var aggregator in aggregators)
            {
                aggregatedMetrics.AddRange(aggregator.Value.GetAggregatedMetrics().Select(HerculesEventMetricBuilder.Build));
            }

            var insertQuery = new InsertEventsQuery(settings.TargetStreamName, aggregatedMetrics);

            var insertResult = await settings.GateClient
                .InsertAsync(insertQuery, settings.EventsWriteTimeout, cancellationToken)
                .ConfigureAwait(false);

            insertResult.EnsureSuccess();
        }
    }
}