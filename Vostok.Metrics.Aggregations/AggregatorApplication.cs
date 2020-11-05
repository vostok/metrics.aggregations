using System;
using System.Threading.Tasks;
using Vostok.Clusterclient.Core;
using Vostok.Clusterclient.Core.Topology;
using Vostok.Commons.Helpers.Extensions;
using Vostok.Hercules.Consumers;
using Vostok.Hosting.Abstractions;
using Vostok.Hosting.Abstractions.Requirements;
using Vostok.Metrics.Aggregations.AggregateFunctions;
using Vostok.Metrics.Hercules.Readers;
using Vostok.Metrics.Models;
using Vostok.Metrics.Primitives.Gauge;

namespace Vostok.Metrics.Aggregations
{
    [RequiresConfiguration(typeof(AggregatorSettings))]
    [RequiresSecretConfiguration(typeof(AggregatorSecretSettings))]
    public class AggregatorApplication : IVostokApplication
    {
        private readonly Func<IAggregateFunction> aggregateFunction;
        private WindowedStreamConsumer<MetricEvent, MetricTags> consumer;
        private Task writeTask;

        public AggregatorApplication(Func<IAggregateFunction> aggregateFunction) =>
            this.aggregateFunction = aggregateFunction;

        public Task InitializeAsync(IVostokHostingEnvironment environment)
        {
            SetupEventsLimitMetric(environment, () => environment.ConfigurationProvider.Get<AggregatorSettings>().EventsLimitMetric);

            var settings = environment.ConfigurationProvider.Get<AggregatorSettings>();
            Func<string> apiKeyProvider = () => environment.SecretConfigurationProvider.Get<AggregatorSecretSettings>().HerculesApiKey;

            var binaryWriterSettings = new StreamBinaryWriterSettings(
                apiKeyProvider,
                environment.HostExtensions.Get<IClusterProvider>(Constants.GateClusterProviderKey))
            {
                MetricContext = environment.Metrics.Instance,
                GateClientAdditionalSetup = environment.HostExtensions.Get<ClusterClientSetup>(Constants.GateClientAdditionalSetupKey)
            };

            var binaryWriter = new StreamBinaryWriter(binaryWriterSettings, environment.Log);

            var eventsWriterSettings = new StreamBinaryEventsWriterSettings(binaryWriter, settings.TargetStream)
            {
                BufferCapacityLimit = settings.EventsWriteBufferCapacityLimit
            };

            var eventsWriter = new StreamBinaryEventsWriter(eventsWriterSettings, environment.Log);

            var consumerSettings = new WindowedStreamConsumerSettings<MetricEvent, MetricTags>(
                settings.SourceStream,
                apiKeyProvider,
                environment.HostExtensions.Get<IClusterProvider>(Constants.StreamApiClusterProviderKey),
                s => s.Tags,
                s => s.Timestamp,
                _ => new AggregatorWindow(aggregateFunction(), eventsWriter),
                r => new HerculesMetricEventReader(r),
                environment.HostExtensions.Get<IStreamCoordinatesStorage>(Constants.LeftCoordinatesStorageKey),
                environment.HostExtensions.Get<IStreamCoordinatesStorage>(Constants.RightCoordinatesStorageKey),
                () => new StreamShardingSettings(environment.ApplicationReplicationInfo.InstanceIndex, environment.ApplicationReplicationInfo.InstancesCount)
            )
            {
                EventsReadBatchSize = settings.EventsReadBatchSize,
                Lag = settings.Lag,
                Period = settings.Period,
                LagProvider = e => e.AggregationParameters?.GetAggregationLag(),
                PeriodProvider = e => e.AggregationParameters?.GetAggregationPeriod(),
                MetricContext = environment.Metrics.Instance,
                StreamApiClientAdditionalSetup = environment.HostExtensions.Get<ClusterClientSetup>(Constants.StreamApiClientAdditionalSetupKey),
                MaximumDeltaAfterNow = settings.MaximumDeltaAfterNow,
                OnBatchBegin = _ => writeTask?.GetAwaiter().GetResult(),
                OnBatchEnd = _ =>
                {
                    writeTask = eventsWriter.WriteAsync().SilentlyContinue();
                }
            };

            consumer = new WindowedStreamConsumer<MetricEvent, MetricTags>(consumerSettings, environment.Log);

            return Task.CompletedTask;
        }

        public Task RunAsync(IVostokHostingEnvironment environment) =>
            consumer.RunAsync(environment.ShutdownToken);

        private void SetupEventsLimitMetric(IVostokHostingEnvironment environment, Func<int?> limit)
        {
            if (environment.ApplicationReplicationInfo.InstanceIndex == 0)
            {
                environment.Metrics.Application.CreateFuncGauge("events", "type")
                    .For("limit")
                    .SetValueProvider(() => limit());
            }
        }
    }
}