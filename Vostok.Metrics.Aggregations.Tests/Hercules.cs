using System;
using FluentAssertions.Extensions;
using NUnit.Framework;
using Vostok.Hercules.Client;
using Vostok.Hercules.Local;
using Vostok.Logging.Abstractions;
using Vostok.Logging.Console;
using Vostok.Metrics.Hercules.Readers;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.Tests
{
    internal class Hercules
    {
        // ReSharper disable once InconsistentNaming
        private static readonly Lazy<Hercules> instance = new Lazy<Hercules>(() => new Hercules());
        public readonly HerculesCluster Cluster;

        private Hercules()
        {
            var log = new SynchronousConsoleLog();

            Cluster = HerculesCluster.DeployNew(TestContext.CurrentContext.TestDirectory, log.WithMinimumLevel(LogLevel.Warn));

            string GetApiKey() => Cluster.ApiKey;

            var managementSettings = new HerculesManagementClientSettings(
                Cluster.HerculesManagementApiTopology,
                GetApiKey);

            var streamSettings = new HerculesStreamClientSettings(
                Cluster.HerculesStreamApiTopology,
                GetApiKey);

            var metricsStreamSettings = new HerculesStreamClientSettings<MetricEvent>(
                Cluster.HerculesStreamApiTopology,
                GetApiKey,
                buffer => new HerculesMetricEventReader(buffer));

            var gateSettings = new HerculesGateClientSettings(
                Cluster.HerculesGateTopology,
                GetApiKey);

            var sinkSettings = new HerculesSinkSettings(
                Cluster.HerculesGateTopology,
                GetApiKey)
            {
                SendPeriod = 1.Seconds()
            };

            Management = new HerculesManagementClient(
                managementSettings,
                log);

            Sink = new HerculesSink(sinkSettings, log);

            Stream = new HerculesStreamClient(streamSettings, log);

            MetricsStream = new HerculesStreamClient<MetricEvent>(metricsStreamSettings, log);

            Gate = new HerculesGateClient(gateSettings, log);
        }

        public static Hercules Instance => instance.Value;

        public static void Dispose()
        {
            if (instance.IsValueCreated)
            {
                Instance.Sink?.Dispose();
                Instance.Cluster?.Dispose();
            }
        }

        public HerculesSink Sink { get; }
        public HerculesManagementClient Management { get; }
        public HerculesGateClient Gate { get; }
        public HerculesStreamClient Stream { get; }
        public HerculesStreamClient<MetricEvent> MetricsStream { get; }
    }
}