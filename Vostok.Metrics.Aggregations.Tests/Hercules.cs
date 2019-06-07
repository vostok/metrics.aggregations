using System;
using FluentAssertions.Extensions;
using NUnit.Framework;
using Vostok.Hercules.Client;
using Vostok.Hercules.Local;
using Vostok.Logging.Abstractions;
using Vostok.Logging.Console;

namespace Vostok.Metrics.Aggregations.Tests
{
    internal class Hercules
    {
        // ReSharper disable once InconsistentNaming
        private static readonly Lazy<Hercules> instance = new Lazy<Hercules>(() => new Hercules());
        private readonly HerculesCluster cluster;

        private Hercules()
        {
            var log = new SynchronousConsoleLog();

            cluster = HerculesCluster.DeployNew(TestContext.CurrentContext.TestDirectory, log.WithMinimumLevel(LogLevel.Warn));

            string GetApiKey() => cluster.ApiKey;

            var managementSettings = new HerculesManagementClientSettings(
                cluster.HerculesManagementApiTopology,
                GetApiKey);

            var streamSettings = new HerculesStreamClientSettings(
                cluster.HerculesStreamApiTopology,
                GetApiKey);

            var gateSettings = new HerculesGateClientSettings(
                cluster.HerculesGateTopology,
                GetApiKey);

            var sinkSettings = new HerculesSinkSettings(
                cluster.HerculesGateTopology,
                GetApiKey)
            {
                SendPeriod = 1.Seconds()
            };

            Management = new HerculesManagementClient(
                managementSettings,
                log);

            Sink = new HerculesSink(sinkSettings, log);

            Stream = new HerculesStreamClient(streamSettings, log);

            Gate = new HerculesGateClient(gateSettings, log);
        }

        public static Hercules Instance => instance.Value;

        public static void Dispose()
        {
            if (instance.IsValueCreated)
            {
                Instance.Sink?.Dispose();
                Instance.cluster?.Dispose();
            }
        }

        public HerculesSink Sink { get; }
        public HerculesManagementClient Management { get; }
        public HerculesGateClient Gate { get; }
        public HerculesStreamClient Stream { get; }
    }
}