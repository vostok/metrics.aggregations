using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using FluentAssertions.Extensions;
using NUnit.Framework;
using Vostok.Hercules.Client.Abstractions.Queries;
using Vostok.Hercules.Consumers;
using Vostok.Hosting;
using Vostok.Hosting.Setup;
using Vostok.Logging.Abstractions;
using Vostok.Logging.Console;
using Vostok.Metrics.Aggregations.AggregateFunctions;
using Vostok.Metrics.Hercules;
using Vostok.Metrics.Models;
using Vostok.Metrics.Primitives.Timer;
using Vostok.Metrics.Senders;

#pragma warning disable 4014

namespace Vostok.Metrics.Aggregations.Tests
{
    [TestFixture]
    internal class Aggregator_FunctionalTests
    {
        private readonly ILog log = new SynchronousConsoleLog();

        private readonly TimeSpan period = 1.Seconds();
        private readonly TimeSpan lag = 2.Seconds();

        private readonly int streamPartitions = 3;
        private readonly int aggregatorsCount = 2;
        private readonly int sendersCount = 5;
        private readonly int sendTimersPerSecond = 10;

        private readonly int expectedGoodIterations = 3;
        private readonly int readIterations = 5;

        private InMemoryCoordinatesStorage leftCoordinatesStorage;
        private InMemoryCoordinatesStorage rightCoordinatesStorage;
        private HerculesMetricSenderSettings senderSettings;
        private CancellationTokenSource cancellationTokenSource;

        private IMetricContext metricContext;

        private TestsHelpers.TestMetricSender testMetricSender;
        // ReSharper disable once CollectionNeverQueried.Local
        private List<AggregatorApplication> aggregators;

        [SetUp]
        public void SetUp()
        {
            cancellationTokenSource = new CancellationTokenSource();

            leftCoordinatesStorage = new InMemoryCoordinatesStorage();
            rightCoordinatesStorage = new InMemoryCoordinatesStorage();

            senderSettings = new HerculesMetricSenderSettings(Hercules.Instance.Sink);

            var management = Hercules.Instance.Management;

            var streamNames = new[] {senderSettings.FinalStream, senderSettings.TimersStream, senderSettings.CountersStream, senderSettings.HistogramsStream};

            var streamCreateTasks = streamNames.Select(
                    name => Task.Run(
                        async () =>
                        {
                            var createResult = await management.CreateStreamAsync(
                                new CreateStreamQuery(name)
                                {
                                    Partitions = streamPartitions,
                                    ShardingKey = new[] {"tagsHash"}
                                },
                                20.Seconds());
                            createResult.EnsureSuccess();
                        }))
                .ToArray();

            Task.WaitAll(streamCreateTasks);

            var herculesSender = new HerculesMetricSender(new HerculesMetricSenderSettings(Hercules.Instance.Sink));
            testMetricSender = new TestsHelpers.TestMetricSender();

            metricContext = new MetricContext(
                new MetricContextConfig(
                    new CompositeMetricEventSender(
                        new IMetricEventSender[]
                        {
                            herculesSender,
                            testMetricSender
                        }))
                {
                    ErrorCallback = error => log.Error(error)
                });

            aggregators = new List<AggregatorApplication>();
        }

        [TearDown]
        public void TearDown()
        {
            cancellationTokenSource.Cancel();
        }

        [Test]
        public void Should_aggregate_timers()
        {
            var firstAggregatorsToken = new CancellationTokenSource();
            var aggregatedEvents = new List<MetricEvent>();
            var receivedEvents = new Dictionary<(string, DateTime), int>();

            RunAggregators(senderSettings.TimersStream, senderSettings.FinalStream, () => new TimersAggregateFunction(), firstAggregatorsToken.Token);

            RunSenders();

            RunConsumer(aggregatedEvents);

            ReadAggregatedEvents(aggregatedEvents, receivedEvents);

            log.Info("Restarting aggregators.");
            firstAggregatorsToken.Cancel();
            RunAggregators(senderSettings.TimersStream, senderSettings.FinalStream, () => new TimersAggregateFunction(), cancellationTokenSource.Token);
            ReadAggregatedEvents(aggregatedEvents, receivedEvents);

            var sentEvents = testMetricSender.Events();

            var maxTimestamp = receivedEvents.Keys.Max(k => k.Item2);
            sentEvents = sentEvents.Where(e => maxTimestamp - RoundUp(e.Timestamp.UtcDateTime) > period).ToList();
            foreach (var key in receivedEvents.Keys.Where(k => maxTimestamp - k.Item2 <= period).ToList())
                receivedEvents.Remove(key);

            var expectedRecievedEvents = sentEvents
                .GroupBy(e => (e.Tags.Single(t => t.Key == WellKnownTagKeys.Name).Value, RoundUp(e.Timestamp.UtcDateTime)))
                .ToDictionary(g => g.Key, g => g.Count())
                .ToList();

            expectedRecievedEvents.Count.Should().BeGreaterOrEqualTo(expectedGoodIterations);
            receivedEvents.Should().BeEquivalentTo(expectedRecievedEvents);

            leftCoordinatesStorage.GetCurrentAsync().Result.Positions.Sum(p => p.Offset).Should().BeGreaterOrEqualTo(expectedRecievedEvents.Count);
            rightCoordinatesStorage.GetCurrentAsync().Result.Positions.Sum(p => p.Offset).Should().BeGreaterOrEqualTo(expectedRecievedEvents.Count);
        }

        private static DateTime RoundUp(DateTime date)
        {
            var ticks = 1.Seconds().Ticks;
            return new DateTime((date.Ticks + ticks) / ticks * ticks, date.Kind);
        }

        private void ReadAggregatedEvents(List<MetricEvent> aggregatedEvents, Dictionary<(string, DateTime), int> receivedEvents)
        {
            var eventsRecieved = 0L;
            var stopwatch = Stopwatch.StartNew();
            var expectedEvents = sendersCount * sendTimersPerSecond * readIterations;

            while (eventsRecieved < expectedEvents)
            {
                if (stopwatch.Elapsed > 20.Seconds())
                    throw new AssertionException($"Failed to recieve {expectedEvents} events, got only {eventsRecieved} in {stopwatch.Elapsed}.");

                List<MetricEvent> countMetrics;
                lock (aggregatedEvents)
                {
                    countMetrics = aggregatedEvents.Where(e => e.Tags.Any(t => t.Value == WellKnownTagValues.AggregateCount)).ToList();
                    aggregatedEvents.Clear();
                }

                if (!countMetrics.Any())
                {
                    log.Info("No recieved aggregated metrics.");
                    Thread.Sleep(1.Seconds());
                    continue;
                }

                foreach (var metric in countMetrics)
                {
                    eventsRecieved += (int)metric.Value;
                    var name = metric.Tags.Single(t => t.Key == WellKnownTagKeys.Name).Value;
                    log.Info($"Recieved aggregated metric {name} with count {metric.Value} timestamp {metric.Timestamp}.");

                    var key = (name, metric.Timestamp.UtcDateTime);
                    if (!receivedEvents.ContainsKey(key))
                        receivedEvents[key] = 0;
                    receivedEvents[key] += (int)metric.Value;
                }

                Thread.Sleep(1.Seconds());
            }
        }

        private void RunConsumer(List<MetricEvent> receivedEvents)
        {
            var consumer = new StreamConsumer(
                new StreamConsumerSettings(
                    // ReSharper disable once AssignNullToNotNullAttribute
                    senderSettings.FinalStream,
                    Hercules.Instance.Stream,
                    new AdHocEventsHandler(
                        (coordinates, streamResult, token) =>
                        {
                            lock (receivedEvents)
                            {
                                receivedEvents.AddRange(streamResult.Payload.Events.Select(HerculesMetricEventFactory.CreateFrom));
                            }

                            return Task.CompletedTask;
                        }),
                    new InMemoryCoordinatesStorage(),
                    () => new StreamShardingSettings(0, 1)
                ),
                log.ForContext("Consumer"));

            consumer.RunAsync(cancellationTokenSource.Token);
        }

        private void RunSenders()
        {
            for (var sender = 0; sender < sendersCount; sender++)
            {
                var timer = metricContext.CreateTimer(
                    $"timer-{sender}",
                    new TimerConfig
                    {
                        Unit = "unit",
                        AggregationParameters = new Dictionary<string, string>
                            {
                                ["key"] = "value"
                            }
                            .SetAggregationPeriod(period)
                            .SetAggregationLag(lag)
                    });

                var sender1 = sender;
                Task.Run(
                    async () =>
                    {
                        while (!cancellationTokenSource.IsCancellationRequested)
                        {
                            Parallel.For(0, sendTimersPerSecond / 10, t => { timer.Report(t); });

                            log.Info($"Sender {sender1} reported {sendTimersPerSecond} timers.");

                            await Task.Delay(0.1.Seconds());
                        }
                    });
            }
        }

        private void RunAggregators(
            string sourceStreamName,
            string targetStreamName,
            Func<IAggregateFunction> aggregateFunction,
            CancellationToken cancellationToken)
        {
            for (var i = 0; i < aggregatorsCount; i++)
            {
                var index = i;
                //var aggregatorSettings = new AggregatorSettings(
                //    sourceStreamName,
                //    targetStreamName,
                //    aggregateFunction,
                //    Hercules.Instance.MetricsStream,
                //    Hercules.Instance.Gate,
                //    leftCoordinatesStorage,
                //    rightCoordinatesStorage,
                //    () => new StreamShardingSettings(index, aggregatorsCount),
                //    new DevNullMetricContext());

                VostokHostingEnvironmentSetup setup = builder => builder
                    .SetupApplicationIdentity(identity => identity.SetProject("Vostok").SetSubproject("Metrics").SetApplication("Test"))
                    .SetupShutdownToken(cancellationToken);

                var aggregator = new AggregatorApplication();

                var host = new VostokHost(
                    new VostokHostSettings(
                        aggregator,
                        setup));

                host.RunAsync();

                aggregators.Add(aggregator);
            }
        }
    }
}