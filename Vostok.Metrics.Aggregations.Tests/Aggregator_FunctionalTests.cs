using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using FluentAssertions.Extensions;
using JetBrains.Annotations;
using NUnit.Framework;
using Vostok.Commons.Testing;
using Vostok.Hercules.Client.Abstractions.Queries;
using Vostok.Hercules.Consumers;
using Vostok.Logging.Abstractions;
using Vostok.Logging.Console;
using Vostok.Metrics.Aggregations.AggregateFunctions;
using Vostok.Metrics.Hercules;
using Vostok.Metrics.Models;
using Vostok.Metrics.Primitives.Timer;
using Vostok.Metrics.Senders;

namespace Vostok.Metrics.Aggregations.Tests
{
    [TestFixture]
    internal class Aggregator_FunctionalTests
    {
        private readonly ILog log = new SynchronousConsoleLog();

        private readonly TimeSpan period = 1.Seconds();
        private readonly TimeSpan lag = 1.Seconds();

        private readonly int aggregatorsCount = 2;
        private readonly int sendersCount = 4;
        private readonly int sendTimersPerSecond = 10;

        private readonly int expectedGoodIterations = 3;
        private readonly int readIterations = 5;

        private InMemoryCoordinatesStorage aggregatorsCoordinatesStorage;
        private HerculesMetricSenderSettings senderSettings;
        private CancellationTokenSource cancellationTokenSource;

        private IMetricContext metricContext;

        private TestsHelpers.TestMetricSender testMetricSender;
        private List<Aggregator> aggregators;

        [SetUp]
        public void SetUp()
        {
            cancellationTokenSource = new CancellationTokenSource();

            aggregatorsCoordinatesStorage = new InMemoryCoordinatesStorage();

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
                                    Partitions = 2,
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
                new MetricContextConfig(new CompositeMetricEventSender(new IMetricEventSender[] {
                    herculesSender,
                    testMetricSender }))
                {
                    ErrorCallback = error => log.Error(error)
                });

            aggregators = new List<Aggregator>();
        }

        [TearDown]
        public void TearDown()
        {
            cancellationTokenSource.Cancel();
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            Hercules.Instance.Dispose();
        }

        [Test]
        public void Should_aggregate_timers()
        {
            RunAggregators(
                // ReSharper disable AssignNullToNotNullAttribute
                senderSettings.TimersStream,
                senderSettings.FinalStream,
                // ReSharper restore AssignNullToNotNullAttribute
                tags => new TimersAggregateFunction(tags));

            RunSenders();
            
            var aggregatedEvents = new List<MetricEvent>();
            RunConsumer(aggregatedEvents);

            var eventsRecieved = 0L;
            var receivedEvents = new Dictionary<(string, DateTime), int>();
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

            var sentEvents = testMetricSender.Events();

            var maxTimestamp = receivedEvents.Keys.Max(k => k.Item2);
            sentEvents = sentEvents.Where(e => RoundUp(e.Timestamp.UtcDateTime) < maxTimestamp).ToList();
            foreach (var key in receivedEvents.Keys.Where(k => k.Item2 == maxTimestamp).ToList())
                receivedEvents.Remove(key);

            var expectedRecievedEvents = sentEvents
                .GroupBy(e => (e.Tags.Single(t => t.Key == WellKnownTagKeys.Name).Value, RoundUp(e.Timestamp.UtcDateTime)))
                .ToDictionary(g => g.Key, g => g.Count())
                .ToList();

            expectedRecievedEvents.Count.Should().BeGreaterOrEqualTo(expectedGoodIterations);
            receivedEvents.Should().BeEquivalentTo(expectedRecievedEvents);

            Action checkCoordinatesSaving = () => 
                aggregatorsCoordinatesStorage.GetCurrentAsync().Result.Positions.Sum(p => p.Offset).Should().BeGreaterOrEqualTo(eventsRecieved);
            checkCoordinatesSaving.ShouldPassIn(2.Seconds());
        }

        private void RunConsumer(List<MetricEvent> receivedEvents)
        {
#pragma warning disable 4014
            var consumer = new StreamConsumer(
                new StreamConsumerSettings(
                    // ReSharper disable once AssignNullToNotNullAttribute
                    senderSettings.FinalStream,
                    Hercules.Instance.Stream,
                    new AdHocEventsHandler(
                        (coordinates, events, token) =>
                        {
                            lock (receivedEvents)
                            {
                                receivedEvents.AddRange(events.Select(HerculesMetricEventFactory.CreateFrom));
                            }

                            return Task.CompletedTask;
                        }),
                    new InMemoryCoordinatesStorage(),
                    () => new StreamShardingSettings(0, 1)
                ),
                log.ForContext("Consumer"));

            consumer.RunAsync(cancellationTokenSource.Token);
#pragma warning restore 4014
        }

        private void RunSenders()
        {
            for (var sender = 0; sender < sendersCount; sender++)
            {
                var timer = metricContext.CreateTimer($"timer-{sender}");

                var sender1 = sender;
                Task.Run(
                    async () =>
                    {
                        while (!cancellationTokenSource.IsCancellationRequested)
                        {
                            Parallel.For(0, sendTimersPerSecond / 10, t =>
                            {
                                timer.Report(t);
                            });

                            log.Info($"Sender {sender1} reported {sendTimersPerSecond} timers.");
                            
                            await Task.Delay(0.1.Seconds());
                        }
                    });
            }
        }

        private void RunAggregators(
            [NotNull] string sourceStreamName,
            [NotNull] string targetStreamName,
            [NotNull] Func<MetricTags, IAggregateFunction> aggregateFunctionFactory)
        {
            for (var i = 0; i < aggregatorsCount; i++)
            {
                var index = i;
                var aggregatorSettings = new AggregatorSettings(
                    sourceStreamName,
                    targetStreamName,
                    aggregateFunctionFactory,
                    Hercules.Instance.Stream,
                    Hercules.Instance.Gate,
                    aggregatorsCoordinatesStorage,
                    () => new StreamShardingSettings(index, aggregatorsCount)
                )
                {
                    DefaultPeriod = period,
                    DefaultLag = lag
                };

                var aggregator = new Aggregator(aggregatorSettings, log.ForContext($"Aggregator-{i}"));

                aggregator.RunAsync(cancellationTokenSource.Token);

                aggregators.Add(aggregator);
            }
        }

        private static DateTime RoundUp(DateTime date)
        {
            var ticks = 1.Seconds().Ticks;
            return new DateTime((date.Ticks + ticks) / ticks * ticks, date.Kind);
        }
    }
}