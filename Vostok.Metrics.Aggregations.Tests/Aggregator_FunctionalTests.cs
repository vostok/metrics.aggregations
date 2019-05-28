using System;
using System.Collections.Generic;
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

namespace Vostok.Metrics.Aggregations.Tests
{
    [TestFixture]
    internal class Aggregator_FunctionalTests
    {
        private readonly ILog log = new SynchronousConsoleLog();
        private readonly TimeSpan period = 1.Seconds();
        private readonly TimeSpan lag = 1.Seconds();
        private readonly int aggregatorsCount = 2;
        private readonly int sendersCount = 10;
        private readonly int sendTimers = 100;
        private HerculesMetricSenderSettings senderSettings;
        private CancellationTokenSource cancellationTokenSource;
        
        [SetUp]
        public void SetUp()
        {
            cancellationTokenSource = new CancellationTokenSource();

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
            var herculesSender = new HerculesMetricSender(new HerculesMetricSenderSettings(Hercules.Instance.Sink));

            var context = new MetricContext(
                new MetricContextConfig(herculesSender)
                {
                    ErrorCallback = error => log.Error(error)
                }) as IMetricContext;

            RunAggregators(
                // ReSharper disable AssignNullToNotNullAttribute
                senderSettings.TimersStream,
                senderSettings.FinalStream,
                // ReSharper restore AssignNullToNotNullAttribute
                tags => new TimersAggregateFunction(tags));

            RunSenders(context);
            
            var aggregatedEvents = new List<MetricEvent>();

#pragma warning disable 4014
            new StreamConsumer(
                new StreamConsumerSettings(
                    senderSettings.FinalStream,
                    Hercules.Instance.Stream,
                    new AdHocEventsHandler(
                        (coordinates, events, token) =>
                        {
                            lock (aggregatedEvents)
                            {
                                aggregatedEvents.AddRange(events.Select(HerculesMetricEventFactory.CreateFrom));
                            }

                            return Task.CompletedTask;
                        }),
                    new InMemoryCoordinatesStorage(),
                    () => new StreamShardingSettings(0, 1)
                ),
                log.ForContext("Checker")).RunAsync(cancellationTokenSource.Token);
#pragma warning restore 4014

            var goodBatches = new HashSet<string>();
            var passedIterations = 0;

            Action assertion = () =>
            {
                List<MetricEvent> countMetrics;
                lock (aggregatedEvents)
                {
                    countMetrics = aggregatedEvents.Where(e => e.Tags.Any(t => t.Value == WellKnownTagValues.AggregateCount)).ToList();
                    aggregatedEvents.Clear();
                }

                foreach (var metric in countMetrics)
                {
                    var name = metric.Tags.Single(t => t.Key == WellKnownTagKeys.Name).Value;
                    log.Info($"Recieved aggregated metric {name} with count {metric.Value} timestamp {metric.Timestamp}.");
                    if (metric.Value >= 9*sendTimers)
                        goodBatches.Add(name);
                }

                if (goodBatches.Count == sendersCount)
                    passedIterations++;
                else
                    passedIterations = 0;

                passedIterations.Should().Be(3);
            };

            assertion.ShouldPassIn(10.Seconds(), 1.Seconds());
        }

        private void RunSenders(IMetricContext context)
        {
            for (var sender = 0; sender < sendersCount; sender++)
            {
                var timer = context.CreateTimer($"timer-{sender}");

                var sender1 = sender;
                Task.Run(
                    async () =>
                    {
                        while (!cancellationTokenSource.IsCancellationRequested)
                        {
                            Parallel.For(0, sendTimers, t =>
                            {
                                timer.Report(t);
                            });

                            log.Info($"Sender {sender1} reported {sendTimers} timers.");
                            
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
                    new InMemoryCoordinatesStorage(),
                    () => new StreamShardingSettings(index, aggregatorsCount)
                )
                {
                    DefaultPeriod = period,
                    DefaultLag = lag
                };

                var aggregator = new Aggregator(aggregatorSettings, log.ForContext($"Aggregator-{i}"));

                aggregator.RunAsync(cancellationTokenSource.Token);
            }
        }
    }
}