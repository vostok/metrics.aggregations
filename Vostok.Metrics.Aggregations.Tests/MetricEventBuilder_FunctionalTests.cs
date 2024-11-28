using System;
using System.Collections.Generic;
using FluentAssertions;
using FluentAssertions.Extensions;
using NUnit.Framework;
using Vostok.Hercules.Client.Abstractions;
using Vostok.Hercules.Client.Abstractions.Events;
using Vostok.Hercules.Client.Abstractions.Queries;
using Vostok.Metrics.Hercules;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.Tests
{
    [TestFixture]
    [Explicit]
    internal class MetricEventBuilder_FunctionalTests
    {
        private readonly TimeSpan timeout = 10.Seconds();
        private readonly string streamName = "serialization";

        [Test]
        public void Should_correctly_build_events_from_buffer()
        {
            var management = Hercules.Instance.Management;
            management.CreateStream(
                new CreateStreamQuery(streamName)
                {
                    ShardingKey = new[] {"hash"},
                    Partitions = 1
                },
                timeout);

            var events = new List<HerculesEvent>();
            var random = new Random();

            for (var i = 0; i < 1000; i++)
            {
                var tags = MetricTags.Empty
                    .Append("common", "value")
                    .Append("id", (i % 10).ToString());

                var @event = new MetricEvent(
                    i,
                    tags,
                    DateTimeOffset.UtcNow,
                    WellKnownUnits.Milliseconds,
                    WellKnownAggregationTypes.Counter,
                    new Dictionary<string, string>
                    {
                        ["common-param"] = "param",
                        ["id-param"] = (i % 10).ToString()
                    });

                var builder = new HerculesEventBuilder();
                HerculesEventMetricBuilder.Build(@event, builder);

                CorruptEvent(random, builder);

                events.Add(builder.BuildEvent());
            }

            Hercules.Instance.Gate.Insert(new InsertEventsQuery(streamName, events), timeout).EnsureSuccess();

            var actual = Hercules.Instance.MetricsStream.Read(new ReadStreamQuery(streamName), timeout).Payload.Events;

            var expected = new List<MetricEvent>();
            foreach (var @event in events)
            {
                try
                {
                    expected.Add(HerculesMetricEventFactory.CreateFrom(@event));
                }
                // ReSharper disable once EmptyGeneralCatchClause
                catch (Exception)
                {
                }
            }

            actual.Should().BeEquivalentTo(expected);
        }

        private static void CorruptEvent(Random random, HerculesEventBuilder builder)
        {
            if (random.Next(10) == 0)
            {
                builder.RemoveTags("tags");
            }

            if (random.Next(10) == 0)
            {
                builder.AddContainer("a", b => b.AddContainer("b", bb => bb.AddValue("value", "str")));
            }
        }
    }
}