using System;
using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using FluentAssertions.Extensions;
using NUnit.Framework;
using Vostok.Hercules.Client.Abstractions;
using Vostok.Hercules.Client.Abstractions.Queries;
using Vostok.Metrics.Hercules;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.Tests
{
    [TestFixture]
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
                    ShardingKey = new[] { "hash" },
                    Partitions = 1
                },
                timeout);

            var events = new List<MetricEvent>();

            for (var i = 0; i < 100; i++)
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

                events.Add(@event);

            }

            Hercules.Instance.Gate.Insert(new InsertEventsQuery(streamName, events.Select(HerculesEventMetricBuilder.Build).ToList()), timeout).EnsureSuccess();

            var actual = Hercules.Instance.MetricsStream.Read(new ReadStreamQuery(streamName), timeout).Payload.Events;

            actual.Should().BeEquivalentTo(events);
        }
    }
}