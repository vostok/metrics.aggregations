using System;
using System.Collections.Generic;
using System.Threading;
using FluentAssertions;
using FluentAssertions.Extensions;
using NUnit.Framework;
using Vostok.Hercules.Client.Abstractions;
using Vostok.Hercules.Client.Abstractions.Events;
using Vostok.Hercules.Client.Abstractions.Models;
using Vostok.Hercules.Client.Abstractions.Queries;
using Vostok.Hercules.Consumers;
using Vostok.Hercules.Consumers.Helpers;
using Vostok.Logging.Abstractions;
using Vostok.Logging.Console;
using Vostok.Metrics.Aggregations.Helpers;

namespace Vostok.Metrics.Aggregations.Tests.Helpers
{
    [TestFixture]
    internal class StreamSegmentReader_Tests
    {
        private readonly TimeSpan timeout = 10.Seconds();
        private readonly Random random = new Random();
        private readonly string streamName = "segment";
        private readonly ILog log = new SynchronousConsoleLog();

        private readonly int partitions = 3;
        private readonly int readers = 2;
        
        [Test]
        public void Should_read_segments()
        {
            var management = Hercules.Instance.Management;
            management.CreateStream(
                new CreateStreamQuery(streamName)
                {
                    ShardingKey = new[] {"hash"},
                    Partitions = partitions
                },
                timeout);

            var segments = new List<Segment>();

            for (var times = 0; times < 10; times++)
            {
                var start = GetEndCoordinates();
                var events = GenerateEvents(random.Next(500));
                SendEvents(events);
                var end = GetEndCoordinates();

                StreamCoordinatesMerger.Distance(start, end).Should().Be(events.Count);

                segments.Add(new Segment(start, end, events));
            }

            foreach (var segment in segments)
            {
                CheckSegment(segment);
            }
        }

        private void CheckSegment(Segment segment)
        {
            log.Info($"Checking segment: start: {segment.Start}, end: {segment.End}.");

            var segmentReaderSettings = new StreamSegmentReaderSettings(
                streamName,
                Hercules.Instance.Stream,
                segment.Start,
                segment.End)
            {
                EventsBatchSize = 133,
                EventsReadTimeout = timeout
            };

            var segmentReader = new StreamSegmentReader(segmentReaderSettings, log);

            var result = new List<HerculesEvent>();

            for (var reader = 0; reader < readers; reader++)
            {
                var coordinates = segment.Start;

                while (true)
                {
                    var (query, part) = segmentReader.ReadAsync(coordinates, new StreamShardingSettings(reader, readers), CancellationToken.None).GetAwaiter().GetResult();
                    if (part == null)
                        break;

                    result.AddRange(part.Payload.Events);

                    StreamCoordinatesMerger.Distance(query.Coordinates, part.Payload.Next).Should().Be(part.Payload.Events.Count);

                    coordinates = part.Payload.Next;
                }
            }

            ShouldBeEqual(result, segment.Events);
        }

        private StreamCoordinates GetEndCoordinates()
        {
            var end = Hercules.Instance.Stream.SeekToEnd(new SeekToEndStreamQuery(streamName), timeout);
            return end.Payload.Next;
        }

        private List<HerculesEvent> GenerateEvents(int count)
        {
            var result = new List<HerculesEvent>();
            for (var i = 0; i < count; i++)
            {
                var @event = new HerculesEventBuilder();
                @event.SetTimestamp(DateTimeOffset.Now);
                @event.AddValue("hash", random.Next(10));
                @event.AddValue("id", Guid.NewGuid());
                result.Add(@event.BuildEvent());
            }
            return result;
        }

        private void SendEvents(List<HerculesEvent> events)
        {
            Hercules.Instance.Gate.Insert(new InsertEventsQuery(streamName, events), timeout).EnsureSuccess();
        }

        private static void ShouldBeEqual(IEnumerable<HerculesEvent> actualEvents, IEnumerable<HerculesEvent> expectedEvents)
        {
            // FluentAssertions is slow on large sequences
            new HashSet<HerculesEvent>(actualEvents)
                .SetEquals(expectedEvents)
                .Should()
                .BeTrue();
        }

        private class Segment
        {
            public readonly List<HerculesEvent> Events;
            public readonly StreamCoordinates Start, End;

            public Segment(StreamCoordinates start, StreamCoordinates end, List<HerculesEvent> events)
            {
                Start = start;
                End = end;
                Events = events;
            }
        }
    }
}