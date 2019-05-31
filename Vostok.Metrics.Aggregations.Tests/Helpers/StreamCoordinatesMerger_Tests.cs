using FluentAssertions;
using NUnit.Framework;
using Vostok.Hercules.Client.Abstractions.Models;
using Vostok.Metrics.Aggregations.Helpers;

namespace Vostok.Metrics.Aggregations.Tests.Helpers
{
    [TestFixture]
    internal class StreamCoordinatesMerger_Tests
    {
        [Test]
        public void MergeMin_should_works_correctly()
        {
            var a = new StreamCoordinates(
                new[]
                {
                    new StreamPosition {Partition = 0, Offset = 1},
                    new StreamPosition {Partition = 1, Offset = 1},
                    new StreamPosition {Partition = 2, Offset = 3},
                    new StreamPosition {Partition = 3, Offset = 4},
                });

            var b = new StreamCoordinates(
                new[]
                {
                    new StreamPosition {Partition = 0, Offset = 1},
                    new StreamPosition {Partition = 1, Offset = 2},
                    new StreamPosition {Partition = 2, Offset = 2},
                    new StreamPosition {Partition = 5, Offset = 4},
                });

            var min = new StreamCoordinates(
                new[]
                {
                    new StreamPosition {Partition = 0, Offset = 1},
                    new StreamPosition {Partition = 1, Offset = 1},
                    new StreamPosition {Partition = 2, Offset = 2}
                });

            StreamCoordinatesMerger.MergeMin(a, b).Should().BeEquivalentTo(min);
            StreamCoordinatesMerger.MergeMin(b, a).Should().BeEquivalentTo(min);
        }

        [Test]
        public void Distance_should_works_correctly()
        {
            var a = new StreamCoordinates(
                new[]
                {
                    new StreamPosition {Partition = 0, Offset = 1},
                    new StreamPosition {Partition = 1, Offset = 100}
                });

            var b = new StreamCoordinates(
                new[]
                {
                    new StreamPosition {Partition = 1, Offset = 200},
                    new StreamPosition {Partition = 2, Offset = 2}
                });

            StreamCoordinatesMerger.Distance(a, b).Should().Be(100);
            StreamCoordinatesMerger.Distance(b, a).Should().Be(-100);
        }
    }
}