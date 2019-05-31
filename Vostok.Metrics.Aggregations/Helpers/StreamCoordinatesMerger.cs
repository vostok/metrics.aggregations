using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Vostok.Hercules.Client.Abstractions.Models;

namespace Vostok.Metrics.Aggregations.Helpers
{
    internal static class StreamCoordinatesMerger
    {
        [NotNull]
        public static StreamCoordinates MergeMin([NotNull] StreamCoordinates left, [NotNull] StreamCoordinates right)
        {
            var map = left.ToDictionary();
            var result = new List<StreamPosition>();

            foreach (var position in right.Positions)
            {
                if (map.TryGetValue(position.Partition, out var p))
                {
                    result.Add(new StreamPosition
                    {
                        Offset = Math.Min(position.Offset, p.Offset),
                        Partition = position.Partition
                    });
                }
            }

            return new StreamCoordinates(result.ToArray());
        }

        public static long Distance([NotNull] StreamCoordinates from, [NotNull] StreamCoordinates to)
        {
            var map = from.ToDictionary();
            var result = 0L;

            foreach (var position in to.Positions)
            {
                if (map.TryGetValue(position.Partition, out var p))
                {
                    result += position.Offset - p.Offset;
                }
            }

            return result;
        }
    }
}