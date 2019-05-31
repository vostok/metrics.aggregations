using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Vostok.Hercules.Client.Abstractions.Models;

namespace Vostok.Metrics.Aggregations.Helpers
{
    internal static class StreamCoordinatesMerger
    {
        [NotNull]
        public static StreamCoordinates MergeMin([NotNull] StreamCoordinates leftCoordinates, [NotNull] StreamCoordinates rightCoordinates)
        {
            var left = leftCoordinates.ToDictionary();
            var right = rightCoordinates.ToDictionary();

            var merged = new Dictionary<int, StreamPosition>();

            foreach (var key in left.Keys)
            {
                if (right.ContainsKey(key))
                {
                    merged[key] = new StreamPosition
                    {
                        Partition = key,
                        Offset = Math.Min(left[key].Offset, right[key].Offset)
                    };
                }
            }

            return new StreamCoordinates(merged.Values.ToArray());
        }

        public static long Distance([NotNull] StreamCoordinates fromCoordinates, [NotNull] StreamCoordinates toCoordinates)
        {
            var from = fromCoordinates.ToDictionary();
            var to = toCoordinates.ToDictionary();

            long result = 0;

            foreach (var key in from.Keys)
            {
                if (to.ContainsKey(key))
                {
                    result += to[key].Offset - from[key].Offset;
                }
            }

            return result;
        }
    }
}