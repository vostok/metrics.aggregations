using System.Linq;
using Vostok.Hercules.Client.Abstractions.Models;

namespace Vostok.Metrics.Aggregations.Helpers
{
    internal static class StreamCoordinatesExtensions
    {
        public static StreamCoordinates SetPosition(this StreamCoordinates coordinates, StreamPosition position)
        {
            var dict = coordinates.ToDictionary();
            dict[position.Partition] = position;
            return new StreamCoordinates(dict.Values.ToArray());
        }
    }
}