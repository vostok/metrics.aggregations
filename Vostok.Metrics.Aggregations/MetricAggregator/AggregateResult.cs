using System.Collections.Generic;
using Vostok.Hercules.Client.Abstractions.Models;
using Vostok.Hercules.Consumers.Helpers;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.MetricAggregator
{
    internal class AggregateResult
    {
        public readonly List<MetricEvent> AggregatedEvents = new List<MetricEvent>();

        public StreamCoordinates FirstActiveEventCoordinates;

        public long ActiveEventsCount;

        public long ActiveWindowsCount;

        public void AddActiveWindow(Window window)
        {
            ActiveWindowsCount++;
            ActiveEventsCount += window.Count;
            AddActiveCoordinates(window.FirstEventCoordinates);
        }

        public void AddAggregateResult(AggregateResult other)
        {
            AggregatedEvents.AddRange(other.AggregatedEvents);
            AddActiveCoordinates(other.FirstActiveEventCoordinates);
            ActiveEventsCount += other.ActiveEventsCount;
            ActiveWindowsCount += other.ActiveWindowsCount;
        }

        private void AddActiveCoordinates(StreamCoordinates coordinates)
        {
            if (FirstActiveEventCoordinates == null)
                FirstActiveEventCoordinates = coordinates;
            else if (coordinates != null)
                FirstActiveEventCoordinates = StreamCoordinatesMerger.MergeMin(FirstActiveEventCoordinates, coordinates);
        }
    }
}