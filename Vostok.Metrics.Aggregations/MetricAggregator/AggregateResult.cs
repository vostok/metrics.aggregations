using System.Collections.Generic;
using Vostok.Hercules.Client.Abstractions.Models;
using Vostok.Metrics.Aggregations.Helpers;
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
            ActiveEventsCount += window.EventsCount;
            AddActiveCoordinates(window.FirstEventCoordinates);
        }

        public void AddAggregateResult(AggregateResult other)
        {
            AggregatedEvents.AddRange(other.AggregatedEvents);
            AddActiveCoordinates(other.FirstActiveEventCoordinates);
            ActiveEventsCount += other.ActiveEventsCount;
            ActiveWindowsCount += other.ActiveWindowsCount;
        }

        public void AddActiveCoordinates(StreamCoordinates coordinates)
        {
            FirstActiveEventCoordinates = FirstActiveEventCoordinates == null 
                ? coordinates 
                : StreamCoordinatesMerger.MergeMin(FirstActiveEventCoordinates, coordinates);
        }
    }
}