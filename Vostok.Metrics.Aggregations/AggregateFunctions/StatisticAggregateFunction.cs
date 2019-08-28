using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using JetBrains.Annotations;
using Vostok.Commons.Time;
using Vostok.Metrics.Models;

namespace Vostok.Metrics.Aggregations.AggregateFunctions
{
    [PublicAPI]
    public class StatisticAggregateFunction : IAggregateFunction
    {
        private MetricEvent lastEvent;
        
        private readonly Tree tree = new Tree();
        private readonly DateTimeOffset from;
        private readonly DateTimeOffset to;
        private readonly StreamWriter writer;

        public StatisticAggregateFunction(DateTimeOffset @from, DateTimeOffset to, StreamWriter writer)
        {
            this.@from = @from;
            this.to = to;
            this.writer = writer;
        }

        public void AddEvent(MetricEvent @event)
        {
            lastEvent = @event;

            if (@from <= @event.Timestamp && @event.Timestamp < to)
                tree.Increment(@event.Tags.ToList());

            Print();
        }

        public IEnumerable<MetricEvent> Aggregate(DateTimeOffset timestamp)
        {
            return new MetricEvent[0];
        }

        public void Print()
        {
            if (lastEvent.Timestamp - 1.Minutes() < to)
                return;

            tree.Print(writer);

            writer.Close();

            Environment.Exit(0);
        }

        private class Tree
        {
            private readonly Dictionary<MetricTag, Tree> edges = new Dictionary<MetricTag, Tree>();
            public int Count;

            public void Increment(List<MetricTag> tags, int position = 0)
            {
                Count++;
                
                if (position == tags.Count)
                {
                    return;
                }

                var tag = tags[position];
                if (!edges.ContainsKey(tag))
                    edges[tag] = new Tree();

                edges[tag].Increment(tags, position + 1);
            }

            public void Print(StreamWriter writer, int position = 0)
            {
                //if (position > 2)
                //    return;

                foreach (var edge in edges.OrderByDescending(e => e.Value.Count))
                {
                    writer.Write(new string('\t', position));
                    writer.Write(edge.Value.Count);
                    writer.Write("\t");
                    writer.Write(edge.Key);
                    writer.WriteLine();

                    edge.Value.Print(writer, position + 1);
                }
            }
        }
    }
}