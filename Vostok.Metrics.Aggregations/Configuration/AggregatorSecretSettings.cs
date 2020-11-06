using JetBrains.Annotations;

namespace Vostok.Metrics.Aggregations.Configuration
{
    [PublicAPI]
    public class AggregatorSecretSettings
    {
        public string HerculesApiKey { get; set; }
    }
}