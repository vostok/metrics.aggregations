using NUnit.Framework;
using Vostok.Commons.Threading;

namespace Vostok.Metrics.Aggregations.Tests
{
    [SetUpFixture]
    internal class SetUpFixture
    {
        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            ThreadPoolUtility.Setup();
        }
    }
}