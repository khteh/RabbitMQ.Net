using System.Threading.Tasks;
using RabbitMq.Core.Consumer;
using RabbitMq.UnitTests.Helpers;
namespace RabbitMq.UnitTests.Consumer
{
    public class TestMessage1Consumer : IRabbitMqConsumer
    {
        private TestResultContext _context;
        public TestMessage1Consumer(TestResultContext context)
        {
            _context = context;
        }
        public async Task Consume(IRabbitMqConsumeContext consumeContext)
        {
            try
            {
                // consumeContext.Nack(false);
                await Task.Run(() => { });
                _context.IncrementSuccessCount();
            }
            finally
            {
                _context.SetCompleted();
            }
        }
    }
}
