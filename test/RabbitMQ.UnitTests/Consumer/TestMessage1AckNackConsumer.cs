using RabbitMQ.Core.Interfaces;
using RabbitMQ.UnitTests.Helpers;
using System.Threading.Tasks;
namespace RabbitMQ.UnitTests.Consumer
{
    public class TestMessage1AckNackConsumer : IRabbitMQConsumer
    {
        private TestResultContext _context;
        public TestMessage1AckNackConsumer(TestResultContext context)
        {
            _context = context;
        }
        public async Task Consume(IRabbitMQConsumeContext consumeContext)
        {
            try
            {
                if (consumeContext.RequiresAck)
                    consumeContext.Ack();

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