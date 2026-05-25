using RabbitMq.Core.Interfaces;
using RabbitMq.UnitTests.Helpers;
using System.Threading.Tasks;
namespace RabbitMq.UnitTests.Consumer
{
    public class TestMessage1AckNackConsumer : IRabbitMqConsumer
    {
        private TestResultContext _context;
        public TestMessage1AckNackConsumer(TestResultContext context)
        {
            _context = context;
        }
        public async Task Consume(IRabbitMqConsumeContext consumeContext)
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