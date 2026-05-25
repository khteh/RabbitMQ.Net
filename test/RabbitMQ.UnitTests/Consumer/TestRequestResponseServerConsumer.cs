using System.Threading.Tasks;
using RabbitMq.Core.Consumer;
using RabbitMq.UnitTests.Helpers;
namespace RabbitMq.UnitTests.Consumer
{
    public class TestRequestResponseServerConsumer : IRabbitMqConsumer
    {
        private TestResultContext _context;
        public TestRequestResponseServerConsumer(TestResultContext context)
        {
            _context = context;
        }
        public async Task Consume(IRabbitMqConsumeContext consumeContext)
        {
            await consumeContext.Respond<TestResponse>(new TestResponse() { Title = consumeContext.Message.Title + "Response" });
            _context.IncrementSuccessCount();
            _context.SetCompleted();
        }
    }
}