using System.Threading.Tasks;
using RabbitMQ.Core.Consumer;
using RabbitMQ.UnitTests.Helpers;
namespace RabbitMQ.UnitTests.Consumer;

public class TestRequestResponseServerConsumer : IRabbitMQConsumer
{
    private TestResultContext _context;
    public TestRequestResponseServerConsumer(TestResultContext context) => _context = context;
    public async Task Consume(IRabbitMQConsumeContext consumeContext)
    {
        await consumeContext.Respond<TestResponse>(new TestResponse() { Title = consumeContext.Message.Title + "Response" });
        _context.IncrementSuccessCount();
        _context.SetCompleted();
    }
}
