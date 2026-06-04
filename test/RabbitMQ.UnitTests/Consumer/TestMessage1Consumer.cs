using System.Threading.Tasks;
using RabbitMQ.Core.Consumer;
using RabbitMQ.UnitTests.Helpers;
namespace RabbitMQ.UnitTests.Consumer;

public class TestMessage1Consumer : IRabbitMQConsumer
{
    private TestResultContext _context;
    public TestMessage1Consumer(TestResultContext context) => _context = context;
    public async Task Consume(IRabbitMQConsumeContext consumeContext)
    {
        try
        {
            // consumeContext.Nack(false);
            _context.IncrementSuccessCount();
        }
        finally
        {
            _context.SetCompleted();
        }
    }
}
