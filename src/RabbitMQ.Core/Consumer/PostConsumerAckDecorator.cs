using System;
using System.Threading.Tasks;
using RabbitMQ.Core.Interfaces;
namespace RabbitMQ.Core.Consumer;

public sealed class PostConsumerAckDecorator<TMessage> : IRabbitMQConsumer<TMessage> where TMessage : class
{
    private readonly IRabbitMQConsumer<TMessage> _decorated;

    /// <summary>
    /// Instance of PostConsumerAckDecorator
    /// </summary>
    /// <param name="decorated">Decorated Consumer</param>
    public PostConsumerAckDecorator(IRabbitMQConsumer<TMessage> decorated) => _decorated = decorated;

    /// <inheritdoc/>
    public async Task Consume(IRabbitMQConsumeContext<TMessage> consumeContext)
    {
        await _decorated.Consume(consumeContext);
        if (!consumeContext.AutoAck)
            consumeContext.Ack();
    }
}
