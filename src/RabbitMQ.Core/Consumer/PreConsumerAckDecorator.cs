using System;
using System.Threading.Tasks;
using RabbitMQ.Core.Interfaces;
namespace RabbitMQ.Core.Consumer;

public sealed class PreConsumerAckDecorator<TMessage> : IRabbitMQConsumer<TMessage> where TMessage : class
{
    private readonly IRabbitMQConsumer<TMessage> _decorated;

    /// <summary>
    /// Instance of PreConsumerAckDecorator
    /// </summary>
    /// <param name="decorated">Decorated Consumer</param>
    public PreConsumerAckDecorator(IRabbitMQConsumer<TMessage> decorated) => _decorated = decorated;

    /// <inheritdoc/>
    public async Task Consume(IRabbitMQConsumeContext<TMessage> consumeContext)
    {
        if (!consumeContext.AutoAck)
            consumeContext.Ack();
        await _decorated.Consume(consumeContext);
    }
}
