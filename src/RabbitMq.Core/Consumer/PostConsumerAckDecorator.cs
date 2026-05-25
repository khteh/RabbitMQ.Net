using System;
using System.Threading.Tasks;
using RabbitMq.Core.Interfaces;

namespace RabbitMq.Core.Consumer
{
    public sealed class PostConsumerAckDecorator<TMessage> : IRabbitMqConsumer<TMessage> where TMessage : class
    {
        private readonly IRabbitMqConsumer<TMessage> _decorated;

        /// <summary>
        /// Instance of PreConsumerAckDecorator
        /// </summary>
        /// <param name="decorated">Decorated Consumer</param>
        public PostConsumerAckDecorator(IRabbitMqConsumer<TMessage> decorated)
        {
            _decorated = decorated;
        }

        /// <inheritdoc/>
        public async Task Consume(IRabbitMqConsumeContext<TMessage> consumeContext)
        {
            await _decorated.Consume(consumeContext);
            if (!consumeContext.AutoAck)
                consumeContext.Ack();
        }
    }
}