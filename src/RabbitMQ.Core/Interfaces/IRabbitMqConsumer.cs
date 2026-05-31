namespace RabbitMQ.Core.Interfaces;

public interface IRabbitMQConsumer<TMessage> where TMessage : class
{
    /// <summary>
    /// Method which consumes the Message from RabbitMQ
    /// </summary>
    Task Consume(IRabbitMQConsumeContext<TMessage> consumeContext);
}
