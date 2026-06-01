using System;
using RabbitMQ.Core.Interfaces;

namespace RabbitMQ.Core.Consumer;

public class RabbitMQMessageConsumeContext<TMessage> :
    RabbitMQConsumeContextProxy,
    IRabbitMQConsumeContext<TMessage>
    where TMessage : class
{
    private TMessage _message;

    /// <summary>
    /// Creates an instance of the RabbitMQMessageConsumeContext
    /// </summary>
    /// <param name="context">Generic RabbitMQ Consume Context</param>
    /// <param name="message">Message</param>
    public RabbitMQMessageConsumeContext(IRabbitMQConsumeContext context, TMessage message)
        : base(context) => _message = message;

    /// <inheritdoc/>
    public TMessage Message => _message;
}
