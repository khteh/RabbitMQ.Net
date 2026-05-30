using System;
using RabbitMq.Core.Interfaces;

namespace RabbitMq.Core.Consumer;

public class RabbitMqMessageConsumeContext<TMessage> :
    RabbitMqConsumeContextProxy,
    IRabbitMqConsumeContext<TMessage>
    where TMessage : class
{
    private TMessage _message;

    /// <summary>
    /// Creates an instance of the RabbitMqMessageConsumeContext
    /// </summary>
    /// <param name="context">Generic RabbitMq Consume Context</param>
    /// <param name="message">Message</param>
    public RabbitMqMessageConsumeContext(IRabbitMqConsumeContext context, TMessage message)
        : base(context) => _message = message;

    /// <inheritdoc/>
    public TMessage Message => _message;
}
