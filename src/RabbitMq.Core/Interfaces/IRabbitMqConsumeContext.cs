namespace RabbitMq.Core.Interfaces;

public interface IRabbitMqConsumeContext
{
    /// <summary>
    /// Basic properties
    /// </summary>
    //IBasicProperties BasicProperties { get; }       

    /// <summary>
    /// Indicates if the Ack/Nack is auto
    /// </summary>
    bool AutoAck { get; }

    /// <summary>
    /// The consumer tag of the consumer that the message was delivered to
    /// </summary>
    string ConsumerTag { get; }

    /// <summary>
    /// The exchange the message was originally published to
    /// </summary>
    string Exchange { get; }

    /// <summary>
    /// The routing key used when the message was originally published
    /// </summary>
    string RoutingKey { get; }

    /// <summary>
    /// The queue name the message was received from
    /// </summary>
    string QueueName { get; }

    /// <summary>
    /// Respond to Rabbit Mq that the message is processed successfully
    /// </summary>
    void Ack();

    /// <summary>
    /// Respond to Rabbit Mq that either the message was malformed , there was error in processing, etc
    /// </summary>
    /// <param name="reQueue">If its a temperory error then, we meed to put it on the queue again</param>
    void Nack(bool reQueue);


    /// <summary>
    /// Respond to the a Request Message via RPC call
    /// </summary>
    /// <typeparam name="TResponse">Response Type</typeparam>
    /// <param name="response">Response</param>
    Task Respond<TResponse>(TResponse response)
       where TResponse : class;

    /// <summary>
    ///     Returns the specified message type if available, otherwise returns false
    /// </summary>
    /// <typeparam name="TMessage">System Message Type</typeparam>
    /// <returns></returns>
    IRabbitMqConsumeContext<TMessage> GetConsumeContext<TMessage>()
        where TMessage : class;
}

/// <summary>
/// Types Consume ontext
/// </summary>
/// <typeparam name="TMessage">Type of Message</typeparam>
public interface IRabbitMqConsumeContext<TMessage> : IRabbitMqConsumeContext
{
    /// <summary>
    /// Message that comes as part of the message
    /// </summary>
    TMessage Message { get; }
}
