using RabbitMq.Core.Events;
using RabbitMQ.Client.Events;
namespace RabbitMq.Core.Interfaces;

public interface IRabbitMqChannel : IDisposable
{
    /// <summary>
    ///  Unique identifier of the channel
    /// </summary>
    string Id { get; }

    /// <summary>
    /// Send an Ack delivery confirmation to Rabbit Mq
    /// </summary>
    /// <param name="deliveryTag">delivery Tag</param>
    /// <param name="ackMultipleMessages">Multiple or single messages</param>
    Task Ack(ulong deliveryTag, bool ackMultipleMessages);

    /// <summary>
    /// Send an Nack delivery confirmation to Rabbit Mq
    /// </summary>
    /// <param name="deliveryTag">delivery Tag</param>
    /// <param name="nackMultipleMessages">Multiple or single messages</param>
    /// <param name="reQueue">should it be requeued</param>
    Task Nack(ulong deliveryTag, bool nackMultipleMessages, bool reQueue);

    /// <summary>
    /// Publish a message on RabbitMq
    /// </summary>
    /// <typeparam name="TMessage">Message</typeparam>    
    /// <param name="exchange">Exchange to target</param>
    /// <param name="routingKey">RoutingKey to be used</param>
    /// <param name="message">Message to be published on the bus</param>        
    /// <param name="configuration">Publishing configurations</param>
    Task<PublishResult> Publish<TMessage>(string exchange, string routingKey, TMessage message, Action<IPublishingProperties> configuration)
        where TMessage : class;

    Task<PublishResult> Publish<TMessage>(TMessage message, IPublishingProperties properties) where TMessage : class;
    /// <summary>
    /// Request a response message from RabbitMq subscriber
    /// </summary>
    /// <typeparam name="TMessage">Request Message</typeparam>   
    /// <typeparam name="TResponse">Response Message</typeparam> 
    /// <param name="exchange">Exchange to target</param>
    /// <param name="routingKey">RoutingKey to be used</param>
    /// <param name="message">Message to be published on the bus</param>        
    /// <param name="configuration">Publishing Configuration on the bus</param>
    Task<TResponse> Request<TMessage, TResponse>(string exchange, string routingKey, TMessage message, Action<IRequestResponseProperties> configuration)
        where TMessage : class
        where TResponse : class;


    /// <summary>
    /// Publish a response message on RabbitMq
    /// </summary>
    /// <typeparam name="TResponse">Message</typeparam>    
    /// <param name="consumeArgument">Rabbit Mq Consumer Argument</param>    
    /// <param name="response">Message to be published on the bus</param>        
    /// <param name="configuration">Message Request Response Configuration</param>
    Task Respond<TResponse>(BasicDeliverEventArgs consumeArgument, TResponse response, Action<IPublishingProperties> configuration)
        where TResponse : class;

    /// <summary>
    /// Subscribe to a message on the channel
    /// </summary>
    /// <param name="queueName">Queue Name</param>
    /// <param name="autoAck">auto ACK</param>
    /// <param name="onConsume">Consume Handler</param>
    /// <param name="onDisconnected">Consumer Disconnection handler</param>
    /// <returns>Consumer Tag</returns>
    Task<string> Subscribe(bool autoAck, AsyncEventHandler<BasicDeliverEventArgs> onConsume, AsyncEventHandler<RabbitMqSubscriberDisconnectedEventArgs> onDisconnected, List<string> bindings);

    /// <summary>
    /// Provides callbacks to consumer when the connection is down
    /// </summary>
    event AsyncEventHandler<RabbitMqChannelDisconnectedEventArgs> Disconnected;
}