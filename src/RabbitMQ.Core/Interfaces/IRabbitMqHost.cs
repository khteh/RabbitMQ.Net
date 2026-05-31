namespace RabbitMQ.Core.Interfaces;

public interface IRabbitMQHost : IDisposable
{
    /// <summary>
    /// Publish a message on RabbitMQ
    /// </summary>
    /// <typeparam name="TMessage">Message</typeparam>    
    /// <param name="exchange">Exchange to target</param>
    /// <param name="routingKey">RoutingKey to be used</param>
    /// <param name="message">Message to be published on the bus</param>
    /// <param name="configurations">Publishing Properties</param>
    Task Publish<TMessage>(string exchange, string routingKey, TMessage message, Action<IPublishingProperties> configurations) where TMessage : class;

    /// <summary>
    /// Request a response message from RabbitMQ subscriber
    /// </summary>
    /// <typeparam name="TMessage">Request Message</typeparam>   
    /// <typeparam name="TResponse">Response Message</typeparam> 
    /// <param name="exchange">Exchange to target</param>
    /// <param name="routingKey">RoutingKey to be used</param>
    /// <param name="message">Message to be published on the bus</param>         
    /// <param name="configurations">Properties used for publishing</param> 
    Task<TResponse> Request<TMessage, TResponse>(string exchange, string routingKey, TMessage message, Action<IRequestResponseProperties> configurations)
        where TMessage : class
        where TResponse : class;

    /// <summary>
    /// Start the Host
    /// </summary>
    void Start();
}
