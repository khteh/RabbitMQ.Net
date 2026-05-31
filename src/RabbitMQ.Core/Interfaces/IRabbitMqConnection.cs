using RabbitMQ.Core.Events;
using RabbitMQ.Client;
namespace RabbitMQ.Core.Interfaces;

public interface IRabbitMQConnection : IDisposable
{
    /// <summary>
    /// Creates a Rabbit Mq Channel
    /// </summary>
    /// <returns>Rabbit Mq Channel</returns>
    Task<IRabbitMQChannel> CreateChannel(string exchange, string type, string routingKey, IQueueProperties properties);

    /// <summary>
    /// Connects to the RabbitMQ Endpoint
    /// </summary>
    Task<IConnection> Start();

    /// <summary>
    /// Shows the state of the connection
    /// </summary>
    bool IsConnected();

    /// <summary>
    /// Provides callbacks to consumer when the connection is estabilished
    /// This would mean recreating channels for communication
    /// </summary>
    event EventHandler<RabbitMQConnectedEventArgs> Connected;

    /// <summary>
    /// Provides callbacks to consumers when the connection is down
    /// This would mean discarding the existing channels in use
    /// </summary>
    event EventHandler<RabbitMQDisconnectedEventArgs> Disconnected;
}
