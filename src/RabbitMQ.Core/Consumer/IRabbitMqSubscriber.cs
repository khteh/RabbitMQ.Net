using System;
using System.Threading.Tasks;

namespace RabbitMQ.Core.Consumer;

public interface IRabbitMQSubscriber : IDisposable
{
    /// <summary>
    /// Connect to the Channel
    /// </summary>
    Task Connect();
}
public interface IRabbitMQSubscriber<TMessage> : IRabbitMQSubscriber
    where TMessage : class
{
}
