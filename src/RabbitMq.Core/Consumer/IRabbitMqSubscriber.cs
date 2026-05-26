using System;
using System.Threading.Tasks;

namespace RabbitMq.Core.Consumer;

public interface IRabbitMqSubscriber : IDisposable
{
    /// <summary>
    /// Connect to the Channel
    /// </summary>
    Task Connect();
}
public interface IRabbitMqSubscriber<TMessage> : IRabbitMqSubscriber
    where TMessage : class
{
}
