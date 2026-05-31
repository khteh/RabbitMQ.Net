using RabbitMQ.Core.Configuration;
using RabbitMQ.Core.Interfaces;
namespace RabbitMQ.Core;

public class QueueProperties : IQueueProperties
{
    public bool Temporary { get; set; }
    public bool Durable { get; set; }
    public bool Exclusive { get; set; }
    public bool AutoDelete { get; set; }
    public string Name { get; set; }
    public QueueProperties() { }
    public QueueProperties(RabbitMQQueueConfig config)
    {
        Temporary = config.Temporary;
        Durable = config.Durable;
        Exclusive = config.Exclusive;
        AutoDelete = config.AutoDelete;
        Name = config.Name;
    }
}
