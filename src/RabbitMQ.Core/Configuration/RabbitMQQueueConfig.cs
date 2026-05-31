namespace RabbitMQ.Core.Configuration;

public sealed class RabbitMQQueueConfig
{
    public bool Temporary { get; set; }
    public bool Durable { get; set; }
    public bool Exclusive { get; set; }
    public bool AutoDelete { get; set; }
    public string Name { get; set; }
}