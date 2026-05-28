namespace RabbitMq.Core.Interfaces;

public interface IQueueProperties
{
    bool Temporary { get; set; }
    bool Durable { get; set; }
    bool Exclusive { get; set; }
    bool AutoDelete { get; set; }
    string Name { get; set; }
}
