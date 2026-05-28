using RabbitMq.Core.Interfaces;
namespace RabbitMq.Core;

public class SubscriberProperties : ISubscriberProperties
{
    public string Exchange { get; set; }
    public string ExchangeType { get; set; }
    public List<string> Bindings { get; set; } = new List<string>();
}