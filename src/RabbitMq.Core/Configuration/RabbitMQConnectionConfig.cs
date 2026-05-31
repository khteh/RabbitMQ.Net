namespace RabbitMq.Core.Configuration;

public sealed class RabbitMQConnectionConfig
{
    public string ConnectionName { get; set; }
    public string Endpoint { get; set; }
    public int Port { get; set; }
    public string UserName { get; set; }
    public string Password { get; set; }
    public string VHost { get; set; }
    public string Exchange { get; set; }
    public string Bindings { get; set; }
    public string RoutingKey { get; set; }
}
