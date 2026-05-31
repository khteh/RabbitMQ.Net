namespace RabbitMQ.Core.Configuration;

public sealed class RabbitMQChannelConfig
{
    public bool PublisherConfirmationsEnabled { get; set; }
    public bool PublisherConfirmationTrackingEnabled { get; set; }
    public int MaxOutstandingConfirmation { get; set; }
}