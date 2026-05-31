namespace RabbitMq.Core.Configuration;

public sealed class RabbitMQChannelConfiguration
{
    public bool PublisherConfirmationsEnabled { get; set; }
    public bool PublisherConfirmationTrackingEnabled { get; set; }
    public int MaxOutstandingConfirmation { get; set; }
}