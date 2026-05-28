namespace RabbitMq.Core.Interfaces;

public interface IRequestResponseProperties : IPublishingProperties
{
    /// <summary>
    /// Reply Wait Time
    /// </summary>
    TimeSpan ReplyWaitTime { get; set; }
}
