using System;
using RabbitMQ.Core.Interfaces;
namespace RabbitMQ.Core;

public class RequestResponseProperties : PublishingProperties, IRequestResponseProperties
{
    /// <summary>
    /// Creates instance of Publishing properties
    /// </summary>
    public RequestResponseProperties() : base() => ReplyWaitTime = new TimeSpan(0, 0, 0, 30, 0);
    /// <inheritdoc/>
    public TimeSpan ReplyWaitTime { get; set; }
}