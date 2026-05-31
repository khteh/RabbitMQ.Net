using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using RabbitMQ.Core.Interfaces;

namespace RabbitMQ.Core;

/// <summary>
/// https://www.rabbitmq.com/dotnet-api-guide.html
/// </summary>
public class PublishingProperties : IPublishingProperties
{
    /// <summary>
    /// Creates instance of Publishing properties
    /// </summary>
    public PublishingProperties()
    {
        DeliveryMode = 2; // persistent
        EnsureDeliveryToQueue = true;
        EnablePublisherConfirm = true;
        PublishReturnWaitTime = new TimeSpan(0, 0, 0, 0, 2000);
        Headers = new ConcurrentDictionary<string, object>();
        //Serializer = new JsonMessageSerializer();
    }

    /// <summary>
    /// Rabbit Mq Exchange Name
    /// </summary>
    public string Exchange { get; set; }
    public string ExchangeType { get; set; }
    public string Queue { get; set; }
    /// <summary>
    /// RabbitMQ RoutingKey to use
    /// </summary>
    public string RoutingKey { get; set; }
    /// <summary>
    /// Rabbit Mq Mandatory Flag
    /// </summary>
    public bool EnsureDeliveryToQueue { get; set; }

    /// <summary>
    /// Rabbit Mq Publisher Confirm
    /// </summary>
    public bool EnablePublisherConfirm { get; set; }

    /// <summary>
    /// The Default Wait time waiting for Returns
    /// </summary>
    public TimeSpan PublishReturnWaitTime { get; set; }

    // Properties Related to IBasicProperties       

    /// <summary>
    /// CorrelationId
    /// </summary>
    public string CorrelationId { get; set; }

    /// <summary>
    /// DeliveryMode
    /// </summary>
    public byte DeliveryMode { get; set; }

    /// <summary>
    /// Expiration
    /// </summary>
    public string Expiration { get; set; }

    /// <summary>
    /// Headers
    /// </summary>
    public IDictionary<string, object> Headers { get; set; }
    /// <summary>
    /// MessageId
    /// </summary>
    public string MessageId { get; set; }

    /// <summary>
    /// Persistent
    /// </summary>
    public bool Persistent { get; set; }

    /// <summary>
    /// ReplyTo
    /// </summary>
    public string ReplyTo { get; set; }

    /// <summary>
    /// UserId
    /// </summary>
    public string UserId { get; set; }

    /// <summary>
    /// Message Serializer
    /// </summary>
    //public IMessageSerializer Serializer { get; set; }
}
