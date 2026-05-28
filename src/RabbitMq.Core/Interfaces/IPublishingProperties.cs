namespace RabbitMq.Core.Interfaces;

public interface IPublishingProperties
{
    /// <summary>
    /// Rabbit Mq Exchange Name
    /// </summary>
    string Exchange { get; set; }
    string ExchangeType { get; set; }
    string Queue { get; set; }

    /// <summary>
    /// RabbitMq RoutingKey to use
    /// </summary>
    string RoutingKey { get; set; }

    /// <summary>
    /// Rabbit Mq Mandatory Flag
    /// </summary>
    bool EnsureDeliveryToQueue { get; set; }

    /// <summary>
    /// Rabbit Mq Publisher Confirm
    /// </summary>
    bool EnablePublisherConfirm { get; set; }

    /// <summary>
    /// The Default Wait time waiting for Returns
    /// </summary>DeliveryModes
    TimeSpan PublishReturnWaitTime { get; set; }

    // Properties Related to IBasicProperties              
    /// <summary>
    /// CorrelationId
    /// </summary>
    string CorrelationId { get; set; }

    /// <summary>
    /// DeliveryMode
    /// </summary>
    byte DeliveryMode { get; set; }

    /// <summary>
    /// Expiration
    /// </summary>
    string Expiration { get; set; }

    /// <summary>
    /// Headers
    /// </summary>
    IDictionary<string, object> Headers { get; set; }
    /// <summary>
    /// MessageId
    /// </summary>
    string MessageId { get; set; }

    /// <summary>
    /// Persistent
    /// </summary>
    bool Persistent { get; set; }

    /// <summary>
    /// ReplyTo
    /// </summary>
    string ReplyTo { get; set; }

    /// <summary>
    /// UserId
    /// </summary>
    string UserId { get; set; }

    /// <summary>
    /// Message Serializer
    /// </summary>
    //IMessageSerializer Serializer { get; set; }
}
