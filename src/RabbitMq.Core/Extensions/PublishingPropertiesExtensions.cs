using System;
using System.Collections.Generic;
using RabbitMq.Core.Interfaces;
using RabbitMQ.Client;

namespace RabbitMq.Core.Extensions
{
    public static class PublishingPropertiesExtensions
    {
        /// <inheritdoc/>
        public static IBasicProperties CopyTo(this IPublishingProperties publishingProperties, IBasicProperties basicProperties)
        {
            // Check properties before setting as the IBasicProperties track the changes, and fails
            if (!string.IsNullOrWhiteSpace(publishingProperties.CorrelationId))
                basicProperties.CorrelationId = publishingProperties.CorrelationId;

            basicProperties.DeliveryMode = (DeliveryModes)publishingProperties.DeliveryMode;

            if (!string.IsNullOrWhiteSpace(publishingProperties.Expiration))
                basicProperties.Expiration = publishingProperties.Expiration;

            if (!string.IsNullOrWhiteSpace(publishingProperties.MessageId))
                basicProperties.MessageId = publishingProperties.MessageId;

            basicProperties.Persistent = publishingProperties.Persistent;
            if (!string.IsNullOrWhiteSpace(publishingProperties.ReplyTo))
                basicProperties.ReplyTo = publishingProperties.ReplyTo;

            if (!string.IsNullOrWhiteSpace(publishingProperties.UserId))
                basicProperties.UserId = publishingProperties.UserId;

            basicProperties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());

            basicProperties.Headers = new Dictionary<string, object>();
            foreach (var header in publishingProperties.Headers)
                basicProperties.Headers.Add(header.Key, header.Value);
            return basicProperties;
        }
    }
}