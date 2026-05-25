using System;
using System.Collections.Generic;
using Polly;
using Polly.Retry;
using RabbitMq.Core.Interfaces;

namespace RabbitMq.Core.Consumer
{
    public interface IRabbitMqSubscriberFactory<TMessage>
        where TMessage : class
    {
        /// <summary>
        /// Returns an instance of the Rabbit Mq Subscriber
        /// </summary>
        /// <param name="queueName">RabbitMq queue name</param>
        /// <param name="autoAck">AutoACK</param>
        /// <param name="connection">Rabbit Mq Connection</param>
        /// <param name="retryPolicyBuilderAction"> Rabbit Mq subscription Retry Policy</param>
        /// <param name="messageSerializer"></param>
        /// <returns>Rabbit Mq Subscriber</returns>
        IRabbitMqSubscriber<TMessage> GetRabbitMqSubscriber(ISubscriberProperties properties, IQueueProperties queueProperties, bool autoAck,
                IRabbitMqConnection connection, IRabbitMqConsumer<TMessage> consumer,
                Func<PolicyBuilder, AsyncRetryPolicy> retryPolicyBuilderAction);
    }
}