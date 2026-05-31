using System;
using System.Collections.Generic;
using Polly;
using Polly.Retry;
using RabbitMQ.Core.Interfaces;

namespace RabbitMQ.Core.Consumer
{
    public interface IRabbitMQSubscriberFactory<TMessage>
        where TMessage : class
    {
        /// <summary>
        /// Returns an instance of the Rabbit Mq Subscriber
        /// </summary>
        /// <param name="queueName">RabbitMQ queue name</param>
        /// <param name="autoAck">AutoACK</param>
        /// <param name="connection">Rabbit Mq Connection</param>
        /// <param name="retryPolicyBuilderAction"> Rabbit Mq subscription Retry Policy</param>
        /// <param name="messageSerializer"></param>
        /// <returns>Rabbit Mq Subscriber</returns>
        IRabbitMQSubscriber<TMessage> GetRabbitMQSubscriber(ISubscriberProperties properties, IQueueProperties queueProperties, bool autoAck,
                IRabbitMQConnection connection, IRabbitMQConsumer<TMessage> consumer,
                Func<PolicyBuilder, AsyncRetryPolicy> retryPolicyBuilderAction);
    }
}