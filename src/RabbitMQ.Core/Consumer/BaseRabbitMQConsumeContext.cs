using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Core.Interfaces;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Core.Consumer
{
    public abstract class BaseRabbitMQConsumeContext : IRabbitMQConsumeContext
    {
        ILogger _logger;
        private IRabbitMQChannel _channel;
        private bool _ackNacked = false;
        /// <summary>
        /// Raw Rabbit Mq Consume Event
        /// </summary>
        public BasicDeliverEventArgs RabbitMQDeliveredEvent { get; set; }

        /// <inheritdoc/>
        public IReadOnlyBasicProperties BasicProperties { get; private set; }

        /// <inheritdoc/>
        public string ConsumerTag { get; private set; }

        /// <inheritdoc/>
        public string Exchange { get; private set; }

        /// <inheritdoc/>
        public string RoutingKey { get; private set; }

        /// <inheritdoc/>
        public string QueueName { get; private set; }

        /// <inheritdoc/>
        public bool AutoAck { get; private set; }

        /// <summary>
        /// Creates an Instance of BaseRabbitMQConsumeContext 
        /// </summary>
        /// <param name="queueName">RabbitMQ queue name the message was received on</param>
        /// <param name="rabbitMqDeliverArguments">Raw Rabbit Mq Event</param>
        /// <param name="channel">Raw Rabbit Mq Channel</param>        
        /// <param name="requiredAck">Subscription requires explicit Ack/Nack</param>
        /// <param name="logger">Logger Instance</param>
        public BaseRabbitMQConsumeContext(string queueName, BasicDeliverEventArgs rabbitMqDeliverArguments, IRabbitMQChannel channel, bool autoAck, ILogger logger)
        {
            _channel = channel;
            _logger = logger;
            RabbitMQDeliveredEvent = rabbitMqDeliverArguments;
            BasicProperties = rabbitMqDeliverArguments.BasicProperties;
            ConsumerTag = rabbitMqDeliverArguments.ConsumerTag;
            Exchange = rabbitMqDeliverArguments.Exchange;
            RoutingKey = rabbitMqDeliverArguments.RoutingKey;
            QueueName = queueName;
            AutoAck = autoAck;
        }

        /// <inheritdoc/>
        public void Ack()
        {
            if (AutoAck)
            {
                _logger.LogError($"Ack is done on a subscription with autoAck, on queue {QueueName}");
                throw new InvalidOperationException($"Ack is done on a subscription without ack or nack, on queue {QueueName}");
            }
            if (_ackNacked)
            {
                _logger.LogInformation($"Cannot Ack again, as message with {RabbitMQDeliveredEvent.DeliveryTag} is already Ack or Nack");
                return;
            }
            _ackNacked = true;
            // We only support single message level acks/nacks
            _channel.Ack(RabbitMQDeliveredEvent.DeliveryTag, false);

        }

        /// <inheritdoc/>
        public void Nack(bool reQueue)
        {
            if (AutoAck)
            {
                _logger.LogError($"Nack is done on a subscription with autoAck, on queue {QueueName}");
                throw new InvalidOperationException($"Nack is done on a subscription without ack or nack, on queue {QueueName}");
            }

            if (_ackNacked)
            {
                _logger.LogInformation($"Cannot Nack again, as message with {RabbitMQDeliveredEvent.DeliveryTag} is already Ack or Nack");
                return;
            }
            ;
            _ackNacked = true;
            // We only support single message level acks/nacks
            _channel.Nack(RabbitMQDeliveredEvent.DeliveryTag, false, reQueue);
        }

        /// <summary>
        /// Respond to RPC Request
        /// </summary>
        /// <typeparam name="TResponse">Response Type</typeparam>
        /// <param name="response">Response Object</param>
        public async Task Respond<TResponse>(TResponse response) where TResponse : class
            => await _channel.Respond<TResponse>(RabbitMQDeliveredEvent, response, null);

        /// <summary>
        /// Get the Generic 
        /// </summary>
        /// <typeparam name="TMessage">Type of the Message</typeparam>
        /// <returns>Succeeded/Failure</returns>
        public abstract IRabbitMQConsumeContext<TMessage> GetConsumeContext<TMessage>() where TMessage : class;
    }
}