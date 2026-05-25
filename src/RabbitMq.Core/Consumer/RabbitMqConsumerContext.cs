using Microsoft.Extensions.Logging;
using RabbitMq.Core.Interfaces;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Text.Json;
namespace RabbitMq.Core.Consumer;

public class RabbitMqConsumerContext : BaseRabbitMqConsumeContext
{
    ILogger _logger;
    //private readonly IMessageSerializer _messageSerializer;

    public RabbitMqConsumerContext(string queueName, BasicDeliverEventArgs rabbitMqDeliveredArgs, IRabbitMqChannel channel, bool ackRequired, ILogger logger)
        : base(queueName, rabbitMqDeliveredArgs, channel, ackRequired, logger)
    {
        _logger = logger;
        //_messageSerializer = messageSerializer;
    }

    public override IRabbitMqConsumeContext<TMessage> GetConsumeContext<TMessage>()
    {
        try
        {
            byte[] body = RabbitMqDeliveredEvent.Body.ToArray();
            string message = Encoding.UTF8.GetString(body);

            // 2. Access metadata
            string routingKey = RabbitMqDeliveredEvent.RoutingKey;
            var props = RabbitMqDeliveredEvent.BasicProperties;
            if (_logger.IsEnabled(LogLevel.Debug))
            {

                // 3. Deserialize the response
                //var response = message.FromJson<TResponse>();
                //responseReceivedTask.TrySetResult(JsonConvert.DeserializeObject<TResponse>(message));
                //
                //var message = Encoding.UTF8.GetString(RabbitMqDeliveredEvent.Body);
                _logger.LogDebug(
                    $"Rabbit Mq: Deserializing Message id:{RabbitMqDeliveredEvent.BasicProperties?.MessageId}, body:{message} to type:{typeof(TMessage).Name}");
            }
            //object obj = RabbitMqDeliveredEvent.Body.FromByteArray<TMessage>();//_messageSerializer.Deserialize<TMessage>(RabbitMqDeliveredEvent.Body);
            return new RabbitMqMessageConsumeContext<TMessage>(this, JsonSerializer.Deserialize<TMessage>(body));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                $"Failed to Convert the incoming message to {typeof(TMessage).Name}, on queue:{QueueName}, exception:{ex}");
            throw;
        }
    }
}