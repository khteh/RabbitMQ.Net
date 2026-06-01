using Microsoft.Extensions.Logging;
using RabbitMQ.Core.Interfaces;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Text.Json;
namespace RabbitMQ.Core.Consumer;

public class RabbitMQConsumerContext : BaseRabbitMQConsumeContext
{
    private readonly ILogger _logger;
    private readonly JsonSerializerOptions _jsonSerializerOptions;

    public RabbitMQConsumerContext(string queueName, BasicDeliverEventArgs rabbitMqDeliveredArgs, IRabbitMQChannel channel, bool ackRequired, ILogger logger)
        : base(queueName, rabbitMqDeliveredArgs, channel, ackRequired, logger)
    {
        _logger = logger;
        _jsonSerializerOptions = new JsonSerializerOptions();
        _jsonSerializerOptions.Converters.Add(new MessageConverter());
    }

    public override IRabbitMQConsumeContext<TMessage> GetConsumeContext<TMessage>()
    {
        try
        {
            byte[] body = RabbitMQDeliveredEvent.Body.ToArray();
            string message = Encoding.UTF8.GetString(body);

            // 2. Access metadata
            string routingKey = RabbitMQDeliveredEvent.RoutingKey;
            var props = RabbitMQDeliveredEvent.BasicProperties;
            if (_logger.IsEnabled(LogLevel.Debug))

                _logger.LogDebug($"{nameof(RabbitMQConsumerContext)}.{nameof(GetConsumeContext)}: Deserializing Message id: {RabbitMQDeliveredEvent.BasicProperties?.MessageId}, body:{message} to type:{typeof(TMessage).Name}");
            // Deserialization of interface or abstract types is not supported. Type 'RabbitMQ.Core.Interfaces.IMessage' wihtout using a custom converter.
            return new RabbitMQMessageConsumeContext<TMessage>(this, JsonSerializer.Deserialize<TMessage>(body, _jsonSerializerOptions));
        }
        catch (Exception ex)
        {
            _logger.LogCritical($"{nameof(RabbitMQConsumerContext)}.{nameof(GetConsumeContext)}: Failed to Convert the incoming message to {typeof(TMessage).Name}, on queue:{QueueName}, exception:{ex}");
            throw;
        }
    }
}