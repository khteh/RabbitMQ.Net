using Microsoft.Extensions.Logging;
using RabbitMq.Core.Interfaces;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Text.Json;
namespace RabbitMq.Core.Consumer;

public class RabbitMqConsumerContext : BaseRabbitMqConsumeContext
{
    private readonly ILogger _logger;
    private readonly JsonSerializerOptions _jsonSerializerOptions;

    public RabbitMqConsumerContext(string queueName, BasicDeliverEventArgs rabbitMqDeliveredArgs, IRabbitMqChannel channel, bool ackRequired, ILogger logger)
        : base(queueName, rabbitMqDeliveredArgs, channel, ackRequired, logger)
    {
        _logger = logger;
        _jsonSerializerOptions = new JsonSerializerOptions();
        _jsonSerializerOptions.Converters.Add(new MessageConverter());
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

                _logger.LogDebug($"{nameof(RabbitMqConsumerContext)}.{nameof(GetConsumeContext)}: Deserializing Message id: {RabbitMqDeliveredEvent.BasicProperties?.MessageId}, body:{message} to type:{typeof(TMessage).Name}");
            // Deserialization of interface or abstract types is not supported. Type 'RabbitMq.Core.Interfaces.IMessage' wihtout using a custom converter.
            return new RabbitMqMessageConsumeContext<TMessage>(this, JsonSerializer.Deserialize<TMessage>(body, _jsonSerializerOptions));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                $"{nameof(RabbitMqConsumerContext)}.{nameof(GetConsumeContext)}: Failed to Convert the incoming message to {typeof(TMessage).Name}, on queue:{QueueName}, exception:{ex}");
            throw;
        }
    }
}