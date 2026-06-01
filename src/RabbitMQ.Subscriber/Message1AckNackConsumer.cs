using RabbitMQ.Core;
using RabbitMQ.Core.Consumer;
using RabbitMQ.Core.Extensions;
using RabbitMQ.Core.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using static System.Console;
using System;
namespace RabbitMQ.Subscriber;

public class Message1AckNackConsumer : IRabbitMQConsumer<IMessage>
{
    private readonly ILogger<Message1AckNackConsumer> _logger;
    private readonly SharedState _sharedState;
    public Message1AckNackConsumer(ILogger<Message1AckNackConsumer> logger, SharedState sharedState) => (_logger, _sharedState) = (logger, sharedState);
    public async Task Consume(IRabbitMQConsumeContext<IMessage> consumeContext)
    {
        try
        {
            //_context.IncrementSuccessCount();
            _logger.LogInformation($"{nameof(Message1AckNackConsumer)} [x] Received {consumeContext.RoutingKey}: {consumeContext.Message.Message} @ {consumeContext.Message.Timestamp}");
            if (_sharedState.SignalEvent.CurrentCount == 0)
                _sharedState.SignalEvent.Release();
        }
        catch (Exception e)
        {
            //_context.SetCompleted();
            _logger.LogCritical($"{nameof(Message1AckNackConsumer)} Exception! {e.Message} {e.GetInnerMessage()} {e.StackTrace}");
        }
    }
}