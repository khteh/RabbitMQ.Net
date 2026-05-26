using RabbitMq.Core;
using RabbitMq.Core.Consumer;
using RabbitMq.Core.Extensions;
using RabbitMq.Core.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using static System.Console;
using System;
namespace RabbitMq.Subscriber;

public class Message1AckNackConsumer : IRabbitMqConsumer<IMessage>
{
    private readonly ILogger<Message1AckNackConsumer> _logger;
    private readonly SharedState _sharedState;
    public Message1AckNackConsumer(ILogger<Message1AckNackConsumer> logger, SharedState sharedState) => (_logger, _sharedState) = (logger, sharedState);
    public async Task Consume(IRabbitMqConsumeContext<IMessage> consumeContext)
    {
        try
        {
            await Task.Run(() => { });
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