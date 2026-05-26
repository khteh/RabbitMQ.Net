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

public class IMessage1AckNackConsumer : IRabbitMqConsumer<IMessage>
{
    private readonly ILogger<IMessage1AckNackConsumer> _logger;
    public IMessage1AckNackConsumer(ILogger<IMessage1AckNackConsumer> logger) => _logger = logger;
    public async Task Consume(IRabbitMqConsumeContext<IMessage> consumeContext)
    {
        try
        {
            await Task.Run(() => { });
            //_context.IncrementSuccessCount();
            _logger.LogInformation($"{nameof(IMessage1AckNackConsumer)} [x] Received {consumeContext.RoutingKey}: {consumeContext.Message.Message} @ {consumeContext.Message.Timestamp}");
        }
        catch (Exception e)
        {
            //_context.SetCompleted();
            _logger.LogCritical($"{nameof(IMessage1AckNackConsumer)} Exception! {e.Message} {e.GetInnerMessage()} {e.StackTrace}");
        }
    }
}