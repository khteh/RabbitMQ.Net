using System.Text;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMq.Core;
using RabbitMq.Core.Consumer;
using RabbitMq.Core.Interfaces;
namespace RabbitMq.Publisher;

public class PublisherWorker : BackgroundService
{
    private readonly ILogger<PublisherWorker> _logger;
    private readonly IHostApplicationLifetime _hostAppLifetime;
    private readonly SharedState _sharedState;
    protected readonly RabbitMQConfig _rabbitMqOptions;
    private readonly QueueProperties _queueProperties;
    private readonly IRabbitMqConnection _connection;
    private readonly PublishingProperties _publishingProperties;
    private IMessage _message;
    private IRabbitMqChannel _channel;

    public PublisherWorker(IHostApplicationLifetime hostApplicationLifetime, ILogger<PublisherWorker> logger, IOptions<RabbitMQConfig> rabbitMqOptions, SharedState sharedState, IRabbitMqConnection connection, IMessage message)
    {
        _logger = logger;
        _hostAppLifetime = hostApplicationLifetime;
        _sharedState = sharedState;
        _rabbitMqOptions = rabbitMqOptions.Value;
        _connection = connection;
        _message = message;
        _queueProperties = new QueueProperties()
        {
            Temporary = true,
            Durable = true,
            Exclusive = true,
            AutoDelete = true,
            Name = _rabbitMqOptions.QueueName
        };
        _publishingProperties = new PublishingProperties()
        {
            Exchange = string.IsNullOrEmpty(_rabbitMqOptions.Exchange) ? "topic_logs" : _rabbitMqOptions.Exchange,
            ExchangeType = "topic",
            RoutingKey = _rabbitMqOptions.RoutingKey,
            EnablePublisherConfirm = true,
            EnsureDeliveryToQueue = true // This maps to "mandatory" flag of Publish function. False: Silent when there is no subscriber. True: broker will return BasicReturnEventArgs. https://www.rabbitmq.com/dotnet-api-guide.html
        };
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation($"Worker {nameof(PublisherWorker)} starts at: {DateTimeOffset.Now}");
        try
        {
            _logger.LogInformation($"{nameof(PublisherWorker)}.{nameof(ExecuteAsync)} ConnectionName: {_rabbitMqOptions.ConnectionName}, Endpoint: {_rabbitMqOptions.Endpoint}, Port: {_rabbitMqOptions.Port}, VHost: {_rabbitMqOptions.VHost}, Message: {_message.Message} @ {_message.Timestamp}, Exchange: {_rabbitMqOptions.Exchange}, RoutingKey: {_rabbitMqOptions.RoutingKey}, Queue: {_rabbitMqOptions.QueueName}");
            _channel = await _connection.CreateChannel(string.IsNullOrEmpty(_rabbitMqOptions.Exchange) ? "topic_logs" : _rabbitMqOptions.Exchange, "topic", _queueProperties);
            _logger.LogInformation($"");
            /* This will initially block until a consumer calls Release() after processing a message, increasing the count by 1.
            * The following call will not block because it the count is 1. Then it will enters the semaphore with the count decremented by one, continue with the while loop.and call WaitAsync again, which will block.
            */
            //_message.Timestamp = DateTimeOffset.UtcNow;
            PublishResult result = await _channel.Publish<IMessage>(_message, _publishingProperties);
            if (result != null && result.Success && result.Errors == null)
                _logger.LogInformation($" [x] Sent {_rabbitMqOptions.RoutingKey}: {_message.Message} @ {_message.Timestamp}");
            else if (result.Errors != null && result.Errors.Any())
                _logger.LogError($"Publish failed! {result.Errors.First().Code} {result.Errors.First().Description}");
            else
                _logger.LogError("Publish failed with unknown error!");
        }
        catch (Exception ex)
        {
            _logger.LogCritical($"{nameof(PublisherWorker)}.{nameof(ExecuteAsync)} Exception ! {ex.Message}");
        }
        finally
        {
            _logger.LogInformation($"Worker {nameof(PublisherWorker)} finishes at: {DateTimeOffset.Now}");
            if (_channel != null)
                _channel.Dispose();
            _hostAppLifetime.StopApplication();
        }
    }
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Clean up connections gracefully
        _channel?.Dispose();
        _connection?.Dispose();
        _sharedState?.Dispose();
        await base.StopAsync(cancellationToken);
    }
}