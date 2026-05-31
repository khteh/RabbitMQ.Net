using System.Text;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Core;
using RabbitMQ.Core.Configuration;
using RabbitMQ.Core.Consumer;
using RabbitMQ.Core.Interfaces;
namespace RabbitMQ.Subscriber;

public class SubscriberWorker : BackgroundService
{
    private readonly ILogger<SubscriberWorker> _logger;
    private readonly IHostApplicationLifetime _hostAppLifetime;
    private readonly SharedState _sharedState;
    protected readonly RabbitMQConnectionConfig _rabbitMqOptions;
    protected readonly RabbitMQChannelConfig _rabbitMqChannelOptions;
    protected readonly RabbitMQQueueConfig _rabbitMqQueueOptions;
    private readonly IRabbitMQSubscriberFactory<IMessage> _subscriberFactory;
    private readonly IRabbitMQSubscriber<IMessage> _subscriber;
    private readonly SubscriberProperties _subscriberProperties;
    private readonly QueueProperties _queueProperties;
    private readonly IRabbitMQConsumer<IMessage> _consumer;
    private readonly IRabbitMQConnection _connection;
    private bool _isConnected = false;

    public SubscriberWorker(IHostApplicationLifetime hostApplicationLifetime, ILogger<SubscriberWorker> logger, IOptions<RabbitMQConnectionConfig> rabbitMqOptions, IOptions<RabbitMQChannelConfig> rabbitMqChannelOptions, IOptions<RabbitMQQueueConfig> rabbitMqQueueOptions, SharedState sharedState, IRabbitMQSubscriberFactory<IMessage> subscriberFactory, IRabbitMQConnection connection, IRabbitMQConsumer<IMessage> consumer)
    {
        _logger = logger;
        _hostAppLifetime = hostApplicationLifetime;
        _sharedState = sharedState;
        _rabbitMqOptions = rabbitMqOptions.Value;
        _rabbitMqChannelOptions = rabbitMqChannelOptions.Value;
        _rabbitMqQueueOptions = rabbitMqQueueOptions.Value;
        _subscriberFactory = subscriberFactory;
        _consumer = consumer;
        _connection = connection;
        _queueProperties = new QueueProperties(_rabbitMqQueueOptions);
        List<string> bindings = !string.IsNullOrEmpty(_rabbitMqOptions.Bindings) ? _rabbitMqOptions.Bindings.Split(",").ToList() : new List<string>();
        StringBuilder sb = new StringBuilder();
        foreach (string i in bindings)
            sb.Append($"{i}, ");
        _logger.LogInformation($"Bindings: {sb}");
        _subscriberProperties = new SubscriberProperties()
        {
            Exchange = string.IsNullOrEmpty(_rabbitMqOptions.Exchange) ? "topic_logs" : _rabbitMqOptions.Exchange,
            ExchangeType = "topic",
            Bindings = bindings
        };
        _subscriber = _subscriberFactory.GetRabbitMQSubscriber(_subscriberProperties, _queueProperties, true, _connection, _consumer, null);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation($"Worker {nameof(SubscriberWorker)} starts at: {DateTimeOffset.Now}");
        if (!_isConnected)
        {
            await _subscriber.Connect();
            _isConnected = true;
        }
        while (!stoppingToken.IsCancellationRequested)
            try
            {
                _logger.LogInformation(" [*] Waiting for logs...");
                /* This will initially block until a consumer calls Release() after processing a message, increasing the count by 1.
                * Then it enters the semaphore, decrement the count by 1 to 0 and then block waiting for a consumer to call Release() again.
                */
                await _sharedState.SignalEvent.WaitAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogCritical($"{nameof(SubscriberWorker)}.{nameof(ExecuteAsync)} Exception! {ex.Message}");
            }
        // Clean up when the service is stopping
        if (_subscriber != null)
        {
            _subscriber.Dispose();
            _isConnected = false;
        }
        _logger.LogInformation($"Worker {nameof(SubscriberWorker)}.{nameof(ExecuteAsync)} finishes at: {DateTimeOffset.Now}");
        //_hostAppLifetime.StopApplication();
    }
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Clean up connections gracefully
        _subscriber?.Dispose();
        _connection?.Dispose();
        _sharedState?.Dispose();
        await base.StopAsync(cancellationToken);
    }
}