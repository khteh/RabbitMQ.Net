using System.Text;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMq.Core;
using RabbitMq.Core.Consumer;
using RabbitMq.Core.Interfaces;
namespace RabbitMq.Subscriber;

public class SubscriberWorker : BackgroundService
{
    private readonly ILogger<SubscriberWorker> _logger;
    private readonly IHostApplicationLifetime _hostAppLifetime;
    private readonly SharedState _sharedState;
    protected readonly RabbitMQConfig _rabbitMqOptions;
    private readonly IRabbitMqSubscriberFactory<IMessage> _subscriberFactory;
    private readonly IRabbitMqSubscriber<IMessage> _subscriber;
    private readonly SubscriberProperties _subscriberProperties;
    private readonly QueueProperties _queueProperties;
    private readonly IRabbitMqConsumer<IMessage> _consumer;
    private readonly IRabbitMqConnection _connection;
    private bool _isConnected = false;

    public SubscriberWorker(IHostApplicationLifetime hostApplicationLifetime, ILogger<SubscriberWorker> logger, IOptions<RabbitMQConfig> rabbitMqOptions, SharedState sharedState, IRabbitMqSubscriberFactory<IMessage> subscriberFactory, IRabbitMqConnection connection, IRabbitMqConsumer<IMessage> consumer)
    {
        _logger = logger;
        _hostAppLifetime = hostApplicationLifetime;
        _sharedState = sharedState;
        _rabbitMqOptions = rabbitMqOptions.Value;
        _subscriberFactory = subscriberFactory;
        _consumer = consumer;
        _connection = connection;
        _queueProperties = new QueueProperties()
        {
            Temporary = true,
            Durable = true,
            Exclusive = true,
            AutoDelete = true,
            Name = null
        };
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
        _subscriber = _subscriberFactory.GetRabbitMqSubscriber(_subscriberProperties, _queueProperties, true, _connection, _consumer, null);
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
                await _sharedState.SignalEvent.WaitAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogCritical($"{nameof(SubscriberWorker)}.{nameof(ExecuteAsync)} Exception ! {ex.Message}");
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
}