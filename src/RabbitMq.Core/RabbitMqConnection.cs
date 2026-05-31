using System.Diagnostics;
using System.Reflection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMq.Core.Events;
using RabbitMq.Core.Interfaces;
using RabbitMq.Core.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMq.Core.Extensions;
namespace RabbitMq.Core;

public class RabbitMqConnection : DisposableObject, IRabbitMqConnection
{
    private static readonly SemaphoreSlim _connectionLock = new SemaphoreSlim(1, 1);
    private readonly object _connectionEventsLock = new object();
    private IConnection _currentConnection { get; set; }

    private bool _connectionHealthy = true;
    private readonly ILogger<RabbitMqConnection> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private ConnectionFactory _connectionFactory;
    protected readonly RabbitMQConnectionConfig _rabbitMqOptions;
    protected readonly RabbitMQChannelConfiguration _rabbitMqChannelOptions;
    protected readonly RabbitMQQueueConfig _rabbitMqQueueOptions;
    protected readonly CreateChannelOptions _createChannelOptions;
    public event EventHandler<RabbitMqConnectedEventArgs> Connected;
    public event EventHandler<RabbitMqDisconnectedEventArgs> Disconnected;
    private IChannel _channel;
    /// <summary>
    /// Creates an instance of the Connection Configurations 
    /// </summary>
    /// <param name="logger">Logger Instance</param>
    /// <param name="connectionConfigurations">List of connections</param>
    public RabbitMqConnection(ILoggerFactory loggerFactory, ILogger<RabbitMqConnection> logger, IOptions<RabbitMQConnectionConfig> rabbitMqOptions, IOptions<RabbitMQChannelConfiguration> rabbitMqChannelOptions, IOptions<RabbitMQQueueConfig> rabbitMqQueueOptions)
    {
        _rabbitMqOptions = rabbitMqOptions.Value;
        _rabbitMqChannelOptions = rabbitMqChannelOptions.Value;
        _rabbitMqQueueOptions = rabbitMqQueueOptions.Value;
        _logger = logger;
        _loggerFactory = loggerFactory;
        _logger.LogInformation($"{nameof(RabbitMqConnection)}: ConnectionName:{_rabbitMqOptions.ConnectionName}, UserName:{_rabbitMqOptions.UserName}, Password: {_rabbitMqOptions.Password}, Endpoint: {_rabbitMqOptions.Endpoint}, VHost: {_rabbitMqOptions.VHost}, Port: {_rabbitMqOptions.Port}, Exchange: {_rabbitMqOptions.Exchange}, Bindings: {_rabbitMqOptions.Bindings}, RoutingKey: {_rabbitMqOptions.RoutingKey}, Queue: {_rabbitMqQueueOptions.Name}");
        _connectionFactory = new ConnectionFactory()
        {
            // This will enable the subscriber to get notified of cancellation
            // Use ful for the case of clustered queue issues
            // "guest"/"guest" by default, limited to localhost connections
            UserName = _rabbitMqOptions.UserName,
            Password = _rabbitMqOptions.Password,
            VirtualHost = _rabbitMqOptions.VHost,
            HostName = _rabbitMqOptions.Endpoint,
            Port = _rabbitMqOptions.Port
        };
        /*
        * https://github.com/docker-library/rabbitmq/discussions/813
        */
        _connectionFactory.Ssl.Enabled = false;
        // .NET expects this to match the Subject Alternative Name (SAN) or Common Name (CN) on the certificate that the server sends over.
        _connectionFactory.Ssl.ServerName = "*.rabbitmq-nodes.default.svc.cluster.local,rabbitmq.default.svc.cluster.local"; // This MUST match the Subject Alternative Name (SAN) or CN on the peer's (server's) leaf certificate,
                                                                                                                             //_connectionFactory.Ssl.ServerName = string.Empty;
        _connectionFactory.Ssl.CertPath = "server.crt";
        _connectionFactory.Ssl.CertPassphrase = _rabbitMqOptions.Password;
        _createChannelOptions = new CreateChannelOptions(
            publisherConfirmationsEnabled: _rabbitMqChannelOptions.PublisherConfirmationsEnabled,
            publisherConfirmationTrackingEnabled: _rabbitMqChannelOptions.PublisherConfirmationTrackingEnabled,
            outstandingPublisherConfirmationsRateLimiter: new ThrottlingRateLimiter(_rabbitMqChannelOptions.MaxOutstandingConfirmation)
        );
    }

    /// <inheritdoc/>
    public async Task<IRabbitMqChannel> CreateChannel(string exchange, string type, string routingKey, IQueueProperties queueProperties)
    {
        if (IsDisposed || IsDisposing)
            throw new InvalidOperationException($"Channel is already Disposed");
        IConnection rabbitMqConnection = await GetConnection();
        if (rabbitMqConnection != null && rabbitMqConnection.IsOpen)
        {
            _logger.LogInformation($"{nameof(RabbitMqConnection)}.{nameof(CreateChannel)} Exchange: {exchange}, type: {type}, routingKey: {routingKey}, {(queueProperties.Temporary ? "Temporary" : queueProperties.Name)}");
            //return new RabbitMqChannel(_loggerFactory.CreateLogger<RabbitMqChannel>(), rabbitMqConnection.CreateModel(), exchange, type, properties);
            _channel = await rabbitMqConnection.CreateChannelAsync(_createChannelOptions);
            await _channel.ExchangeDeclareAsync(exchange, type);
            string queueName = queueProperties.Name;
            if (queueProperties.Temporary)
            {
                /*
                * A temporary queue is a fresh empty queue.
                * Use case:
                * (1) Receive all messages, not just a subset of them.
                * (2) Interested only in currently flowing messages but not in the old ones.
                */
                var result = await _channel.QueueDeclareAsync(queue: string.Empty,
                                    durable: queueProperties.Durable,
                                    exclusive: queueProperties.Exclusive,
                                    autoDelete: queueProperties.AutoDelete,
                                    arguments: null);
                queueName = result.QueueName;
            }
            else if (!string.IsNullOrEmpty(queueProperties.Name))
            {
                await _channel.QueueDeclareAsync(queue: queueProperties.Name,
                                    durable: queueProperties.Durable,
                                    exclusive: queueProperties.Exclusive,
                                    autoDelete: queueProperties.AutoDelete,
                                    arguments: null);
                /* Ignored when the exchange type is fanout, which routes messages to all bound queues regardless of the routing key. https://www.rabbitmq.com/tutorials/amqp-concepts.html
                * Topic binding rules:
                  * substitutes exactly one word.
                  # substitutes zero or more words.
                  Words are separated by . in routing keys.
                */
                if (!string.IsNullOrEmpty(routingKey))
                    await _channel.QueueBindAsync(queueName, exchange, routingKey, null);
            }
            return new RabbitMqChannel(_loggerFactory.CreateLogger<RabbitMqChannel>(), _channel, exchange, queueName);
        }
        else
            return null;
    }

    /// <inheritdoc/>
    /// We need this connection as this kicks starts all the subscriptions
    public async Task<IConnection> Start() => await GetConnection();
    /// <inheritdoc/>
    public bool IsConnected() => _connectionHealthy && _currentConnection != null && _currentConnection.IsOpen;
    private async Task<IConnection> GetConnection()
    {
        if (IsDisposing || IsDisposed)
            throw new InvalidOperationException($"{nameof(RabbitMqConnection)}.{nameof(GetConnection)} The Connection has been already disposed");
        for (int i = 0, maxRetryCount = 3; i < maxRetryCount; i++)
        {
            // if the Existing Connection is healthy
            if (IsConnected())
                return _currentConnection;
            await _connectionLock.WaitAsync();
            try
            {
                // check again if the connection is good
                if (_currentConnection != null && _currentConnection.IsOpen)
                    return _currentConnection;
                // Make sure we dispose the existing connection
                // Hence using a different lock for the events
                await ClearConnection($"{nameof(RabbitMqConnection)}.{nameof(GetConnection)} reconnecting...");
                try
                {
                    // Create a new Connection, we need to hook it up
                    var excecutingProcess = Process.GetCurrentProcess();
                    FileVersionInfo fvi = FileVersionInfo.GetVersionInfo(Assembly.GetExecutingAssembly().Location);
                    string version = fvi.FileVersion;
                    //GetRabbitMqConnectionFactory(_configuration);
                    //_currentConnection = await _connectionFactory.CreateConnectionAsync();
                    //Uri uri = new Uri($"amqp://{_rabbitMqOptions.UserName}:{_rabbitMqOptions.Password}@{_rabbitMqOptions.Endpoint}/{_rabbitMqOptions.VHost}");
                    //_connectionFactory = new ConnectionFactory() { uri, AutomaticRecoveryEnabled = true };
                    _currentConnection = await _connectionFactory.CreateConnectionAsync(!string.IsNullOrWhiteSpace(_rabbitMqOptions.ConnectionName) ? _rabbitMqOptions.ConnectionName : $"{excecutingProcess?.ProcessName}_{version}");
                    _currentConnection.ConnectionShutdownAsync += OnConnectionShutdown;
                    _currentConnection.ConnectionBlockedAsync += OnConnectionBlocked;
                    _currentConnection.ConnectionUnblockedAsync += OnConnectionUnblocked;
                    _currentConnection.RecoverySucceededAsync += OnRecoverySucceeded;
                    _logger.LogInformation($"{nameof(RabbitMqConnection)}.{nameof(GetConnection)} Connection Established! Endpoint: {_currentConnection?.Endpoint}");
                    OnConnected();
                    break;
                }
                catch (RabbitMQ.Client.Exceptions.BrokerUnreachableException e)
                {
                    _connectionHealthy = false;
                    if (i >= maxRetryCount)
                        throw;
                    _logger.LogCritical($"{nameof(RabbitMqConnection)}.{nameof(GetConnection)} Endpoint: amqp://{_rabbitMqOptions.UserName}:{_rabbitMqOptions.Password}@{_rabbitMqOptions.Endpoint}:{_rabbitMqOptions.Port}/{_rabbitMqOptions.VHost} BrokerUnreachableException! {e.Message} {e.GetInnerMessage()} {e.StackTrace}");
                }
                catch (Exception e)
                {
                    _connectionHealthy = false;
                    if (i >= maxRetryCount)
                        throw;
                    _logger.LogCritical($"{nameof(RabbitMqConnection)}.{nameof(GetConnection)} Endpoint: amqp://{_rabbitMqOptions.UserName}:{_rabbitMqOptions.Password}@{_rabbitMqOptions.Endpoint}:{_rabbitMqOptions.Port}/{_rabbitMqOptions.VHost} Exception! {e.Message} {e.GetInnerMessage()} {e.StackTrace}");
                }
            }
            finally
            {
                _connectionLock.Release();
            }
        }
        return _currentConnection;
    }

    private async Task OnRecoverySucceeded(object sender, AsyncEventArgs e)
    {
        _logger.LogInformation($"{nameof(RabbitMqConnection)}.{nameof(OnRecoverySucceeded)} Connection Recovered! Endpoint:{_currentConnection?.Endpoint}");
        OnConnected();
    }

    private async Task OnConnectionUnblocked(object sender, AsyncEventArgs e)
    {
        _connectionHealthy = true;
        _logger.LogInformation($"{nameof(RabbitMqConnection)}.{nameof(OnConnectionUnblocked)} Connection UnBlocked!");
    }

    private async Task OnConnectionBlocked(object sender, ConnectionBlockedEventArgs e) =>
        _logger.LogWarning($"{nameof(RabbitMqConnection)}.{nameof(OnConnectionBlocked)} Connection Blocked! Reason: {e.Reason}");

    private async Task OnConnectionShutdown(object sender, ShutdownEventArgs e)
    {
        _logger.LogWarning($"{nameof(RabbitMqConnection)}.{nameof(OnConnectionShutdown)} Connection Shutdown! ReplyCode: {e.ReplyCode}, reply Text: {e.ReplyText}. Retrying another endpoint in the cluster");
        OnDisconnected();
    }

    private void OnConnected()
    {
        _logger.LogInformation($"{nameof(RabbitMqConnection)}.{nameof(OnConnected)} ConnectionName: {_rabbitMqOptions.ConnectionName}");
        lock (_connectionEventsLock)
        {
            _connectionHealthy = true;
            //publish the event up, so that host can wireup the new channels
            Connected?.Invoke(this, new RabbitMqConnectedEventArgs());
        }
    }

    private void OnDisconnected()
    {
        _logger.LogInformation($"{nameof(RabbitMqConnection)}.{nameof(OnDisconnected)} ConnectionName: {_rabbitMqOptions.ConnectionName}");
        lock (_connectionEventsLock)
        {
            _connectionHealthy = false;
            //publish the event up, so that host can dispose the existing channels
            Disconnected?.Invoke(this, new RabbitMqDisconnectedEventArgs());
        }
    }

    /// <inheritdoc/>
    private async Task ClearConnection(string reason)
    {
        try
        {
            _logger.LogInformation($"{nameof(RabbitMqConnection)}.{nameof(ClearConnection)} Endpoint: {_currentConnection?.Endpoint.ToString()}, Reason: {reason}");
            if (_currentConnection != null && _currentConnection.IsOpen)
            {
                await _channel.CloseAsync();
                await _channel.DisposeAsync();
                await _currentConnection.CloseAsync(200, reason, TimeSpan.FromMilliseconds(500));
                await _currentConnection.DisposeAsync();
            }
            _channel = null;
            _currentConnection = null;
        }
        catch (Exception e)
        {
            _logger.LogCritical($"{nameof(RabbitMqConnection)}.{nameof(ClearConnection)} Exception! {e.Message} {e.GetInnerMessage()} {e.StackTrace}");
        }
    }

#if false
        private ConnectionFactory GetRabbitMqConnectionFactory(RabbitMqConnectionConfiguration clusterConfiguration)
        {
            if (_connectionFactory != null)
                return _connectionFactory;


            lock (_connectionFactoryLock)
            {
                if (_connectionFactory != null)
                    return _connectionFactory;
                var factory = new ConnectionFactory()
                {
                    // This is required as the Vhost is lost using EndpointResolverFactory
                    Uri = clusterConfiguration.RabbitMqUri,
                    UserName = clusterConfiguration.UserName,
                    Password = clusterConfiguration.Password
                };
                if (clusterConfiguration.ClusteredRabbitMqHosts != null && clusterConfiguration.ClusteredRabbitMqHosts.Any())
                {
                    _logger.LogInformation("Trying to create Rabbit Mq connection. ConnectionName:{ConnectionName}, UserName:{UserName}, ClusteredRabbitMqHosts:{ClusteredRabbitMqHosts}, Endpoint:{Endpoint}", 
                        _configuration.ConnectionName,
                        _configuration.UserName,
                        string.Join(",", clusterConfiguration.ClusteredRabbitMqHosts),
                        clusterConfiguration.RabbitMqUri.ToString());

                    factory.HostName = null;
                    //factory.EndpointResolverFactory = x => _endpointResolver;
                }

                var excecutingProcess = Process.GetCurrentProcess();
                FileVersionInfo fvi = FileVersionInfo.GetVersionInfo(Assembly.GetExecutingAssembly().Location);
                string version = fvi.FileVersion;
                factory.ClientProperties["client"] = "RabbitMq";
                factory.ClientProperties["connected"] = DateTimeOffset.Now.ToString("R");
                factory.ClientProperties["process_id"] = excecutingProcess?.Id;
                factory.ClientProperties["process_name"] = excecutingProcess?.ProcessName;
                // This will enable the subscriber to get notified of cancellation
                // Use ful for the case of clustered queue issues
                factory.ClientProperties["consumer_cancel_notify"] = "true";
                factory.ClientProperties["connection_name"] = !string.IsNullOrWhiteSpace(_configuration.ConnectionName) ? _configuration.ConnectionName : $"{excecutingProcess?.ProcessName}_{version}";
                factory.ClientProperties["assembly_version"] = version;
                _connectionFactory = factory;
                return factory;
            }
        }
#endif
    /// <inheritdoc/>
    protected override void Disposing()
    {
        if (_connectionFactory != null)
            _connectionFactory.AutomaticRecoveryEnabled = false;
        Task.Run(async () => await ClearConnection($"{nameof(RabbitMqConnection)} disposing"));
    }
}
