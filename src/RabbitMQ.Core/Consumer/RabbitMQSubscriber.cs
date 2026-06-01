using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using RabbitMQ.Core.Events;
using RabbitMQ.Core.Exceptions;
using RabbitMQ.Core.Interfaces;
using RabbitMQ.Client.Events;
namespace RabbitMQ.Core.Consumer;

public sealed class RabbitMQSubscriber<TMessage> : DisposableObject, IRabbitMQSubscriber<TMessage>
    where TMessage : class
{
    private IRabbitMQChannel _channel;
    private IRabbitMQConnection _connection;
    private bool _autoAck = false;
    private ISubscriberProperties _properties;
    private IQueueProperties _queueProperties;
    private string _consumerTag;
    private readonly ILogger<RabbitMQSubscriber<TMessage>> _logger;
    public readonly AsyncRetryPolicy _subscriptionRetryPolicy;
    private readonly IRabbitMQConsumer<TMessage> _consumer;
    public RabbitMQSubscriber(ILogger<RabbitMQSubscriber<TMessage>> logger, IRabbitMQConsumer<TMessage> consumer, ISubscriberProperties properties, IQueueProperties queueProperties, bool autoAck, IRabbitMQConnection connection, AsyncRetryPolicy policy)
    {
        _consumer = consumer;
        _logger = logger;
        _autoAck = autoAck;
        _properties = properties;
        _queueProperties = queueProperties;
        _connection = connection;
        _subscriptionRetryPolicy = policy;
    }
    public async Task Connect()
    {
        EnsureNotDisposing();
        await _subscriptionRetryPolicy.ExecuteAsync(Subscribe);
    }
    private async Task Subscribe()
    {
        try
        {
            DisposeChannel();
            _channel = await _connection.CreateChannel(_properties.Exchange, _properties.ExchangeType, string.Empty, _queueProperties);
            _consumerTag = await _channel.Subscribe(_autoAck, OnConsume, OnSubscriberDisconnected, _properties.Bindings);
            // Only List on disconnected if we are able to subscribe to it
            _channel.Disconnected += OnChannelDisconnected;
            _logger.LogInformation($"{nameof(RabbitMQSubscriber<TMessage>)}.{nameof(Subscribe)} Subscribed to {_queueProperties.Name}, autoAck {_autoAck}, consumer Tag: {_consumerTag} on channel {_channel.Id}. Exchange: {_properties.Exchange}, ExchangeType: {_properties.ExchangeType}, Bindings: {_properties.Bindings.Count()} {(_properties.Bindings.Count() == 1 ? _properties.Bindings.First() : string.Empty)}");
        }
        catch (Exception e)
        {
            _logger.LogCritical($"{nameof(RabbitMQSubscriber<TMessage>)}.{nameof(Subscribe)} Failed to subscribe to queue {_queueProperties.Name} on channel. Exchange: {_properties.Exchange}, ExchangeType: {_properties.ExchangeType}, Bindings: {_properties.Bindings.Count()} {(_properties.Bindings.Count() == 1 ? _properties.Bindings.First() : string.Empty)}. Exception! {e}");
            if (_channel != null)
                _channel.Disconnected -= OnChannelDisconnected;
            DisposeChannel();
            throw;
        }
    }
    public async Task OnConsume(object sender, BasicDeliverEventArgs arguments)
    {
        try
        {
            _logger.LogInformation($"{nameof(RabbitMQSubscriber<TMessage>)}.{nameof(OnConsume)} Received message from Exchange: {arguments?.Exchange}, Routing Key: {arguments?.RoutingKey} to Queue: {_queueProperties.Name}, on channel: {_channel?.Id}, Redelivered :{arguments?.Redelivered}, MessageId:{arguments?.BasicProperties?.MessageId}");
            EnsureNotDisposing();
            // _logger.LogInformation($"Received message from Exchange: {arguments?.Exchange}, Routing Key: {arguments?.RoutingKey} to Queue: {_queueProperties.Name}, on channel:{_channel?.Id}, Redelivered :{arguments?.Redelivered}, MessageId:{arguments?.BasicProperties?.MessageId}");
            var jsonContext = new RabbitMQConsumerContext(_queueProperties.Name, arguments, _channel, _autoAck, _logger);
            IRabbitMQConsumeContext<TMessage> consumeContext = jsonContext.GetConsumeContext<TMessage>();
            //using (var scope = _container.CreateScope())
            //{
            //var consumer = _container.GetRequiredService<IRabbitMQConsumer<TMessage>>();
            // Call async consumer via the sync Rabbit Mq consumer
            EnsureNotDisposing();
            try
            {
                _consumer.Consume(consumeContext).ConfigureAwait(false).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                _logger.LogCritical($"{nameof(RabbitMQSubscriber<TMessage>)}.{nameof(OnConsume)} Rabbit Mq Consumer Failed, type {_consumer.GetType()}. Exception! {ex}");
                // Ignore Consumer Exceptions
            }
            //}
        }
        catch (Exception ex)
        {
            _logger.LogCritical($"{nameof(RabbitMQSubscriber<TMessage>)}.{nameof(OnConsume)} Failed to route message to consumer from Exchange: {arguments?.Exchange}, Routing Key: {arguments?.RoutingKey} to Queue: {_queueProperties.Name}. Exception! {ex}");
            if (!_autoAck && arguments != null)
            {
                _logger.LogInformation($"{nameof(RabbitMQSubscriber<TMessage>)}.{nameof(OnConsume)} Requeing the message, from Exchange: {arguments?.Exchange}, Routing Key: {arguments?.RoutingKey} to Queue: {_queueProperties.Name} Rabbit Message Consumer");
                _channel?.Nack(arguments.DeliveryTag, false, ex is ChannelDisposingException /* Requeue if this exception is caused by channel being disposed so any transient issues are handles*/);
            }
        }
    }
    private async Task OnSubscriberDisconnected(object sender, RabbitMQSubscriberDisconnectedEventArgs e)
    {
        _logger.LogInformation($"Subscription Disconnected to {_queueProperties.Name}, consumer Tag {e.SubscriberID}");
        if (!e.IsSubscriberRunning && IsDisposeOrDisposing())
            return;
        // Try to Reconnect, if the subscription drops due to interruptions
        // possible cases:
        // 1 : We are disposing the channel
        // 2 : the queue being deleted
        // 3 : in a clustered scenario, the node on which the queue is located failing, will cause the consumption to be cancelled
        //      , but the client channel will not be informed, hence trying to reconnect
        try
        {
            await Connect();
        }
        catch (Exception ex)
        {
            _logger.LogCritical($"{nameof(RabbitMQSubscriber<TMessage>)}.{nameof(OnSubscriberDisconnected)} Couldn't reconnect the subscriber. Exception! {ex}");
        }
    }
    private async Task OnChannelDisconnected(object sender, RabbitMQChannelDisconnectedEventArgs e)
    {
        _logger.LogInformation($"{nameof(RabbitMQSubscriber<TMessage>)}.{nameof(OnChannelDisconnected)} Subscription Channel Disconnected for queue {_queueProperties.Name}");
        DisposeChannel();
        try
        {
            await Connect();
        }
        catch (Exception ex)
        {
            _logger.LogCritical($"{nameof(RabbitMQSubscriber<TMessage>)}.{nameof(OnChannelDisconnected)} Couldn't reconnect the subscriber to queue {_queueProperties.Name}. Exception! {ex}");
        }
    }
    private void EnsureNotDisposing()
    {
        if (IsDisposeOrDisposing())
            throw new ChannelDisposingException($"Subscriber is already dispose/disposing");
    }
    private bool IsDisposeOrDisposing()
    {
        return IsDisposed || IsDisposing;
    }
    protected override async Task Disposing()
    {
        DisposeChannel();
        await base.Disposing();
    }
    private void DisposeChannel()
    {
        try
        {
            if (_channel != null)
                _channel.Dispose();
            _channel = null;
            _consumerTag = null;
        }
        catch (Exception e)
        {
            _logger.LogCritical($"{nameof(RabbitMQSubscriber<TMessage>)}.{nameof(DisposeChannel)} Failed to dispose channel channel {_channel?.Id}. Exception! {e}");
        }
    }
}
