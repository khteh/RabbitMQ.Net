using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using RabbitMq.Core.Events;
using RabbitMq.Core.Exceptions;
using RabbitMq.Core.Interfaces;
using RabbitMQ.Client.Events;

namespace RabbitMq.Core.Consumer;

public sealed class RabbitMqSubscriber<TMessage> : DisposableObject, IRabbitMqSubscriber<TMessage>
    where TMessage : class
{
    private IRabbitMqChannel _channel;
    private IRabbitMqConnection _connection;
    private bool _autoAck = false;
    private ISubscriberProperties _properties;
    private IQueueProperties _queueProperties;
    private string _consumerTag;
    private readonly ILogger<RabbitMqSubscriber<TMessage>> _logger;
    public readonly AsyncRetryPolicy _subscriptionRetryPolicy;
    private readonly IRabbitMqConsumer<TMessage> _consumer;
    public RabbitMqSubscriber(ILogger<RabbitMqSubscriber<TMessage>> logger, IRabbitMqConsumer<TMessage> consumer,
                ISubscriberProperties properties, IQueueProperties queueProperties, bool autoAck, IRabbitMqConnection connection, AsyncRetryPolicy policy)
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
        await _subscriptionRetryPolicy.ExecuteAsync(() => Task.Run(() => Subscribe()));
    }

    public async Task OnConsume(object sender, BasicDeliverEventArgs arguments)
    {
        try
        {
            _logger.LogInformation("Received message from Exchange: {Exchange}, Routing Key : {RoutingKey} to Queue: {QueueName}, on channel:{ChannelId}, Redelivered :{Redelivered}, MessageId:{MessageId}",
                arguments?.Exchange,
                arguments?.RoutingKey,
                _queueProperties.Name,
                _channel?.Id,
                arguments?.Redelivered,
                arguments?.BasicProperties?.MessageId
                );
            EnsureNotDisposing();
            // _logger.LogInformation($"Received message from Exchange: {arguments?.Exchange}, Routing Key : {arguments?.RoutingKey} to Queue: {_queueProperties.Name}, on channel:{_channel?.Id}, Redelivered :{arguments?.Redelivered}, MessageId:{arguments?.BasicProperties?.MessageId}");
            var jsonContext = new RabbitMqConsumerContext(_queueProperties.Name, arguments, _channel, _autoAck, _logger);
            IRabbitMqConsumeContext<TMessage> consumeContext = jsonContext.GetConsumeContext<TMessage>();
            //using (var scope = _container.CreateScope())
            //{
            //var consumer = _container.GetRequiredService<IRabbitMqConsumer<TMessage>>();
            // Call async consumer via the sync Rabbit Mq consumer
            EnsureNotDisposing();
            try
            {
                _consumer.Consume(consumeContext).ConfigureAwait(false).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"Rabbit Mq Consumer Failed, type {_consumer.GetType()}, exception:{ex}");
                // Ignore Consumer Exceptions
            }
            //}
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Failed to route message to consumer from Exchange: {Exchange}, Routing Key : {RoutingKey} to Queue: {QueueName} Rabbit Message Consumer, exception:{@Exception}",
                arguments?.Exchange,
                arguments?.RoutingKey,
                _queueProperties.Name,
                ex);
            if (!_autoAck && arguments != null)
            {
                _logger.LogInformation("Requeing the message, from Exchange: {Exchange}, Routing Key : {RoutingKey} to Queue: {QueueName} Rabbit Message Consumer",
                    arguments?.Exchange,
                    arguments?.RoutingKey,
                    _queueProperties.Name);
                _channel?.Nack(arguments.DeliveryTag, false, ex is ChannelDisposingException /* Requeue if this exception is caused by channel being disposed so any transient issues are handles*/);
            }
        }
    }

    protected override void Disposing()
    {
        DisposeChannel();
        base.Disposing();
    }


    private async Task Subscribe()
    {
        _logger.LogInformation($"Trying to subscribe to Exchange: {_properties.Exchange}, ExchangeType: {_properties.ExchangeType}, Queue: {_queueProperties.Name}, autoAck {_autoAck}, {_properties.Bindings.Count()} bindings {(_properties.Bindings.Count() == 1 ? _properties.Bindings.First() : string.Empty)}");
        try
        {
            DisposeChannel();
            _channel = await _connection.CreateChannel(_properties.Exchange, _properties.ExchangeType, _queueProperties);
            _consumerTag = await _channel.Subscribe(_autoAck, OnConsume, OnSubscriberDisconnected, _properties.Bindings);
            // Only List on disconnected if we are able to subscribe to it
            _channel.Disconnected += OnChannelDisconnected;
            _logger.LogInformation(
                $"Subscribed to {_queueProperties.Name}, autoAck {_autoAck}, consumer Tag : {_consumerTag} on channel {_channel.Id}");
        }
        catch (Exception)
        {
            if (_channel != null)
                _channel.Disconnected -= OnChannelDisconnected;
            DisposeChannel();
            throw;
        }
    }
    private async Task OnSubscriberDisconnected(object sender, RabbitMqSubscriberDisconnectedEventArgs e)
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
            Connect().ConfigureAwait(false).GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            _logger.LogWarning($"Couldn't reconnect the subscriber. ex {ex}");
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

    private async Task OnChannelDisconnected(object sender, RabbitMqChannelDisconnectedEventArgs e)
    {
        _logger.LogInformation($"Subscription Channel Disconnected for queue {_queueProperties.Name}");

        DisposeChannel();
        try
        {
            Connect().ConfigureAwait(false).GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            _logger.LogError($"Couldn't reconnect the subscriber. ex {ex}");
        }
    }

    private void DisposeChannel()
    {
        try
        {
            if (_channel != null)
            {
                _channel.Dispose();
            }
            _channel = null;
            _consumerTag = null;
        }
        catch (Exception)
        {
        }
    }
}
