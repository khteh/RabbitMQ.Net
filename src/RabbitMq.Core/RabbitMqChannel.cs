using System.Net;
using System.Reflection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMq.Core.Events;
using RabbitMq.Core.Exceptions;
using RabbitMq.Core.Extensions;
using RabbitMq.Core.Interfaces;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text.Json;
using System.Text;
namespace RabbitMq.Core;

public class RabbitMqChannel : DisposableObject, IRabbitMqChannel
{
    private IChannel _channel;
    private static readonly SemaphoreSlim _channelLock = new SemaphoreSlim(1, 1);
    private string _channelId, _exchange, _queue;
    private IDictionary<string, AsyncEventingBasicConsumer> _consumers;
    private readonly ILogger<RabbitMqChannel> _logger;

    public event AsyncEventHandler<RabbitMqChannelDisconnectedEventArgs> Disconnected;

    public RabbitMqChannel(ILogger<RabbitMqChannel> logger, IChannel channel, string exchange, string type, IQueueProperties queueProperties, string queueName)
    {
        _channel = channel;
        _channelId = _channel.ChannelNumber.ToString();
        _logger = logger;
        _consumers = new Dictionary<string, AsyncEventingBasicConsumer>();
        _queue = queueName;
        _channel.ChannelShutdownAsync += OnChannelShutdown;
        _exchange = exchange;
        _logger.LogInformation($"Channel Created {_channel.ChannelNumber}, Queue: {_queue}");
    }

    public string Id
    {
        get => _channelId;
    }

    private async Task OnChannelShutdown(object sender, ShutdownEventArgs e)
    {
        _logger.LogInformation($"Channel Shutdown {_channelId}, Reason: {e.Cause}, Reply Text: {e.ReplyText}");
        //publish the event up, so that host can wireup the new channels
        Disconnected?.Invoke(this, new RabbitMqChannelDisconnectedEventArgs());
    }
    public async Task<PublishResult> Publish<TMessage>(string exchange, string routingKey, TMessage message, Action<IPublishingProperties> configuration)
        where TMessage : class
    {
        var properties = new PublishingProperties();
        properties.Exchange = exchange;
        properties.RoutingKey = routingKey;

        // Apply Custom settings
        configuration?.Invoke(properties);

        return await Publish<TMessage>(message, properties);
    }

    public async Task<PublishResult> Publish<TMessage>(TMessage message, IPublishingProperties properties) where TMessage : class
    {
        EnsureChannelHealthy();
        var body = JsonSerializer.SerializeToUtf8Bytes(message);//properties.SerializeMessage(message);

        // Set a default time interval for the wait
        TaskCompletionSource<PublishResult> returnReceivedTask = new TaskCompletionSource<PublishResult>(default(TaskCreationOptions));

        AsyncEventHandler<BasicReturnEventArgs> basicReturnHandler = async (o, e) =>
        {
            returnReceivedTask.TrySetException(new InvalidOperationException($"Publish message returned Code: {e.ReplyCode}, Reply: {e.ReplyText}, Exchange: {properties.Exchange}, ExchangeType: {properties.ExchangeType}, RoutingKey: {properties.RoutingKey}, Queue: {properties.Queue}"));
        };

        AsyncEventHandler<BasicAckEventArgs> basicAckHandler = async (o, e) =>
        {
            _logger.LogInformation($"Published Message Accepted with delivery Tag : {e.DeliveryTag}");
            returnReceivedTask.TrySetResult(new PublishResult(true, e.DeliveryTag, e.Multiple, null));
        };

        if (properties.EnablePublisherConfirm)
        {
            await _channelLock.WaitAsync();
            try
            {
                _channel.BasicAcksAsync += basicAckHandler;
            }
            finally
            {
                _channelLock.Release();
            }
        }
        if (properties.EnsureDeliveryToQueue)
        {
            await _channelLock.WaitAsync();
            try
            {
                _channel.BasicReturnAsync += basicReturnHandler;
            }
            finally
            {
                _channelLock.Release();
            }
        }
        else if (!properties.EnablePublisherConfirm && !properties.EnsureDeliveryToQueue)
        {
            returnReceivedTask.TrySetResult(new PublishResult(true, 0, false, null));
        }
        await _channelLock.WaitAsync();
        try
        {
            BasicProperties p = new BasicProperties { Persistent = true };
            properties.CopyTo(p);
            await _channel.BasicPublishAsync(properties.Exchange, properties.RoutingKey, properties.EnsureDeliveryToQueue, p, body);
        }
        finally
        {
            _channelLock.Release();
        }
        using (CancellationTokenSource ct = new CancellationTokenSource(properties.PublishReturnWaitTime))
            try
            {
                ct.Token.Register(async () =>
                {
                    if (_channel != null)
                    {
                        if (properties.EnsureDeliveryToQueue)
                        {
                            await _channelLock.WaitAsync();
                            try
                            {
                                _channel.BasicReturnAsync -= basicReturnHandler;
                            }
                            finally
                            {
                                _channelLock.Release();
                            }
                        }
                        if (properties.EnablePublisherConfirm)
                        {
                            await _channelLock.WaitAsync();
                            try
                            {
                                _channel.BasicAcksAsync -= basicAckHandler;
                            }
                            finally
                            {
                                _channelLock.Release();
                            }
                        }
                    }
                    returnReceivedTask.TrySetException(new TimeoutException($"Publishing message of type {typeof(TMessage).Name} didn't get accepted in time {properties.PublishReturnWaitTime}"));
                }, useSynchronizationContext: false);
                var result = await returnReceivedTask.Task.ConfigureAwait(false);
                _logger.LogInformation($"Published Message type {typeof(TMessage).Name}, result:{result}");
                return result;
            }
            catch (Exception e)
            {
                _logger.LogCritical($"{nameof(RabbitMqChannel)}.{nameof(Publish)} Exception! {e.Message} {e.GetInnerMessage()} {e.StackTrace}");
                return new PublishResult(false, 0, false, new List<Error>() { new Error(HttpStatusCode.InternalServerError.ToString(), $"Exception! {e.Message} {e.GetInnerMessage()} {e.StackTrace}") });
            }
    }

    public async Task Respond<TResponse>(BasicDeliverEventArgs consumeArgument, TResponse response, Action<IPublishingProperties> configuration)
        where TResponse : class
    {
        EnsureChannelHealthy();
        if (consumeArgument.BasicProperties == null)
            throw new InvalidOperationException("Message's Properties is missing");

        if (string.IsNullOrWhiteSpace(consumeArgument.BasicProperties.ReplyTo))
            throw new InvalidOperationException("Cannot Respond to the Request, as Requester hasn't specified Message's ReplyTo Properties");

        Action<IPublishingProperties> configurationAction = (prop) =>
        {
            configuration?.Invoke(prop);
            prop.CorrelationId = consumeArgument.BasicProperties.CorrelationId;
            // Since we are using Direct ReplyTo, we cannot use Mandatory flag
            // Indicates to use pseudo-queue amq.rabbitmq.reply - to as replyTo
            // https://www.rabbitmq.com/direct-reply-to.html
            prop.EnsureDeliveryToQueue = false;
        };
        await Publish<TResponse>(string.Empty, consumeArgument.BasicProperties.ReplyTo, response, configurationAction);
    }
    public async Task<string> Subscribe(bool autoAck, AsyncEventHandler<BasicDeliverEventArgs> onConsume, AsyncEventHandler<RabbitMqSubscriberDisconnectedEventArgs> onDisconnected, List<string> bindings)
    {
        try
        {
            EnsureChannelHealthy();
            await _channelLock.WaitAsync();
            try
            {
                if (!_channel.IsOpen)
                    throw new InvalidOperationException($"Channel is closed, cannot subscribe to queue {_queue} exchange {_exchange}");
                if (!bindings.Any())
                    throw new InvalidOperationException($"{nameof(RabbitMqChannel)}.{nameof(Subscribe)} Cannot subscribe without any binding!");
                foreach (string key in bindings)
                {
                    _logger.LogInformation($"{nameof(RabbitMqChannel)}.{nameof(Subscribe)} to {key}");
                    await _channel.QueueBindAsync(queue: _queue, exchange: _exchange, routingKey: key);
                }
                AsyncEventingBasicConsumer rabbitMqConsumer = new AsyncEventingBasicConsumer(_channel);
                rabbitMqConsumer.ReceivedAsync += onConsume;
                rabbitMqConsumer.UnregisteredAsync += async (sender, eventArg) =>
                {
                    onDisconnected?.Invoke(sender, new RabbitMqSubscriberDisconnectedEventArgs() { ConsumerTags = eventArg.ConsumerTags, IsSubscriberRunning = rabbitMqConsumer.IsRunning });
                };
                var consumerTag = await _channel.BasicConsumeAsync(_queue, autoAck, rabbitMqConsumer);
                _consumers.Add(consumerTag, rabbitMqConsumer);
                return consumerTag;
            }
            finally
            {
                _channelLock.Release();
            }
        }
        catch (Exception e)
        {
            _logger.LogError($"{nameof(RabbitMqChannel)}.{nameof(Subscribe)} Exception! {e.Message} {e.GetInnerMessage()} {e.StackTrace}");
            throw;
        }
    }
    public async Task<TResponse> Request<TMessage, TResponse>(string exchange, string routingKey, TMessage message, Action<IRequestResponseProperties> configuration)
        where TMessage : class
        where TResponse : class
    {
        var properties = new RequestResponseProperties();
        properties.Exchange = exchange;
        properties.RoutingKey = routingKey;

        // Apply Custom settings
        configuration?.Invoke(properties);
        return await Request<TMessage, TResponse>(message, properties);
    }

    public async Task<TResponse> Request<TMessage, TResponse>(TMessage message, RequestResponseProperties properties)
        where TMessage : class
        where TResponse : class
    {
        EnsureChannelHealthy();
        TaskCompletionSource<TResponse> responseReceivedTask = new TaskCompletionSource<TResponse>(default(TaskCreationOptions));

        // Using Direct Reply To
        string replyQueueName = "amq.rabbitmq.reply-to";

        // Set the Reply queueName for the responder
        properties.ReplyTo = replyQueueName;
        var correlationId = Guid.NewGuid().ToString();
        // Set Explicit CorrelationId so that we can differentiate between true response
        properties.CorrelationId = correlationId;

        // Subscribe for the response to come
        Subscribe(true, async (o, deliveryArguments) =>
        {
            try
            {
                // Reject response for duplicate message
                if (deliveryArguments.BasicProperties.CorrelationId != correlationId)
                {
                    _logger.LogWarning($"Ignoring response from queue:{replyQueueName} with a not matching correlationId received:{deliveryArguments.BasicProperties.CorrelationId}, expected:{correlationId}");
                    return;
                }
                // 1. Extract the body (and copy if using asynchronously)
                byte[] body = deliveryArguments.Body.ToArray();
                string message = Encoding.UTF8.GetString(body);

                // 2. Access metadata
                string routingKey = deliveryArguments.RoutingKey;
                var props = deliveryArguments.BasicProperties;

                // 3. Deserialize the response
                responseReceivedTask.TrySetResult(JsonSerializer.Deserialize<TResponse>(body));
            }
            catch (Exception ex)
            {
                responseReceivedTask.TrySetException(ex);
            }
        },
        async (o, cancellationArg) =>
        {
            RabbitMqSubscriberDisconnectedEventArgs args = cancellationArg as RabbitMqSubscriberDisconnectedEventArgs;
            responseReceivedTask.TrySetException(new InvalidOperationException($"The Subscription has been cancelled. subscriber tags: {string.Join(",", args.ConsumerTags)}, is subscriber running: {args.IsSubscriberRunning}"));
        }, new List<string>() { properties.RoutingKey }
        );

        // Publish the message and wait for the response
        await Publish<TMessage>(message, properties);
        CancellationTokenSource ct = null;
        try
        {
            ct = new CancellationTokenSource(properties.ReplyWaitTime);
            ct.Token.Register(() => responseReceivedTask.TrySetException(new TimeoutException($"Responder on the queue {replyQueueName}, didn't respond in time")), useSynchronizationContext: false);

            return await responseReceivedTask.Task;
        }
        finally
        {
            ct?.Dispose();
        }
    }
    public async Task Ack(ulong deliveryTag, bool ackMultipleMessages)
    {
        EnsureChannelHealthy();
        _logger.LogInformation($"Ack message: {deliveryTag}, on channel {_channel.ChannelNumber}");

        await _channelLock.WaitAsync();
        try
        {
            if (_channel.IsOpen)
                await _channel.BasicAckAsync(deliveryTag, ackMultipleMessages);
        }
        finally
        {
            _channelLock.Release();
        }
    }
    public async Task Nack(ulong deliveryTag, bool nackMultipleMessages, bool reQueue)
    {
        EnsureChannelHealthy();
        _logger.LogWarning($"Nack message:{deliveryTag} re-queue:{reQueue}, on channel {_channel.ChannelNumber}");
        await _channelLock.WaitAsync();
        try
        {
            if (_channel.IsOpen)
                await _channel.BasicNackAsync(deliveryTag, nackMultipleMessages, reQueue);
        }
        catch (Exception ackEx)
        {
            _logger.LogError($"An error occurred trying to NACK a message with delivery tag: {deliveryTag}, exception: {ackEx}");
        }
        finally
        {
            _channelLock.Release();
        }
    }
    protected override void Disposing()
    {
        int channelId = _channel?.ChannelNumber ?? -1;
        using (var cancellationTokenSource = new CancellationTokenSource())
        {
            var disposeTask = Task.Run(() =>
            {
                try
                {
                    if (_channel != null && _channel.IsOpen)
                    {
                        lock (_channelLock)
                        {
                            _channel.ChannelShutdownAsync -= OnChannelShutdown;
                            //_channel?.Close(200, $"{nameof(RabbitMqChannel)} Disposing");
                        }
                    }

                    _logger.LogInformation("Channel Id:{ChannelId} dispose succeeded", channelId);
                }
                catch (Exception exception)
                {
                    _logger.LogInformation("Channel Id:{ChannelId} failed to dispose. Exception:{@Exception}", channelId, exception);
                }
            });

            if (!disposeTask.Wait(TimeSpan.FromSeconds(2)))
            {
                cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(2));
                _logger.LogInformation("Channel Id:{ChannelId} failed to dispose within time limit.", channelId);
            }
        }
    }

    private bool IsChannelHealthy()
    {
        if (_channel != null && _channel.IsOpen)
            return true;
        return false;
    }

    private void EnsureChannelHealthy()
    {
        if (IsChannelHealthy())
            return;
        throw new ChannelNotAvailableException();
    }
}