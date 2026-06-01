using Microsoft.Extensions.Logging;
using RabbitMQ.Core.Events;
using RabbitMQ.Core.Exceptions;
using RabbitMQ.Core.Extensions;
using RabbitMQ.Core.Interfaces;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text.Json;
using System.Text;
using System.Buffers.Binary;
namespace RabbitMQ.Core;

public class RabbitMQChannel : DisposableObject, IRabbitMQChannel
{
    private IChannel _channel;
    private static readonly SemaphoreSlim _channelLock = new SemaphoreSlim(1, 1);
    private string _channelId, _exchange, _queue;
    private IDictionary<string, AsyncEventingBasicConsumer> _consumers;
    private readonly ILogger<RabbitMQChannel> _logger;
    public event AsyncEventHandler<RabbitMQChannelDisconnectedEventArgs> Disconnected;
    public string Id
    {
        get => _channelId;
    }
    public RabbitMQChannel(ILogger<RabbitMQChannel> logger, IChannel channel, string exchange, string queueName)
    {
        _channel = channel;
        _channelId = _channel.ChannelNumber.ToString();
        _logger = logger;
        _queue = queueName;
        _exchange = exchange;
        _consumers = new Dictionary<string, AsyncEventingBasicConsumer>();
        _channel.ChannelShutdownAsync += OnChannelShutdown;
        _logger.LogDebug($"{nameof(RabbitMQChannel)} {_channel.ChannelNumber}, Queue: {_queue}");
    }
    private async Task OnChannelShutdown(object sender, ShutdownEventArgs e)
    {
        _logger.LogInformation($"Channel Shutdown {_channelId}, Reason: {e.Cause}, Reply Text: {e.ReplyText}");
        //publish the event up, so that host can wireup the new channels
        Disconnected?.Invoke(this, new RabbitMQChannelDisconnectedEventArgs());
    }
    public async Task<PublishResult> Publish<TMessage>(string exchange, string routingKey, TMessage message, Action<IPublishingProperties> configuration)
        where TMessage : class
    {
        PublishingProperties properties = new PublishingProperties()
        {
            Exchange = exchange,
            RoutingKey = routingKey
        };

        // Apply Custom settings
        configuration?.Invoke(properties);
        return await Publish<TMessage>(message, properties);
    }
    /// <summary>
    /// https://www.rabbitmq.com/tutorials/tutorial-seven-dotnet
    /// Publish a message as usual and wait for its confirmation by await-ing the task returned by BasicPublishAsync. 
    /// The await returns as soon as the message has been confirmed. 
    /// If the message is is nack-ed or returned (meaning the broker could not take care of it for some reason), the method will throw an exception. 
    /// The handling of the exception usually consists in logging an error message and/or retrying to send the message.
    /// </summary>
    /// <typeparam name="TMessage"></typeparam>
    /// <param name="message"></param>
    /// <param name="properties"></param>
    /// <returns></returns>
    public async Task<PublishResult> Publish<TMessage>(TMessage message, IPublishingProperties properties) where TMessage : class
    {
        EnsureChannelHealthy();
        byte[] body = JsonSerializer.SerializeToUtf8Bytes(message);
        // Set a default time interval for the wait
        TaskCompletionSource<PublishResult> returnReceivedTask = new TaskCompletionSource<PublishResult>();
        AsyncEventHandler<BasicReturnEventArgs> basicReturnHandler = async (o, e) =>
        {
            ulong sequenceNumber = 0;
            IReadOnlyBasicProperties props = e.BasicProperties;
            if (props.Headers is not null)
            {
                object? maybeSeqNum = props.Headers[Constants.PublishSequenceNumberHeader];
                if (maybeSeqNum is not null)
                    sequenceNumber = BinaryPrimitives.ReadUInt64BigEndian((byte[])maybeSeqNum);
            }
            _logger.LogError($"Message sequence number {sequenceNumber} has been basic.return-ed! Reply: {e.ReplyCode}, {e.ReplyText}, Exchange: {properties.Exchange}, ExchangeType: {properties.ExchangeType}, RoutingKey: {properties.RoutingKey}, Queue: {properties.Queue}");
            returnReceivedTask.TrySetException(new InvalidOperationException($"Message sequence number {sequenceNumber} has been basic.return-ed! Reply: {e.ReplyCode}, {e.ReplyText}, Exchange: {properties.Exchange}, ExchangeType: {properties.ExchangeType}, RoutingKey: {properties.RoutingKey}, Queue: {properties.Queue}"));
        };
        AsyncEventHandler<BasicNackEventArgs> basicNackHandler = async (o, e) =>
        {
            _logger.LogError($"Published Message NACK-ed with delivery Tag {e.DeliveryTag}, multiple: {e.Multiple}");
            returnReceivedTask.TrySetException(new InvalidOperationException($"Published Message NACK-ed with delivery Tag {e.DeliveryTag}, multiple: {e.Multiple}, Exchange: {properties.Exchange}, ExchangeType: {properties.ExchangeType}, RoutingKey: {properties.RoutingKey}, Queue: {properties.Queue}"));
        };
        AsyncEventHandler<BasicAckEventArgs> basicAckHandler = async (o, e) =>
        {
            _logger.LogDebug($"Published Message Accepted with delivery Tag {e.DeliveryTag}, multiple: {e.Multiple}");
            returnReceivedTask.TrySetResult(new PublishResult(true, e.DeliveryTag, e.Multiple, null));
        };
        await _channelLock.WaitAsync();
        try
        {
            if (properties.EnablePublisherConfirm)
            {
                _channel.BasicAcksAsync += basicAckHandler;
                _channel.BasicNacksAsync += basicNackHandler;
            }
            else
            {
                _channel.BasicAcksAsync -= basicAckHandler;
                _channel.BasicNacksAsync -= basicNackHandler;
            }
            if (properties.EnsureDeliveryToQueue)
                _channel.BasicReturnAsync += basicReturnHandler;
            else
                _channel.BasicReturnAsync -= basicReturnHandler;
        }
        finally
        {
            _channelLock.Release();
        }

        if (!properties.EnablePublisherConfirm && !properties.EnsureDeliveryToQueue)
            returnReceivedTask.TrySetResult(new PublishResult(true, 0, false, null));
        await _channelLock.WaitAsync();
        try
        {
            BasicProperties p = new BasicProperties
            {
                ContentType = "text/plain",
                Persistent = properties.Persistent,
                DeliveryMode = properties.Persistent ? DeliveryModes.Persistent : DeliveryModes.Transient
            };
            properties.CopyTo(p);
            await _channel.BasicPublishAsync(properties.Exchange, properties.RoutingKey, properties.EnsureDeliveryToQueue, p, body);
        }
        finally
        {
            if (properties.EnablePublisherConfirm)
            {
                _channel.BasicAcksAsync -= basicAckHandler;
                _channel.BasicNacksAsync -= basicNackHandler;
            }
            if (properties.EnsureDeliveryToQueue)
                _channel.BasicReturnAsync -= basicReturnHandler;
            _channelLock.Release();
        }
        PublishResult result = await returnReceivedTask.Task.ConfigureAwait(false);
        _logger.LogInformation($"{nameof(RabbitMQChannel)}.{nameof(Publish)} result:{result.Success}, deliveryTag: {result.DeliveryTag}, multiple: {result.Multiple}");
        return result;
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
    public async Task<string> Subscribe(bool autoAck, AsyncEventHandler<BasicDeliverEventArgs> onConsume, AsyncEventHandler<RabbitMQSubscriberDisconnectedEventArgs> onDisconnected, List<string> bindings)
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
                    throw new InvalidOperationException($"{nameof(RabbitMQChannel)}.{nameof(Subscribe)} Cannot subscribe without any binding!");
                foreach (string key in bindings)
                {
                    _logger.LogInformation($"{nameof(RabbitMQChannel)}.{nameof(Subscribe)} to {key}");
                    await _channel.QueueBindAsync(queue: _queue, exchange: _exchange, routingKey: key);
                }
                AsyncEventingBasicConsumer rabbitMqConsumer = new AsyncEventingBasicConsumer(_channel);
                rabbitMqConsumer.ReceivedAsync += onConsume;
                rabbitMqConsumer.UnregisteredAsync += async (sender, eventArg) =>
                {
                    onDisconnected?.Invoke(sender, new RabbitMQSubscriberDisconnectedEventArgs() { ConsumerTags = eventArg.ConsumerTags, IsSubscriberRunning = rabbitMqConsumer.IsRunning });
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
            _logger.LogError($"{nameof(RabbitMQChannel)}.{nameof(Subscribe)} Exception! {e.Message} {e.GetInnerMessage()} {e.StackTrace}");
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
            RabbitMQSubscriberDisconnectedEventArgs args = cancellationArg as RabbitMQSubscriberDisconnectedEventArgs;
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
                            //_channel?.Close(200, $"{nameof(RabbitMQChannel)} Disposing");
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