using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;
using RabbitMq.Core.Extensions;
using RabbitMq.Core.Interfaces;

namespace RabbitMq.Core.Consumer
{
    public sealed class RabbitMqSubscriberFactory<TMessage> : IRabbitMqSubscriberFactory<TMessage>
        where TMessage : class
    {
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger<RabbitMqSubscriberFactory<TMessage>> _logger;
        private Func<PolicyBuilder, AsyncRetryPolicy> _subscribeRetryPolicyAction;
        public RabbitMqSubscriberFactory(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory;
            _logger = _loggerFactory.CreateLogger<RabbitMqSubscriberFactory<TMessage>>();
            //_subscribeRetryPolicyAction = (policy) => policy.WaitAndRetryAsync(6, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)) + TimeSpan.FromMilliseconds(new Random().Next(0, 100)) );
            _subscribeRetryPolicyAction = (policy) => policy.WaitAndRetryAsync(new[]
                                               {
                                                TimeSpan.FromSeconds(1),
                                                TimeSpan.FromSeconds(2),
                                                TimeSpan.FromSeconds(3)
                                              }, (ex, t, i, context) =>
                                              {
                                                  _logger.LogInformation($"Wait And Retry Async: Subscribe retry count {i}, ex:{ex}");
                                              });
        }

        /// <summary>
        /// Gets a Typed Subscriber
        /// Note : The reference to this is via dynamic
        /// </summary>
        /// <param name="queueName">Queue Name</param>
        /// <param name="autoAck">Auto ACK</param>
        /// <param name="connection">Connection</param>
        /// <param name="retryPolicyBuilderAction">Retry Policy</param>
        /// <param name="messageSerializer">Message Serializer</param>
        /// <returns></returns>
        public IRabbitMqSubscriber<TMessage> GetRabbitMqSubscriber(ISubscriberProperties properties, IQueueProperties queueProperties, bool autoAck,
                IRabbitMqConnection connection, IRabbitMqConsumer<TMessage> consumer,
                Func<PolicyBuilder, AsyncRetryPolicy> retryPolicyBuilderAction)
        {
            PolicyBuilder policyBuilder = Policy.Handle<Exception>(ex => ex.IsTransientRabbitMqException());
            AsyncRetryPolicy retryPolicy = retryPolicyBuilderAction != null ? retryPolicyBuilderAction(policyBuilder) : _subscribeRetryPolicyAction(policyBuilder);
            // Circuit breaker to avoid continous retries, close the circuit for 5 min
            AsyncCircuitBreakerPolicy circuitBreakerPolicy = Policy
                .Handle<Exception>(ex => ex.IsTransientRabbitMqException())
                .CircuitBreakerAsync(3,
                TimeSpan.FromMinutes(5),
                (ex, t) => { _logger.LogInformation($"Circuit Breaker:Subscribing to queue:{queueProperties.Name}, retries have been broken for the break Time"); },
                () => { _logger.LogInformation($"Circuit Breaker:Subscribing to queue:{queueProperties.Name}, retries have been restored after the break Time"); });
            return new RabbitMqSubscriber<TMessage>(_loggerFactory.CreateLogger<RabbitMqSubscriber<TMessage>>(), consumer, properties, queueProperties, autoAck, connection, retryPolicy);
        }
    }
}