using System;
using System.Threading.Tasks;

namespace RabbitMq.Core.Interfaces
{
    public interface IPublishingBus : IDisposable
    {
        /// <summary>
        ///     Publishes the specified message. Delivers the event to the registered event handler.
        /// </summary>
        /// <typeparam name="TMessage">The type of the message.</typeparam>
        /// <param name="exchange">The exchange.</param>
        /// <param name="routingKey">The routing key.</param>
        /// <param name="message">The message.</param>
        /// <param name="configurations">The publising configurations.</param>
        Task Publish<TMessage>(string exchange, string routingKey, TMessage message, Action<IPublishingProperties> configurations = null)
            where TMessage : class;

        /// <summary>
        /// Send the request to the specified queue, and complete the response task when the response is received.
        /// </summary>
        /// <typeparam name="TRequest">Request Type</typeparam>
        /// <typeparam name="TResponse">Response Type</typeparam>        
        /// <param name="exchange">Exchange Name</param>
        /// <param name="routingKey">Routing Key</param>
        /// <param name="requestMessage">Request Message</param>
        /// <param name="configurations">Properties action for Requesting</param>
        /// <returns>The async <see cref="Task{TResponse}"/> response.</returns>
        Task<TResponse> Request<TRequest, TResponse>(string exchange, string routingKey, TRequest requestMessage, Action<IRequestResponseProperties> configurations = null)
            where TRequest : class
            where TResponse : class;
    }
}