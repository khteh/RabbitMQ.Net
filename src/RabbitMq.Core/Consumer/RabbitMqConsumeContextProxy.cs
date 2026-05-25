using System;
using System.Threading.Tasks;
using RabbitMq.Core.Interfaces;
using RabbitMQ.Client;

namespace RabbitMq.Core.Consumer
{
    public abstract class RabbitMqConsumeContextProxy : IRabbitMqConsumeContext
    {
        private IRabbitMqConsumeContext _context;

        /// <summary>
        /// Creates an instance of RabbitMq Consume Proxy
        /// </summary>
        /// <param name="context">Original Context</param>
        protected RabbitMqConsumeContextProxy(IRabbitMqConsumeContext context) => _context = context;

#if false
        /// <inheritdoc/>
        public IBasicProperties BasicProperties
        {
            get
            {
                return _context.BasicProperties;
            }
        }
#endif
        /// <inheritdoc/>
        public string ConsumerTag
        {
            get => _context.ConsumerTag;
        }

        /// <inheritdoc/>
        public string Exchange
        {
            get => _context.Exchange;
        }

        /// <inheritdoc/>
        public string QueueName
        {
            get => _context.QueueName;
        }

        /// <inheritdoc/>
        public bool AutoAck
        {
            get => _context.AutoAck;
        }

        /// <inheritdoc/>
        public string RoutingKey
        {
            get => _context.RoutingKey;
        }

        /// <inheritdoc/>
        public void Ack() => _context.Ack();

        /// <inheritdoc/>
        public void Nack(bool reQueue) => _context.Nack(reQueue);

        /// <inheritdoc/>
        public async Task Respond<TResponse>(TResponse response)
            where TResponse : class
        {
            await _context.Respond<TResponse>(response);
        }

        /// <inheritdoc/>
        public IRabbitMqConsumeContext<T> GetConsumeContext<T>() 
            where T : class
        {
            return _context.GetConsumeContext<T>();
        }
    }
}