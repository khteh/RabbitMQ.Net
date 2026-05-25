using System;
using System.Threading.Tasks;

namespace RabbitMq.Core.Interfaces
{
    public interface IRabbitMqConsumer<TMessage> where TMessage : class
    {
        /// <summary>
        /// Method which consumes the Message from RabbitMq
        /// </summary>
        Task Consume(IRabbitMqConsumeContext<TMessage> consumeContext);
    }
}