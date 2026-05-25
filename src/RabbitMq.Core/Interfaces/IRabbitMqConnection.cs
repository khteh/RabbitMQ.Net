using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMq.Core.Events;
using RabbitMQ.Client;

namespace RabbitMq.Core.Interfaces
{
    public interface IRabbitMqConnection : IDisposable
    {
        /// <summary>
        /// Creates a Rabbit Mq Channel
        /// </summary>
        /// <returns>Rabbit Mq Channel</returns>
        Task<IRabbitMqChannel> CreateChannel(string exchange, string type, IQueueProperties properties);

        /// <summary>
        /// Connects to the RabbitMq Endpoint
        /// </summary>
        Task<IConnection> Start();

        /// <summary>
        /// Shows the state of the connection
        /// </summary>
        bool IsConnected();

        /// <summary>
        /// Provides callbacks to consumer when the connection is estabilished
        /// This would mean recreating channels for communication
        /// </summary>
        event EventHandler<RabbitMqConnectedEventArgs> Connected;

        /// <summary>
        /// Provides callbacks to consumers when the connection is down
        /// This would mean discarding the existing channels in use
        /// </summary>
        event EventHandler<RabbitMqDisconnectedEventArgs> Disconnected;
    }
}