using System;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Core.Events
{
    public class RabbitMQSubscriberDisconnectedEventArgs : AsyncEventArgs
    {
        /// <summary>
        /// Subscriber Identifier
        /// </summary>
        public string SubscriberID { get; set; }
        public string[] ConsumerTags { get; set; }

        /// <summary>
        /// Is the Subscriber running
        /// </summary>
        public bool IsSubscriberRunning { get; set; }
    }
}