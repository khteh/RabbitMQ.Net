using System;
using RabbitMQ.Client.Events;

namespace RabbitMq.Core.Events
{
    public class RabbitMqSubscriberDisconnectedEventArgs : AsyncEventArgs
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