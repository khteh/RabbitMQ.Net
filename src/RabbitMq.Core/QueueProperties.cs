using System;
using RabbitMq.Core.Interfaces;

namespace RabbitMq.Core
{
    public class QueueProperties : IQueueProperties
    {
        public bool Temporary { get; set; }
        public bool Durable { get; set; }
        public bool Exclusive { get; set; }
        public bool AutoDelete { get; set; }
        public string Name { get; set; }
    }
}