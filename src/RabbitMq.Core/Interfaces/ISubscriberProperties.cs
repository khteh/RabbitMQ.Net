using System;
using System.Collections.Generic;

namespace RabbitMq.Core.Interfaces
{
    public interface ISubscriberProperties
    {
        string Exchange { get; set; }
        string ExchangeType {get; set; }
        List<string> Bindings { get; set; }
    }
}