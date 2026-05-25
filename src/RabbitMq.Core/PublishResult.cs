using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace RabbitMq.Core
{
    public class PublishResult
    {
        [JsonProperty]
        public bool Success { get; private set; } = false;
        [JsonProperty]
        public ulong DeliveryTag { get; private set; }
        //
        // Summary:
        //     Whether this acknoledgement applies to one message or multiple messages.
        [JsonProperty]
        public bool Multiple { get; private set; }
        [JsonProperty]
        public List<Error> Errors { get; private set; } = new List<Error>();
        public PublishResult(bool success, ulong tag, bool isMultiple, List<Error> errors)
        {
            Success = success;
            DeliveryTag = tag;
            Multiple = isMultiple;
            Errors = errors;
        }
    }
}