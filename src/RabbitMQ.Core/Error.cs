using System;

namespace RabbitMQ.Core
{
    public sealed class Error
    {
        public string Code { get; private set; }
        public string Description { get; private set; }

        public Error(string code, string description)
        {
            Code = code;
            Description = description;
        }
        public override string ToString() => $"Code: {Code}, Description: {Description}";
    }
}