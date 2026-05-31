using System.Text.Json.Serialization;
using RabbitMQ.Core.Interfaces;
namespace RabbitMQ.Core;

[Serializable]
public class TestMessage : IMessage
{
    public string Message { get; set; }
    public DateTimeOffset Timestamp { get; set; }
    [JsonConstructor]
    public TestMessage(string message, DateTimeOffset timestamp) => (Message, Timestamp) = (message, timestamp);
}
