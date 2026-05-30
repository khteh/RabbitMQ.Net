using System.Text.Json.Serialization;
using RabbitMq.Core.Interfaces;
namespace RabbitMq.Core;

[Serializable]
public class TestMessage : IMessage
{
    public string Message { get; set; }
    public DateTimeOffset Timestamp { get; set; }
    [JsonConstructor]
    public TestMessage(string msg, DateTimeOffset timestamp)
    {
        Message = msg;
        Timestamp = timestamp;
    }
}
