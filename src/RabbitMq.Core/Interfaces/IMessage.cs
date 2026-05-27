namespace RabbitMq.Core.Interfaces;

public interface IMessage
{
    string Message { get; set; }
    DateTimeOffset Timestamp { get; set; }
}