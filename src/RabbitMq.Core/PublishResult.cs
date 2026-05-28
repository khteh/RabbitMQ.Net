namespace RabbitMq.Core;

public class PublishResult
{
    public bool Success { get; private set; } = false;
    public ulong DeliveryTag { get; private set; }
    //
    // Summary:
    //     Whether this acknoledgement applies to one message or multiple messages.
    public bool Multiple { get; private set; }
    public List<Error> Errors { get; private set; } = new List<Error>();
    public PublishResult(bool success, ulong tag, bool isMultiple, List<Error> errors)
    {
        Success = success;
        DeliveryTag = tag;
        Multiple = isMultiple;
        Errors = errors;
    }
}
