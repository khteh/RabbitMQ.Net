using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using RabbitMQ.Core;
using RabbitMQ.Core.Interfaces;

public class MessageConverter : JsonConverter<IMessage>
{
    public override IMessage Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        // Copy the reader to look ahead into the JSON token document

        using JsonDocument doc = JsonDocument.ParseValue(ref reader);
        JsonElement root = doc.RootElement;
        // Look for a unique property to determine the type
        if (root.TryGetProperty("Message", out _) && root.TryGetProperty("Timestamp", out _))
            //return JsonSerializer.Deserialize<TestMessage>(root.GetRawText(), options);
            return new TestMessage(root.GetProperty("Message").GetString(), root.GetProperty("Timestamp").GetDateTimeOffset());
        throw new JsonException($"Unknown message type. JSON: {root.GetRawText()}");
    }

    public override void Write(Utf8JsonWriter writer, IMessage value, JsonSerializerOptions options) =>
        JsonSerializer.Serialize(writer, value, value.GetType(), options);
}
