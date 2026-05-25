using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMq.Core;
using RabbitMq.Core.Interfaces;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using static System.Console;
namespace RabbitMq.Publisher;

class Program
{
    static async Task Main(string[] args)
    {
        // $ DOTNET_ENVIRONMENT=Development ./RabbitMq.Publisher
        string environment = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "Development";
        string severity = args.Length > 0 ? args[0] : "Anonymous.Info";
        string exchange = Environment.GetEnvironmentVariable("EXCHANGE");
        string message = Environment.GetEnvironmentVariable("MESSAGE");
        string routingKey = Environment.GetEnvironmentVariable("ROUTINGKEY");
        if (!string.IsNullOrEmpty(routingKey))
            severity = routingKey;
        IServiceCollection services = new ServiceCollection();
        IConfiguration config = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", false, true)
            .AddJsonFile($"appsettings.{environment}.json", false, true)
            .AddJsonFile("appsettings.secret.json", true, true)
            .AddEnvironmentVariables()
            .Build();
        services.AddLogging(i => i.AddConsole().AddDebug());
        services.AddOptions();
        services.Configure<RabbitMQConfig>(config.GetSection(nameof(RabbitMQConfig)));
        services.AddTransient<IRabbitMqConnection, RabbitMqConnection>();
        QueueProperties queueProperties = new QueueProperties()
        {
            Temporary = false,
            Durable = true,
            Exclusive = true,
            AutoDelete = true,
            Name = null
        };
        PublishingProperties publishingProperties = new PublishingProperties()
        {
            Exchange = string.IsNullOrEmpty(exchange) ? "topic_logs" : exchange,
            ExchangeType = "topic",
            RoutingKey = severity,
            EnablePublisherConfirm = true,
            EnsureDeliveryToQueue = true // This maps to "mandatory" flag of Publish function. False: Silent when there is no subscriber. True: broker will return BasicReturnEventArgs. https://www.rabbitmq.com/dotnet-api-guide.html
        };
        IServiceProvider serviceProvider = services.BuildServiceProvider();
        using IRabbitMqConnection connection = serviceProvider.GetRequiredService<IRabbitMqConnection>();
        using IRabbitMqChannel channel = await connection.CreateChannel(string.IsNullOrEmpty(exchange) ? "topic_logs" : exchange, "topic", queueProperties);
        string msg = args.Length > 1 ? string.Join(" ", args.Skip(1).ToArray()) : "Hello World!!!";
        if (!string.IsNullOrEmpty(message))
            msg = message;
        IMessage testMessage = new TestMessage(msg, DateTimeOffset.UtcNow);
        PublishResult result = await channel.Publish<IMessage>(testMessage, publishingProperties);
        if (result != null && result.Success && result.Errors == null)
            WriteLine($" [x] Sent {severity}: {msg}");
        else if (result.Errors != null && result.Errors.Any())
            WriteLine($"Publish failed! {result.Errors.First().Code} {result.Errors.First().Description}");
        else
            WriteLine("Publish failed with unknown error!");
    }
}