using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMq.Core;
using RabbitMq.Core.Consumer;
using RabbitMq.Core.Extensions;
using RabbitMq.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Console;

namespace RabbitMq.Subscriber;

public class IMessage1AckNackConsumer : IRabbitMqConsumer<IMessage>
{
    private readonly ILogger<IMessage1AckNackConsumer> _logger;
    public IMessage1AckNackConsumer(ILogger<IMessage1AckNackConsumer> logger) => _logger = logger;
    public async Task Consume(IRabbitMqConsumeContext<IMessage> consumeContext)
    {
        try
        {
            await Task.Run(() => { });
            //_context.IncrementSuccessCount();
            WriteLine($"{nameof(IMessage1AckNackConsumer)} [x] Received {consumeContext.RoutingKey}: {consumeContext.Message.Message} @ {consumeContext.Message.Timestamp}");
        }
        catch (Exception e)
        {
            //_context.SetCompleted();
            _logger.LogCritical($"{nameof(IMessage1AckNackConsumer)} Exception! {e.Message} {e.GetInnerMessage()} {e.StackTrace}");
        }
    }
}

class Program
{
    static async Task Main(string[] args)
    {
        // $ DOTNET_ENVIRONMENT=Development ./RabbitMq.Subscriber
        string environment = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "Development";
        string exchange = Environment.GetEnvironmentVariable("EXCHANGE");
        string envBindings = Environment.GetEnvironmentVariable("BINDINGS");
        string userName = Environment.GetEnvironmentVariable("UserName");
        string password = Environment.GetEnvironmentVariable("Password");
        IServiceCollection services = new ServiceCollection();
        IConfiguration config = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", false, true)
            .AddJsonFile($"appsettings.{environment}.json", false, true)
            .AddJsonFile("appsettings.secret.json", true, true)
            .AddEnvironmentVariables()
            .Build();
        services.AddLogging(cfg => cfg.AddConsole().AddDebug());
        services.AddOptions();
        IConfigurationSection rabbitMqConfigSection = config.GetSection(nameof(RabbitMQConfig));
        rabbitMqConfigSection["UserName"] = userName ?? config["UserName"];
        rabbitMqConfigSection["Password"] = password ?? config["Password"];
        services.Configure<RabbitMQConfig>(rabbitMqConfigSection);
        //RabbitMqSubscriberFactory<string> subscriberFactory = new RabbitMqSubscriberFactory<string>();
        services.AddTransient<IRabbitMqConsumer<IMessage>, IMessage1AckNackConsumer>();
        services.Decorate<IRabbitMqConsumer<IMessage>, PostConsumerAckDecorator<IMessage>>();
        services.AddTransient<IRabbitMqConnection, RabbitMqConnection>();
        QueueProperties queueProperties = new QueueProperties()
        {
            Temporary = true,
            Durable = true,
            Exclusive = true,
            AutoDelete = true,
            Name = null
        };
        List<string> bindings = !string.IsNullOrEmpty(envBindings) ? envBindings.Split(",").ToList() : args.ToList();
        SubscriberProperties subscriberProperties = new SubscriberProperties()
        {
            Exchange = string.IsNullOrEmpty(exchange) ? "topic_logs" : exchange,
            ExchangeType = "topic",
            Bindings = bindings
        };
        services.AddSingleton<IRabbitMqSubscriberFactory<IMessage>, RabbitMqSubscriberFactory<IMessage>>();
        IServiceProvider serviceProvider = services.BuildServiceProvider();
        ILogger<Program> logger = serviceProvider.GetRequiredService<ILogger<Program>>();
        logger.LogInformation($"UserName: {userName} {config["UserName"]} {rabbitMqConfigSection["UserName"]}, Password: {password} {config["Password"]} {rabbitMqConfigSection["Password"]}");
        logger.LogInformation($"ConnectionName: {rabbitMqConfigSection["ConnectionName"]}, Endpoint: {rabbitMqConfigSection["Endpoint"]}, VHost: {rabbitMqConfigSection["VHost"]}");
        StringBuilder sb = new StringBuilder();
        foreach (string i in bindings)
            sb.Append($"{i}, ");
        logger.LogInformation($"Bindings: {sb.ToString()}");
        IRabbitMqSubscriberFactory<IMessage> subscriberFactory = serviceProvider.GetRequiredService<IRabbitMqSubscriberFactory<IMessage>>();
        using IRabbitMqSubscriber<IMessage> subscriber = subscriberFactory.GetRabbitMqSubscriber(subscriberProperties, queueProperties,
            true, serviceProvider.GetRequiredService<IRabbitMqConnection>(),
            serviceProvider.GetRequiredService<IRabbitMqConsumer<IMessage>>(), null);
        await subscriber.Connect();
        logger.LogInformation(" [*] Waiting for logs...");
        logger.LogInformation("Press ENTER to exit:");
        while (true) ;
    }
}