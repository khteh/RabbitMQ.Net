using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMq.Core;
using RabbitMq.Core.Configuration;
using RabbitMq.Core.Consumer;
using RabbitMq.Core.Interfaces;
using Serilog;
using Serilog.Events;
using RabbitMq.Publisher;
Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateBootstrapLogger();
try
{
    IHostBuilder builder = Host.CreateDefaultBuilder(args).ConfigureAppConfiguration((hostingContext, configuration) =>
     {
         configuration.Sources.Clear();
         IHostEnvironment env = hostingContext.HostingEnvironment;
         IConfiguration config = configuration.AddJsonFile("appsettings.json", false, true)
                     .AddJsonFile($"appsettings.{env.EnvironmentName}.json", false, true)
                     .AddEnvironmentVariables()
                     .AddCommandLine(args)
                     .Build();
         Log.Information($"env: {env.EnvironmentName}");
     })
     .ConfigureServices((context, services) =>
     {
         services.AddOptions();
         IConfigurationSection RabbitMQConnectionConfigSection = context.Configuration.GetSection(nameof(RabbitMQConnectionConfig));
         IConfigurationSection rabbitMqQueueConfigSection = context.Configuration.GetSection(nameof(RabbitMQQueueConfig));
         IConfigurationSection rabbitMqChannelConfigSection = context.Configuration.GetSection(nameof(RabbitMQChannelConfig));
         RabbitMQConnectionConfigSection["UserName"] = context.Configuration["UserName"];
         RabbitMQConnectionConfigSection["Password"] = context.Configuration["Password"];
         services.Configure<RabbitMQConnectionConfig>(RabbitMQConnectionConfigSection);
         services.Configure<RabbitMQQueueConfig>(rabbitMqQueueConfigSection);
         services.Configure<RabbitMQChannelConfig>(rabbitMqChannelConfigSection);
         services.AddHostedService<PublisherWorker>()
             .AddSingleton<IConfiguration>(context.Configuration)
             .AddSingleton<SharedState>()
             .AddSingleton<IRabbitMqSubscriberFactory<IMessage>, RabbitMqSubscriberFactory<IMessage>>()
             .AddTransient<IMessage>(serviceProvider =>
                {
                    return new TestMessage(context.Configuration["MESSAGE"], DateTimeOffset.UtcNow);
                })
             .AddTransient<IRabbitMqConnection, RabbitMqConnection>();
     })
     .UseSerilog((ctx, svc, config) =>
     {
         config.ReadFrom.Configuration(ctx.Configuration).ReadFrom.Services(svc).Enrich.FromLogContext();
         if (ctx.HostingEnvironment.IsDevelopment() || ctx.HostingEnvironment.IsStaging())
             config.WriteTo.Console(LogEventLevel.Verbose, "{NewLine}{Timestamp:HH:mm:ss} [{Level}] ({CorrelationToken}) {Message}{NewLine}{Exception}");
     });
    using IHost host = builder.Build();
    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Application start-up failed");
}
finally
{
    Log.CloseAndFlush();
}