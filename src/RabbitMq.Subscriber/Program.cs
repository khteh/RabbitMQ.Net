using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMq.Core;
using RabbitMq.Core.Configuration;
using RabbitMq.Core.Consumer;
using RabbitMq.Core.Interfaces;
using RabbitMq.Subscriber;
using Serilog;
using Serilog.Events;
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
         IConfigurationSection rabbitMqConfigSection = context.Configuration.GetSection(nameof(RabbitMQConfig));
         IConfigurationSection rabbitMqQueueConfigSection = context.Configuration.GetSection(nameof(RabbitMQQueueConfig));
         rabbitMqConfigSection["UserName"] = context.Configuration["UserName"];
         rabbitMqConfigSection["Password"] = context.Configuration["Password"];
         services.Configure<RabbitMQConfig>(rabbitMqConfigSection);
         services.Configure<RabbitMQQueueConfig>(rabbitMqQueueConfigSection);
         services.AddHostedService<SubscriberWorker>()
             .AddSingleton<IConfiguration>(context.Configuration)
             .AddSingleton<SharedState>()
             .AddSingleton<IRabbitMqSubscriberFactory<IMessage>, RabbitMqSubscriberFactory<IMessage>>()
             .AddTransient<IRabbitMqConsumer<IMessage>, Message1AckNackConsumer>()
             .Decorate<IRabbitMqConsumer<IMessage>, PostConsumerAckDecorator<IMessage>>()
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