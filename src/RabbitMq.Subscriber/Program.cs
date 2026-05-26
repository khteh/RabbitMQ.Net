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
using Serilog;
using Serilog.Events;
using Serilog.Formatting.Elasticsearch;
using RabbitMq.Subscriber;
using Microsoft.Extensions.Hosting;

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
                     .AddJsonFile($"appsettings.{env.EnvironmentName}.json", true, true) // Development only has one file
                     .AddJsonFile($"appsettings.CriticalSubscriber.{env.EnvironmentName}.json", true, true)
                     .AddJsonFile($"appsettings.KernSubscriber.{env.EnvironmentName}.json", true, true)
                     .AddEnvironmentVariables()
                     .AddCommandLine(args)
                     .Build();
         Log.Information($"env: {env.EnvironmentName}");
     })
     .ConfigureServices((context, services) =>
     {
         services.AddOptions();
         IConfigurationSection rabbitMqConfigSection = context.Configuration.GetSection(nameof(RabbitMQConfig));
         rabbitMqConfigSection["UserName"] = context.Configuration["UserName"];
         rabbitMqConfigSection["Password"] = context.Configuration["Password"];
         Log.Information($"ConnectionName: {rabbitMqConfigSection["ConnectionName"]}, Endpoint: {rabbitMqConfigSection["Endpoint"]}, VHost: {rabbitMqConfigSection["VHost"]}");
         services.Configure<RabbitMQConfig>(rabbitMqConfigSection);
         services.AddTransient<IRabbitMqConnection, RabbitMqConnection>();
         services.AddHostedService<SubscriberWorker>()
             .AddSingleton<IConfiguration>(context.Configuration)
             .AddSingleton<IRabbitMqSubscriberFactory<IMessage>, RabbitMqSubscriberFactory<IMessage>>()
             .AddTransient<IRabbitMqConsumer<IMessage>, IMessage1AckNackConsumer>()
             .Decorate<IRabbitMqConsumer<IMessage>, PostConsumerAckDecorator<IMessage>>()
             .AddTransient<IRabbitMqConnection, RabbitMqConnection>()
             .AddHealthChecks();
     }).UseSerilog((ctx, config) =>
     {
         config.ReadFrom.Configuration(ctx.Configuration);
         if (ctx.HostingEnvironment.IsDevelopment() || ctx.HostingEnvironment.IsStaging())
             config.WriteTo.Console(LogEventLevel.Verbose, "{NewLine}{Timestamp:HH:mm:ss} [{Level}] ({CorrelationToken}) {Message}{NewLine}{Exception}");
         else
             config.WriteTo.Console(new ElasticsearchJsonFormatter());
         LoggerConfiguration logConfig = new LoggerConfiguration().ReadFrom.Configuration(ctx.Configuration);
         logConfig.WriteTo.Console(new ElasticsearchJsonFormatter());
         // Create the logger
         Log.Logger = logConfig.CreateLogger();
         string connectionString = ctx.Configuration.GetConnectionString("Default");
         Log.Debug($"Connection String: {connectionString}");
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