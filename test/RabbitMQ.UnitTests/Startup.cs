using System;
using Polly;
using Microsoft.AspNetCore.Builder;
namespace RabbitMq.UnitTests
{
    public class Startup
    {
#if false
        Action<InMemoryRabbitMqRouter> _routerConfiguration;
        public Startup(Action<InMemoryRabbitMqRouter> routerConfiguration)
        {
            _routerConfiguration = routerConfiguration;
        }
#endif
        public virtual void Initialize(IApplicationBuilder app)
        {
            app.UseSerilogFileLogger(
                "RabbitMq.UnitTests",
                "test/logging"
            );
            // app.UseSerilogConsoleLogger("RabbitMq.UnitTests", Serilog.Events.LogEventLevel.Verbose);
            app.UseSimpleInjector(containerBuilder =>
            {
                containerBuilder.LoadModule<TestModule>();
            });
            app.UseAppSettingsSecureConfigurationStore();
            app.UseAppSettingsConfigurationStore();
            app.UseRabbitMq(config =>
            {
                config
                .AddInMemoryRabbitMQConnectionConfiguration(router =>
                {
                    router.ExchangeDeclare("testExchange1");
                    router.ExchangeDeclare("testExchange2");
                    router.ExchangeDeclare("testExchange3");
                    router.QueueBind("test1.q1", "testExchange1", "test1.q1");
                    router.QueueBind("test1.rpc2", "testExchange3", "test1.rpc2");
                    router.QueueBind("test1.reqResp1", "testExchange2", "test1.rpc");
                    router.QueueBind("test1.q1", "testExchange1", "test1.q1");
                    router.QueueBind("test1.q_mock", "testExchange3", "test1.q_mock");
                    //_routerConfiguration?.Invoke(router);
                })
                // .DiscoverRabbitMqClusteredConnection("test/rabbitMq1", "test/rabbitMq1-credentials")
                .AddPublishRetryPolicyConfiguration((policy) => policy.WaitAndRetryAsync(2, (retryCount) =>
                                              {
                                                  return TimeSpan.FromSeconds(1);
                                              }))
                .Subscribe<TestMessage1>("test1.q1")
                .Subscribe<TestMessage1>("test1.q_mock")
                .Subscribe<TestMessage1>("test1.q2", true)
                .Subscribe<TestRequest1>("test1.reqResp1");
            });
        }
    }
}