using Microsoft.Extensions.DependencyInjection;
using System;

namespace RabbitMq.UnitTests
{
    /// <summary>
    /// An implementation of <see cref="IModule"/> to register the TestModule services.
    /// </summary>
    public sealed class TestModule
    {
        /// <summary>
        /// Initializes the current module instance into a specified container services.
        /// </summary>
        /// <param name="services">The dependency injection container services.</param>
        public void Initialize(IServiceCollection services)
        {
            if (services == null)
                throw new ArgumentNullException(nameof(services));
            var testResultContext = new TestResultContext();
            services.Register<TestResultContext>(() => testResultContext);

            services.RegisterRabbitMqMessageConsumer<TestMessage1, TestMessage1Consumer>();
            services.RegisterRabbitMqMessageConsumer<TestMessage1, TestMessage1AckNackConsumer>();

            services.RegisterRabbitMqMessageConsumer<TestRequest1, TestRequestResponseServerConsumer>();
        }
    }
}