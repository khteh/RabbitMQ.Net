using Microsoft.Extensions.DependencyInjection;
using System;

namespace RabbitMQ.UnitTests
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

            services.RegisterRabbitMQMessageConsumer<TestMessage1, TestMessage1Consumer>();
            services.RegisterRabbitMQMessageConsumer<TestMessage1, TestMessage1AckNackConsumer>();

            services.RegisterRabbitMQMessageConsumer<TestRequest1, TestRequestResponseServerConsumer>();
        }
    }
}