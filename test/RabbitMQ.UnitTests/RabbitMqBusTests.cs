using RabbitMq.Core.Interfaces;
using RabbitMQ.Client.Exceptions;
using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;
namespace RabbitMq.UnitTests
{

    public class RabbitMqBusTests
    {

        [Fact]
        public async Task SuccessfulMessageConsumerTest()
        {
            using (var container = Initialize())
            {
                var publishingBus = container.GetInstance<IPublishingBus>();
                await publishingBus.Publish("testExchange1", "test1.q1", new TestMessage1() { Title = "Message Type 1", Now = DateTime.Now },
                    (prop) => prop.PublishReturnWaitTime = TimeSpan.FromSeconds(1000));

                var testContext = container.GetInstance<TestResultContext>();
                Assert.True(testContext.Wait());
                Assert.Equal(1, testContext.SuccessCount);
            }
        }

        [Fact]
        public async Task SuccessfulMessageConsumerWithAckNackTest()
        {
            using (var container = Initialize())
            {
                var publishingBus = container.GetInstance<IPublishingBus>();
                await publishingBus.Publish("testExchange1", "test1.q1.ack", new TestMessage1() { Title = "Message Type 1" },
                    (prop) => prop.PublishReturnWaitTime = TimeSpan.FromSeconds(10));

                var testContext = container.GetInstance<TestResultContext>();
                Assert.True(testContext.Wait());
                Assert.Equal(1, testContext.SuccessCount);
            }
        }

        [Fact]
        public async Task RequestResponseMessageConsumerTest()
        {
            using (var container = Initialize())
            {
                var publishingBus = container.GetInstance<IPublishingBus>();
                var message2 = await publishingBus.Request<TestRequest1, TestResponse>("testExchange2",
                    "test1.rpc",
                    new TestRequest1() { Title = "Message Type 2" },
                    (prop) => prop.ReplyWaitTime = TimeSpan.FromSeconds(2));

                var testContext = container.GetInstance<TestResultContext>();
                Assert.IsTrue(testContext.Wait());
                Assert.AreEqual(1, testContext.SuccessCount);
            }
        }

        [Fact]
        [ExpectedException(typeof(TimeoutException))]
        public async Task RequestResponseMessageNoResponderTest()
        {
            using (var container = Initialize())
            {
                var publishingBus = container.GetInstance<IPublishingBus>();
                var message2 = await publishingBus.Request<TestRequest2, TestResponse>("testExchange3",
                    "test1.rpc2",
                    new TestRequest2() { Title = "Message Type 2" },
                    (prop) => prop.ReplyWaitTime = TimeSpan.FromSeconds(2));
            }
        }

        [Fact]
        public async Task PublishRetryPolicyTest()
        {
            TestResultContext testContext = null;
            var retryCount = 0;
            using (var container = Initialize(router => router.RegisterCustomAction("testExchange3", "test1.q_mock", (message, prop) =>
                    {
                        testContext.IncrementRetryCount();
                        if (retryCount++ == 0)
                        {
                            throw new ConnectFailureException("Test Exception", new FileNotFoundException());
                        }
                    })))
            {
                testContext = container.GetInstance<TestResultContext>();
                var publishingBus = container.GetInstance<IPublishingBus>();
                await publishingBus.Publish("testExchange3", "test1.q_mock", new TestMessage1() { Title = "Message Type 1" },
                    (prop) => prop.PublishReturnWaitTime = TimeSpan.FromSeconds(2));


                Assert.IsTrue(testContext.Wait());
                Assert.AreEqual(2, testContext.RetryCount);
            }
        }

        private static IDependencyContainer Initialize()
        {
            var appBuilder = new TestAppInitializer<Startup>();
            return appBuilder.Build();
        }
#if false
        private static IDependencyContainer Initialize(Action<InMemoryRabbitMqRouter> routerConfiguration)
        {
            var appBuilder = new TestAppInitializer<Startup>(new Startup(routerConfiguration));
            return appBuilder.Build();
        }
#endif
    }
}
