using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

namespace RabbitMq.UnitTests.Helpers
{
    public sealed class TestResultContext
    {
        private readonly ManualResetEvent _event = new ManualResetEvent(false);
        private int _successCount = 0;
        private int _retryCount = 0;

        public int RetryCount
        {
            get { return _retryCount; }
        }

        public void IncrementRetryCount()
        {
            Interlocked.Increment(ref _retryCount);
        }

        public int SuccessCount
        {
            get { return _successCount; }
        }

        public void IncrementSuccessCount()
        {
            Interlocked.Increment(ref _successCount);
        }

        public void SetCompleted()
        {
            _event.Set();
        }

        public bool Wait()
        {
            return _event.WaitOne(TimeSpan.FromSeconds(30));
        }
    }
}