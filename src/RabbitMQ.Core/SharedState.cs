public class SharedState : IDisposable
{
    // Initialize with 0 if it should wait initially
    public SemaphoreSlim SignalEvent { get; } = new SemaphoreSlim(0, 1);

    public void Dispose()
    {
        SignalEvent?.Dispose();
        GC.SuppressFinalize(this);
    }
}