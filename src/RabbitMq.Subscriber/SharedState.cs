public class SharedState
{
    // Initialize with 0 if it should wait initially
    public SemaphoreSlim SignalEvent { get; } = new SemaphoreSlim(0, 1);
}