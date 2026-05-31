using System;
using System.ComponentModel;
namespace RabbitMQ.Core;

public abstract class DisposableObject : IDisposable
{
    /// <summary>
    ///     Gets a value indicating whether this instance is disposed.
    /// </summary>
    /// <value><c>true</c> if this instance is disposed; otherwise, <c>false</c>.</value>
    [Browsable(false)]
    public bool IsDisposed { get; private set; }

    /// <summary>
    ///     Gets a value indicating whether this instance is disposing.
    /// </summary>
    /// <value><c>true</c> if this instance is disposed; otherwise, <c>false</c>.</value>
    [Browsable(false)]
    public bool IsDisposing { get; private set; }

    /// <summary>
    ///     Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    ///     Releases unmanaged and - optionally - managed resources.
    /// </summary>
    /// <param name="disposing">
    ///     <c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only
    ///     unmanaged resources.
    /// </param>
    protected virtual void Dispose(bool disposing)
    {
        try
        {
            if (!IsDisposed && disposing)
            {
                IsDisposing = true;
                Disposing();
            }
        }
        finally
        {
            IsDisposed = true;
            IsDisposing = false;
        }
    }

    /// <summary>
    ///     Overridden in implementing objects to perform actual clean-up.
    /// </summary>
    protected virtual void Disposing()
    {
    }
}