using System;
using System.Runtime.Serialization;

namespace RabbitMQ.Core.Exceptions
{
    [Serializable]
    public class ChannelDisposingException : Exception
    {

        /// <summary>
        /// Returns Instance of <see cref="ChannelDisposingException"/>
        /// </summary>
        public ChannelDisposingException() : base()
        {
        }
        /// <summary>
        /// Returns Instance of <see cref="ChannelDisposingException"/>
        /// </summary>
        /// <param name="message">Exception message</param>
        /// <param name="exception">Inner Exception</param>
        public ChannelDisposingException(string message, Exception exception)
           : base(message, exception)
        {
        }

        /// <summary>
        /// Returns Instance of <see cref="ChannelDisposingException"/>
        /// </summary>
        /// <param name="message">Exception message</param>
        public ChannelDisposingException(string message)
           : base(message)
        {
        }

        /// <summary>
        /// Returns Instance of <see cref="ChannelDisposingException"/>
        /// </summary>
        /// <param name="serializationInfo">Serialization Info</param>
        /// <param name="context">Inner Exception</param>
        protected ChannelDisposingException(SerializationInfo serializationInfo, StreamingContext context)
         : base(serializationInfo, context)
        {
        }

        /// <inheritdoc/>
        public override string Message
        {
            get => $"Rabbit Channel is down.{base.Message}";
        }
    }
}