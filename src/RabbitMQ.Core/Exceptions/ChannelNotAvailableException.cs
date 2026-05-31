using System;
using System.Runtime.Serialization;

namespace RabbitMQ.Core.Exceptions
{
    [Serializable]
    public class ChannelNotAvailableException : Exception
    {

        /// <summary>
        /// Returns Instance of Channel Not Available Exception
        /// </summary>
        public ChannelNotAvailableException()
            : base()
        {
        }
        /// <summary>
        /// Returns Instance of Channel Not Available Exception
        /// </summary>
        /// <param name="message">Exception message</param>
        /// <param name="exception">Inner Exception</param>
        public ChannelNotAvailableException(string message, Exception exception)
           : base(message, exception)
        {
        }

        /// <summary>
        /// Returns Instance of Channel Not Available Exception
        /// </summary>
        /// <param name="message">Exception message</param>
        public ChannelNotAvailableException(string message)
           : base(message)
        {
        }

        /// <summary>
        /// Returns Instance of Channel Not Available Exception
        /// </summary>
        /// <param name="serializationInfo">Serialization Info</param>
        /// <param name="context">Inner Exception</param>
        protected ChannelNotAvailableException(SerializationInfo serializationInfo, StreamingContext context)
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