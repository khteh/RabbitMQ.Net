using System;
using RabbitMQ.Core.Exceptions;
using RabbitMQ.Client.Exceptions;

namespace RabbitMQ.Core.Extensions
{
    public static class ExceptionExtensions
    {
        /// <summary>
        /// Try get InnerException.Message
        /// </summary>
        /// <param name="ex"></param>
        /// <returns></returns>
        public static string GetInnerMessage(this Exception ex)
            => ex.InnerException?.InnerException?.Message ?? (ex.InnerException?.Message ?? ex.Message);

        public static bool IsTransientRabbitMQException(this Exception exception) =>
            exception is ConnectFailureException
                || exception is OperationInterruptedException
                || exception is ChannelNotAvailableException
                || exception is TimeoutException;
    }
}