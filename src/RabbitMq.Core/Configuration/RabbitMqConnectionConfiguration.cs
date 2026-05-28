namespace RabbitMq.Core.Configuration;

public sealed class RabbitMqConnectionConfiguration
{
    /// <summary>
    /// Creates and instance of the Rabbit Mq Connection Configuration
    /// </summary>
    /// <param name="rabbitMqEndPointUri">Rabbit Mq Base Address</param>
    /// <param name="clusteredRabbitMqHosts">Rabbit Mq Clustered Hosts</param>
    /// <param name="userName">Rabbit Mq Basic Auth user name</param>
    /// <param name="password">Rabbit Mq basic Auth password</param>
    /// <param name="connectionName">Rabbit Mq connection Name</param>
    public RabbitMqConnectionConfiguration(
       Uri rabbitMqEndPointUri,
       IReadOnlyList<string> clusteredRabbitMqHosts,
       string userName,
       string password,
       string connectionName)
    {
        ClusteredRabbitMqHosts = clusteredRabbitMqHosts;
        RabbitMqUri = rabbitMqEndPointUri;
        UserName = userName;
        Password = password;
        ConnectionName = connectionName;
    }

    /// <summary>
    /// Rabbit Mq Endpoint Uri
    /// </summary>
    public Uri RabbitMqUri { get; set; }

    /// <summary>
    /// Rabbit Mq Clustered Hosts
    /// </summary>
    public IReadOnlyList<string> ClusteredRabbitMqHosts { get; set; }

    /// <summary>
    /// Rabbit Mq basic auth UserName
    /// </summary>
    public string UserName { get; set; }

    /// <summary>
    /// Rabbit Mq Basic Auth Password
    /// </summary>
    public string Password { get; set; }

    /// <summary>
    /// Rabbit Mq Connection Name
    /// </summary>
    public string ConnectionName { get; set; }
}
