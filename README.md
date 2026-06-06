# .Net RabbitMQ Publisher/Subscriber Application

A .NET 10.0 application demonstrating a RabbitMQ publisher/subscriber pattern using the .NET client library.

# Development environment

- Copy `nuget.config.FIXME` to `nuget.config`
- Add `Username` and access token from github Developer Settings

# Publisher

```
2026-05-31T09:05:05.136547357Z   ●   {"@timestamp":"2026-05-31T09:05:05.1365018+00:00","log.level":"Information","message":" [x] Sent kern.info: Hello World!!! @ 05/31/2026 09:05:05 +00:00","ecs.version":"9.0.0","log":{"logger":"RabbitMQ.Publisher.PublisherWorker"},"labels":{"MessageTemplate":" [x] Sent kern.info: Hello World!!! @ 05/31/2026 09:05:05 +00:00"},"agent":{"type":"Elastic.CommonSchema.Serilog","version":"9.0.0+608d9254e808806db6cd119e9af00ab7ae9402c2"},"event":{"created":"2026-05-31T09:05:05.1365018+00:00","severity":2,"timezone":"Coordinated Universal Time"},"host":{"os":{"full":"Ubuntu 24.04.4 LTS","platform":"Unix","version":"7.0.0.22"},"architecture":"X64","hostname":"rabbitmq-publisher-job-29670305-mn8kg"},"process":{"name":"RabbitMQ.Publisher","pid":1,"thread.id":5,"thread.name":".NET TP Worker","title":""},"service":{"name":"RabbitMQ.Publisher","type":"dotnet","version":"10.0+6730b2f6940539ddf41c237ee3c2a7bb3848155c"},"user":{"domain":"rabbitmq-publisher-job-29670305-mn8kg","name":"root"}}
```

# Subscriber

```
2026-05-31T09:05:05.14379578Z    ●   2026-05-31 09:05:05.143661737 +0000 subscriber: {"@timestamp":"2026-05-31T09:05:05.1433053+00:00","log.level":"Information","message":"Message1AckNackConsumer [x] Received kern.info: Hello World!!! @ 05/31/2026 09:05:05 +00:00","ecs.version":"9.0.0","log":{"logger":"RabbitMQ.Subscriber.Message1AckNackConsumer"},"labels":{"MessageTemplate":"Message1AckNackConsumer [x] Received kern.info: Hello World!!! @ 05/31/2026 09:05:05 +00:00"},"agent":{"type":"Elastic.CommonSchema.Serilog","version":"9.0.0+608d9254e808806db6cd119e9af00ab7ae9402c2"},"event":{"created":"2026-05-31T09:05:05.1433053+00:00","severity":2,"timezone":"Coordinated Universal Time"},"host":{"os":{"full":"Ubuntu 24.04.4 LTS","platform":"Unix","version":"7.0.0.22"},"architecture":"X64","hostname":"kern-subscriber-0"},"process":{"name":"RabbitMQ.Subscriber","pid":1,"thread.id":24,"thread.name":".NET TP Worker","title":""},"service":{"name":"RabbitMQ.Subscriber","type":"dotnet","version":"10.0+6730b2f6940539ddf41c237ee3c2a7bb3848155c"},"user":{"domain":"kern-subscriber-0","name":"root"}}
```

# Visual Studio

- Open the solution file <code>RabbitMQ.slnx</code> and build/run.

# Visual Studio Code

- `Ctrl`+`Shift`+`B` to build
- `F5` to start debug session

# Logs

- logs are available at `/var/log/subscriber/logYYYYMMDD_*` and `/var/log/publisher/logYYYYMMDD_*`

## Windows 11

- Enter powershell: `powershell`
- `Get-Content -Path "c:\var\log\subscriber\logYYYYMMDD_<foo>" -Wait` and/or `Get-Content -Path "c:\var\log\publisher\logYYYYMMDD_<foo>" -Wait`

# Continuous Integration:

- Integrated with CircleCI

# Kubernetes

- If ingress uses a prefix path, the prefix needs to be added as an environment variable `PATH_BASE` (or `appsettings.json` mounted from ConfigMap)
- Swagger does NOT work when the `PATH_BASE` is not `/` due to an issued filed as https://github.com/dotnet/aspnetcore/issues/42559
