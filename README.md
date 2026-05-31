# .Net RabbitMQ Publisher/Subscriber Application

A .NET 10.0 application demonstrating a RabbitMQ publisher/subscriber pattern using the .NET client library.

# Development environment

- Copy `nuget.config.FIXME` to `nuget.config`
- Add `Username` and access token from github Developer Settings

# Publisher

```
2026-05-31T05:25:04.960454567Z   ●   2026-05-31 05:25:04.956992639 +0000 publisher: {"@timestamp":"2026-05-31T05:25:02.8907842+00:00","log.level":"Information","message":" [x] Sent kern.info: Hello World!!! @ 05/31/2026 05:25:02 +00:00","ecs.version":"9.0.0","log":{"logger":"RabbitMq.Publisher.PublisherWorker"},"labels":{"MessageTemplate":" [x] Sent kern.info: Hello World!!! @ 05/31/2026 05:25:02 +00:00"},"agent":{"type":"Elastic.CommonSchema.Serilog","version":"9.0.0+608d9254e808806db6cd119e9af00ab7ae9402c2"},"event":{"created":"2026-05-31T05:25:02.8907842+00:00","severity":2,"timezone":"Coordinated Universal Time"},"host":{"os":{"full":"Ubuntu 24.04.4 LTS","platform":"Unix","version":"7.0.0.22"},"architecture":"X64","hostname":"rabbitmq-publisher-job-29670085-flcbp"},"process":{"name":"RabbitMq.Publisher","pid":1,"thread.id":5,"thread.name":".NET TP Worker","title":""},"service":{"name":"RabbitMq.Publisher","type":"dotnet","version":"10.0+dca1a3b92bc811ef7fe0360ba3f3dab1bf31af73"},"user":{"domain":"rabbitmq-publisher-job-29670085-flcbp","name":"root"}}
```

# Subscriber

```
2026-05-31T05:25:02.898039635Z   ●   2026-05-31 05:25:02.897918462 +0000 subscriber: {"@timestamp":"2026-05-31T05:25:02.8976523+00:00","log.level":"Information","message":"Message1AckNackConsumer [x] Received kern.info: Hello World!!! @ 05/31/2026 05:25:02 +00:00","ecs.version":"9.0.0","log":{"logger":"RabbitMq.Subscriber.Message1AckNackConsumer"},"labels":{"MessageTemplate":"Message1AckNackConsumer [x] Received kern.info: Hello World!!! @ 05/31/2026 05:25:02 +00:00"},"agent":{"type":"Elastic.CommonSchema.Serilog","version":"9.0.0+608d9254e808806db6cd119e9af00ab7ae9402c2"},"event":{"created":"2026-05-31T05:25:02.8976523+00:00","severity":2,"timezone":"Coordinated Universal Time"},"host":{"os":{"full":"Ubuntu 24.04.4 LTS","platform":"Unix","version":"7.0.0.22"},"architecture":"X64","hostname":"kern-subscriber-0"},"process":{"name":"RabbitMq.Subscriber","pid":1,"thread.id":20,"thread.name":".NET TP Worker","title":""},"service":{"name":"RabbitMq.Subscriber","type":"dotnet","version":"10.0+dca1a3b92bc811ef7fe0360ba3f3dab1bf31af73"},"user":{"domain":"kern-subscriber-0","name":"root"}}
```

# Visual Studio

- Generate TLS cert and put the `localhost.pfx` into `/tmp`
- Open the solution file <code>AspNetCoreWebApi.sln</code> and build/run.

# Visual Studio Code

- `Ctrl`+`Shift`+`B` to build
- Generate TLS cert and put the `localhost.pfx` into `/tmp`
- `F5` to start debug session

## Unit Testing

- Install .Net Core Test Explorer
- `echo fs.inotify.max_user_instances=524288 | sudo tee -a /etc/sysctl.conf && sudo sysctl -p`
- https://github.com/dotnet/aspnetcore/issues/8449

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
