# .Net RabbitMQ Publisher/Subscriber Application 

A .NET 10.0 application demonstrating a RabbitMQ publisher/subscriber pattern.

# Development environment

- Copy `nuget.config.FIXME` to `nuget.config`
- Add `Username` and access token from github Developer Settings

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

- logs are available at `/var/log/aspnetcore/logYYYYMMDD_*`

## Windows 11

- Enter powershell: `powershell`
- `Get-Content -Path "c:\var\log\aspnetcore\logYYYYMMDD_<foo>" -Wait`

# Continuous Integration:

- Integrated with CircleCI

# Kubernetes

- If ingress uses a prefix path, the prefix needs to be added as an environment variable `PATH_BASE` (or `appsettings.json` mounted from ConfigMap)
- Swagger does NOT work when the `PATH_BASE` is not `/` due to an issued filed as https://github.com/dotnet/aspnetcore/issues/42559