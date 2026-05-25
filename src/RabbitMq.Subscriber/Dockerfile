FROM mcr.microsoft.com/dotnet/runtime:10.0
LABEL org.opencontainers.image.authors="Kok How, Teh <funcoolgeeek@gmail.com>"
RUN apt update -y
RUN DEBIAN_FRONTEND=noninteractive apt install -y tzdata
WORKDIR /app
ADD bin/Release/net10.0/linux-x64/publish .
EXPOSE 5001
#ENTRYPOINT ["dotnet", "Subscriber.dll", "#"]
ENTRYPOINT ["./RabbitMq.Subscriber", "#"]
