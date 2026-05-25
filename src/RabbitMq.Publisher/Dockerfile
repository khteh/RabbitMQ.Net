FROM mcr.microsoft.com/dotnet/core/aspnet:3.0.0-disco
MAINTAINER Kok How, Teh <funcoolgeek@gmail.com>
RUN apt update -y
RUN DEBIAN_FRONTEND=noninteractive apt install -y tzdata
WORKDIR /app
ADD bin/Release/netcoreapp3.0/publish/ .
EXPOSE 5001
ENTRYPOINT ["dotnet", "Publisher.dll", "hello", "Hello World!!!"]
