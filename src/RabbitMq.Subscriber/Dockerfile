FROM mcr.microsoft.com/dotnet/runtime:10.0
LABEL org.opencontainers.image.authors="Kok How, Teh <funcoolgeeek@gmail.com>"
RUN apt update -y
RUN DEBIAN_FRONTEND=noninteractive apt install -y tzdata gnupg2 gnupg gnupg1
WORKDIR /app
ENV PASSWORD="P@$$w0rd"
RUN openssl req -new -newkey rsa:4096 -x509 -nodes -days 365 -keyout server.key -out server.crt -subj "/C=SG/ST=Singapore/L=Singapore /O=Kok How Pte. Ltd./OU=RabbitMq.Subscriber/CN=localhost/emailAddress=funcoolgeek@gmail.com" -passin env:PASSWORD -addext "subjectAltName = DNS:localhost"
RUN openssl pkcs12 -export -out /tmp/localhost.pfx -inkey server.key -in server.crt -passout env:PASSWORD
ADD bin/Release/net10.0/linux-x64/publish .
#EXPOSE 5001
#ENTRYPOINT ["dotnet", "Subscriber.dll", "#"]
ENTRYPOINT ["./RabbitMq.Subscriber", "#"]
