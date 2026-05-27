FROM mcr.microsoft.com/dotnet/runtime:10.0
LABEL org.opencontainers.image.authors="Kok How, Teh <funcoolgeeek@gmail.com>"
RUN apt update -y
RUN DEBIAN_FRONTEND=noninteractive apt install -y tzdata gnupg2 gnupg gnupg1
WORKDIR /app
RUN openssl req -new -newkey rsa:4096 -x509 -nodes -days 365 -keyout server.key -out server.crt -subj "/C=SG/ST=Singapore/L=Singapore /O=Kok How Pte. Ltd./OU=RabbitMq.Subscriber/CN=default.svc.cluster.local/emailAddress=funcoolgeek@gmail.com" -passin pass:P@$$w0rd -addext "subjectAltName = DNS:*.rabbitmq-nodes.default.svc.cluster.local,DNS:rabbitmq.default.svc.cluster.local"
RUN openssl pkcs12 -export -out /tmp/localhost.pfx -inkey server.key -in server.crt -passout pass:P@$$w0rd
ADD bin/Release/net10.0/linux-x64/publish .
#EXPOSE 5001
#ENTRYPOINT ["dotnet", "Subscriber.dll", "#"]
ENTRYPOINT ["./RabbitMq.Subscriber", "#"]
