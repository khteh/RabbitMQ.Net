#!/bin/bash
$(aws ecr get-login --no-include-email)
dotnet publish -c Release test/Publisher
dotnet publish -c Release test/Subscriber
docker build -f test/Publisher/Dockerfile -t publisher test/Publisher
docker build -f test/Subscriber/Dockerfile -t subscriber test/Subscriber
docker tag publisher:latest khteh/rabbitmq-publisher:latest
docker tag subscriber:latest khteh/rabbitmq-subscriber:latest
docker push khteh/rabbitmq-publisher:latest
docker push khteh/rabbitmq-subscriber:latest
