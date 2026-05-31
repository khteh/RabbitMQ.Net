#!/bin/bash
#$(aws ecr get-login --no-include-email)
#dotnet publish -c Release RabbitMQ.Publisher
#dotnet publish -c Release RabbitMQ.Subscriber
docker build -f RabbitMQ.Publisher/Dockerfile -t publisher RabbitMQ.Publisher
docker build -f RabbitMQ.Subscriber/Dockerfile -t subscriber RabbitMQ.Subscriber
docker tag publisher:latest khteh/rabbitmq-publisher:latest
docker tag subscriber:latest khteh/rabbitmq-subscriber:latest
docker push khteh/rabbitmq-publisher:latest
docker push khteh/rabbitmq-subscriber:latest
