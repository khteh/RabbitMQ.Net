#!/bin/bash
#$(aws ecr get-login --no-include-email)
#dotnet publish -c Release RabbitMq.Publisher
#dotnet publish -c Release RabbitMq.Subscriber
docker build -f RabbitMq.Publisher/Dockerfile -t publisher RabbitMq.Publisher
docker build -f RabbitMq.Subscriber/Dockerfile -t subscriber RabbitMq.Subscriber
docker tag publisher:latest khteh/rabbitmq-publisher:latest
docker tag subscriber:latest khteh/rabbitmq-subscriber:latest
docker push khteh/rabbitmq-publisher:latest
docker push khteh/rabbitmq-subscriber:latest
