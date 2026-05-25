#!/bin/bash
#$(aws ecr get-login --no-include-email)
#dotnet publish -c Release src/RabbitMq.Publisher
#dotnet publish -c Release src/RabbitMq.Subscriber
docker build -f src/RabbitMq.Publisher/Dockerfile -t publisher src/RabbitMq.Publisher
docker build -f src/RabbitMq.Subscriber/Dockerfile -t subscriber src/RabbitMq.Subscriber
docker tag publisher:latest khteh/rabbitmq-publisher:latest
docker tag subscriber:latest khteh/rabbitmq-subscriber:latest
docker push khteh/rabbitmq-publisher:latest
docker push khteh/rabbitmq-subscriber:latest
