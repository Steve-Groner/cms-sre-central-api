#!/bin/bash

eval $(alks sessions open -a "473451415060/ALKSAdmin - awsconsumertoolsnp")
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 473451415060.dkr.ecr.us-east-1.amazonaws.com
docker buildx build -f Dockerfile --platform linux/x86_64  --network=host -t cms-sre-central-api .
docker tag cms-sre-central-api:latest 473451415060.dkr.ecr.us-east-1.amazonaws.com/consumer-tools:cms-sre-central-api-latest
docker push 473451415060.dkr.ecr.us-east-1.amazonaws.com/consumer-tools:cms-sre-central-api-latest
aws ecs update-service --cluster skypiea-data-cluster --service cms-sre-central-api-svc --force-new-deployment --no-paginate --output json --no-cli-pager > /dev/null 2>&1

