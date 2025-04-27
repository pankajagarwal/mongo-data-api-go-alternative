#!/bin/bash

IMAGE_NAME="pankajagarwal/mongodataapigo"
IMAGE_TAG="latest"

export DOCKER_CLI_EXPERIMENTAL=enabled

# Create a new buildx builder if not already created
docker buildx create --use --name multiarch_builder || docker buildx use multiarch_builder

# Build and push the multi-arch image
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t ${IMAGE_NAME}:${IMAGE_TAG} \
  --push \
  .

echo "✅ Multi-arch image ${IMAGE_NAME}:${IMAGE_TAG} built and pushed!"
