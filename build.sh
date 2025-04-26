#!/bin/bash

export DOCKER_DEFAULT_PLATFORM=linux/amd64
docker build -t pankajagarwal/mongodataapigo:latest .
docker push pankajagarwal/mongodataapigo:latest
