# syntax=docker/dockerfile:1.5
# Base image for running Orquestra tasks.
# Published at hub.nexus.orquestra.io/zapatacomputing/orquestra-sdk-base
FROM python:3.9-slim-bullseye
ARG SDK_REQUIREMENT

# Set by BuildKit
ARG TARGETARCH

# If statement installs any additional dependencies needed for building for ARM
# (development using an M1 is fun :') )
RUN <<EOF
set -ex
apt update --yes
apt upgrade --yes
apt install --yes --no-install-recommends git openssh-client

if [ "$TARGETARCH" = "arm64" ]; then
  apt install --yes --no-install-recommends gcc libc-dev
fi

python -m pip install -U pip wheel
python -m pip install "${SDK_REQUIREMENT}"
EOF

RUN useradd -ms /bin/bash -d /home/orquestra orquestra --uid 1000 --gid 100
USER 1000
WORKDIR /home/orquestra

ENV RAY_STORAGE=/tmp
# This environment variable configures the Ray runtime to download Git imports.
# Without this set, Git imports are ignored
ENV ORQ_RAY_DOWNLOAD_GIT_IMPORTS=1
