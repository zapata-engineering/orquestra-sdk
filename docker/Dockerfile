# syntax=docker/dockerfile:1.5
# Base image for running Orquestra tasks.
# Published at hub.nexus.orquestra.io/zapatacomputing/orquestra-sdk-base
FROM python:3.9-slim-bullseye
ARG SDK_REQUIREMENT

RUN <<EOF
apt update --yes
apt upgrade --yes
apt install --yes --no-install-recommends git openssh-client
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
