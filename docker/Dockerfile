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
apt-get update --yes
apt-get upgrade --yes
apt-get install --yes --no-install-recommends git openssh-client

if [ "$TARGETARCH" = "arm64" ]; then
  apt-get install --yes --no-install-recommends gcc libc-dev
fi

rm -rf /var/lib/apt/lists/*

useradd -ms /bin/bash -d /home/orquestra orquestra --uid 1000 --gid 100

mkdir -p /opt/orquestra
chown -R 1000:100 /opt/orquestra
EOF

USER 1000
WORKDIR /home/orquestra

ENV VIRTUAL_ENV=/opt/orquestra/venv
RUN python -m venv "$VIRTUAL_ENV" --prompt system

RUN <<EOF
set -ex
. "$VIRTUAL_ENV/bin/activate"
python -m pip install --no-cache-dir -U pip wheel
python -m pip install --no-cache-dir "${SDK_REQUIREMENT}"
EOF

# Prefer to use pip, python, and other binaries from the virtual env.
ENV PATH="/opt/orquestra/venv/bin:$PATH"

# This is needed to ensure that the virtual env is used when running
# non-interactive shells (e.g. when running a startup script)
# https://www.gnu.org/software/bash/manual/html_node/Bash-Startup-Files.html#Bash-Startup-Files
RUN echo "source ${VIRTUAL_ENV}/bin/activate" >> /opt/orquestra/source-venv
ENV BASH_ENV=/opt/orquestra/source-venv

ENV RAY_STORAGE=/tmp
# This environment variable configures the Ray runtime to download Git imports.
# Without this set, Git imports are ignored
ENV ORQ_RAY_DOWNLOAD_GIT_IMPORTS=1
