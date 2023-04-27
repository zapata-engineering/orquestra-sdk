# syntax=docker/dockerfile:1.5
FROM python:3.9-slim-bullseye
ARG SDK_VERSION

RUN <<EOF
apt update --yes
apt upgrade --yes
apt install --yes --no-install-recommends git
python -m pip install -U pip wheel
python -m pip install orquestra-sdk[ray]==${SDK_VERSION}
EOF

RUN useradd -ms /bin/bash -d /home/orquestra orquestra --uid 1000 --gid 100
USER 1000
WORKDIR /home/orquestra

ENV RAY_STORAGE=/tmp
# This environment variable configures the Ray runtime to download Git imports.
# Without this set, Git imports are ignored
ENV ORQ_RAY_DOWNLOAD_GIT_IMPORTS=1
# This environment variable configures the Ray runtime to apply task resources to Ray functions
# Without this set, task resources are ignored and not taken into account when scheduling.
ENV ORQ_RAY_SET_TASK_RESOURCES=1