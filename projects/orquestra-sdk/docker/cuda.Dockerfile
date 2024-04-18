# syntax=docker/dockerfile:1.5
# This is based on the previous QML image.

# Possible base images
# nvcr.io/nvidia/cuquantum-appliance:22.11      python 3.8, CUDA v11.8, CUDNN 8.7
# nvidia/cuda:11.8.0-cudnn8-runtime-ubuntu22.04 python n/a, CUDA v11.8, CUDNN 8.7
# pytorch/pytorch:2.0.1-cuda11.7-cudnn8-runtime python 3.10 (conda),
ARG CUDA_MINOR_VERSION=11.8
FROM nvidia/cuda:${CUDA_MINOR_VERSION}.0-runtime-ubuntu22.04

ARG SDK_REQUIREMENT
ARG PYTHON_VERSION=3.11.6
ARG TARGETARCH="amd64"

RUN <<EOF
set -ex
apt-get update --yes
apt-get upgrade --yes
#pyenv requires wget, gcc, and make
apt-get install curl wget git ssh gcc make -y
# pyenv requirements https://github.com/pyenv/pyenv/wiki#suggested-build-environment
DEBIAN_FRONTEND=noninteractive apt install build-essential libssl-dev zlib1g-dev \
    libbz2-dev libreadline-dev libsqlite3-dev curl \
    libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev -y

# If statement installs any additional dependencies needed for building for ARM
# (development using an M1 is fun :') )
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

#juliacall requires libpython shared object file, so building from source with pyenv and `--enable-shared`
# TODO: python3.10 still gets installed somewhere along the way. Any way to avoid this?
RUN <<EOF
git clone https://github.com/pyenv/pyenv.git /home/orquestra/.pyenv
PYTHON_CONFIGURE_OPTS="--enable-shared" /home/orquestra/.pyenv/bin/pyenv install ${PYTHON_VERSION}
EOF

ENV PATH="/home/orquestra/.pyenv/versions/${PYTHON_VERSION}/bin:$PATH"

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
