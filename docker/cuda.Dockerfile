# syntax=docker/dockerfile:1.5
# Base image for running Orquestra tasks that require a GPU.
# Published at hub.nexus.orquestra.io/zapatacomputing/orquestra-sdk-base with -cuda suffix
# Mounted gpu has cuda v11.5
FROM nvcr.io/nvidia/cuquantum-appliance:22.03-cirq
ARG SDK_REQUIREMENT

WORKDIR /app

# https://askubuntu.com/questions/1408016/the-following-signatures-couldnt-be-verified-because-the-public-key-is-not-avai
RUN <<EOF
apt-key adv --fetch-keys https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/3bf863cc.pub
apt update --yes
apt install --yes wget build-essential gcc git openssh-client
apt install --yes python3-pip
EOF

#download cuquantum from https://developer.nvidia.com/cuquantum-downloads?target_os=Linux&target_arch=x86_64&Distribution=Ubuntu&target_version=20.04&target_type=deb_local
RUN <<EOF
wget https://developer.download.nvidia.com/compute/cuquantum/22.07.0/local_installers/cuquantum-local-repo-ubuntu2004-22.07.0_1.0-1_amd64.deb
dpkg -i cuquantum-local-repo-ubuntu2004-22.07.0_1.0-1_amd64.deb
cp /var/cuquantum-local-repo-ubuntu2004-22.07.0/cuquantum-*-keyring.gpg /usr/share/keyrings/
apt update --yes
apt install --yes cuquantum cuquantum-dev cuquantum-doc
EOF

RUN <<EOF
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/nvidia/cuquantum/lib/
echo "export PATH=/usr/local/cuda-11.5/bin${PATH:+:${PATH} }" >> ~/.bashrc
. ~/.bashrc
EOF

ENV CUQUANTUM_DIR=/opt/nvidia/cuquantum

# get required tools to build qsim
RUN <<EOF
python -m pip install pybind11
apt install --yes git
export DEBIAN_FRONTEND=noninteractive
apt install --yes cmake
EOF

# install qsimcirq and orquestra SDK
RUN <<EOF
git clone https://github.com/quantumlib/qsim.git
cd qsim
make clean
make
python -m pip install .
python -m pip install "${SDK_REQUIREMENT}"
EOF

RUN useradd -ms /bin/bash -d /home/orquestra orquestra --uid 1000 --gid 100
USER 1000
WORKDIR /home/orquestra

ENV RAY_STORAGE=/tmp
# This environment variable configures the Ray runtime to download Git imports.
# Without this set, Git imports are ignored
ENV ORQ_RAY_DOWNLOAD_GIT_IMPORTS=1
