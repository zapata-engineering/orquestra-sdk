ARG SDK_VERSION=0.53.0
from hub.stage.nexus.orquestra.io/zapatacomputing/orquestra-sdk-base:$SDK_VERSION

USER root
RUN apt-get update && \
  apt-get install wget -y && \
  rm -rf /var/lib/apt/lists/*
ARG JULIA_MINOR_VERSION=1.9
ARG JULIA_PATCH_VERSION=1.9.0
RUN wget https://julialang-s3.julialang.org/bin/linux/x64/${JULIA_MINOR_VERSION}/julia-${JULIA_PATCH_VERSION}-linux-x86_64.tar.gz && \
  tar xf julia-${JULIA_PATCH_VERSION}-linux-x86_64.tar.gz -C /home/orquestra && \
  ln -s /home/orquestra/julia-${JULIA_PATCH_VERSION}/bin/julia /usr/local/bin/julia && \
  rm julia-${JULIA_PATCH_VERSION}-linux-x86_64.tar.gz

USER orquestra
RUN julia -e 'using Pkg; Pkg.add("Jabalizer")'
