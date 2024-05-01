IMAGE_NAME ?= hub.stage.nexus.orquestra.io/zapatacomputing/orquestra-sdk-base
IMAGE_TAG ?= dev

SDK_REQUIREMENT ?= "git+https://github.com/zapata-engineering/orquestra-sdk.git@main"

#####################
# Build the sdk base image
#####################
build-base:
	@echo "Building base image with SDK version $(SDK_REQUIREMENT)"
	DOCKER_BUILDKIT=1 docker build -t $(IMAGE_NAME):$(IMAGE_TAG) -f Dockerfile . \
		--build-arg SDK_REQUIREMENT="$(SDK_REQUIREMENT)"

#####################
# Build the sdk cuda image
#####################
# Notes:
# - This must be built for --platform=linux/amd64
#####################
build-cuda:
	@echo "Building cuda image with SDK version $(SDK_REQUIREMENT)"
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG)-cuda -f cuda.Dockerfile . \
		--build-arg SDK_REQUIREMENT="$(SDK_REQUIREMENT)" \
		--platform linux/amd64
