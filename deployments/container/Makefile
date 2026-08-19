# Copyright (c) 2021-2022, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

BUILD_MULTI_ARCH_IMAGES ?= no
DOCKER ?= docker
BUILDX =
ifeq ($(BUILD_MULTI_ARCH_IMAGES),true)
BUILDX = buildx
endif

##### Global variables #####
include $(CURDIR)/versions.mk

ifeq ($(IMAGE_NAME),)
REGISTRY ?= nvidia
IMAGE_NAME := $(REGISTRY)/k8s-cc-manager
endif

IMAGE_VERSION := $(VERSION)

IMAGE_TAG ?= $(IMAGE_VERSION)-$(DIST)
IMAGE = $(IMAGE_NAME):$(IMAGE_TAG)

OUT_IMAGE_NAME ?= $(IMAGE_NAME)
OUT_IMAGE_VERSION ?= $(IMAGE_VERSION)
OUT_IMAGE_TAG = $(OUT_IMAGE_VERSION)-$(DIST)
OUT_IMAGE = $(OUT_IMAGE_NAME):$(OUT_IMAGE_TAG)

##### Public rules #####
DISTRIBUTIONS := distroless
DEFAULT_PUSH_TARGET := distroless

PUSH_TARGETS := $(patsubst %, push-%, $(DISTRIBUTIONS))
BUILD_TARGETS := $(patsubst %, build-%, $(DISTRIBUTIONS))
TEST_TARGETS := $(patsubst %, build-%, $(DISTRIBUTIONS))

.PHONY: $(DISTRIBUTIONS) $(PUSH_TARGETS) $(BUILD_TARGETS) $(TEST_TARGETS)

ifneq ($(BUILD_MULTI_ARCH_IMAGES),true)
include $(CURDIR)/deployments/container/native-only.mk
else
include $(CURDIR)/deployments/container/multi-arch.mk
endif

# For the default push target we also push a short tag equal to the version.
# We skip this for the development release
DEVEL_RELEASE_IMAGE_VERSION ?= devel
PUSH_MULTIPLE_TAGS ?= true
ifeq ($(strip $(OUT_IMAGE_VERSION)),$(DEVEL_RELEASE_IMAGE_VERSION))
PUSH_MULTIPLE_TAGS = false
endif
ifeq ($(PUSH_MULTIPLE_TAGS),true)
push-$(DEFAULT_PUSH_TARGET): push-short
endif

push-%: DIST = $(*)
push-short: DIST = $(DEFAULT_PUSH_TARGET)


build-%: DIST = $(*)
build-%: DOCKERFILE_SUFFIX = $(*)
build-%: DOCKERFILE = $(CURDIR)/deployments/container/Dockerfile.$(DOCKERFILE_SUFFIX)

# Each name in DISTRIBUTIONS maps to build-<name> (e.g. build-distroless).
$(DISTRIBUTIONS): %: build-%
$(BUILD_TARGETS): build-%:
	DOCKER_BUILDKIT=1 \
		$(DOCKER) $(BUILDX) build --pull \
			$(DOCKER_BUILD_OPTIONS) \
			$(DOCKER_BUILD_PLATFORM_OPTIONS) \
			--tag $(IMAGE) \
			--build-arg BASE_DIST="$(DIST)" \
			--build-arg CUDA_VERSION="$(CUDA_VERSION)" \
			--build-arg VERSION="$(VERSION)" \
			--build-arg GIT_COMMIT="$(GIT_COMMIT)" \
			--build-arg BUILD_TIMESTAMP="$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")" \
			--build-arg CVE_UPDATES="$(CVE_UPDATES)" \
			--build-arg GPU_ADMIN_TOOLS_VERSION="$(GPU_ADMIN_TOOLS_VERSION)" \
			--build-arg RUNTIME_VERSION="$(RUNTIME_VERSION)" \
			--file $(DOCKERFILE) \
			$(CURDIR)

PYTHON ?= python3

# The Python dependencies are not fully pinned, so the only way to know which
# dependencies and licenses were included is to inspect the actual Docker image
# that was built.
#
# In CI the image is built as a multi-arch image with buildx and pushed to the
# registry, so the linux/amd64 variant has to be pulled before it can be
# inspected. For a normal local build the image is already available locally
# and can be inspected directly. In both cases the goal is the same: verify the
# licenses against the actual image that was built.
#
# TODO: once the Python dependencies are fully pinned, generate and verify the
# notices directly from that pinned dependency set and remove the Docker image
# inspection step entirely.
define verify-notices-against-image
@candidate=$$($(DOCKER) image inspect --format \
	'{{if .RepoDigests}}{{index .RepoDigests 0}}{{else}}{{.Id}}{{end}}' \
	$(IMAGE)) && \
echo "Verifying $(IMAGE) as $$candidate" && \
$(PYTHON) $(CURDIR)/hack/generate-third-party-notices.py --image "$$candidate"
@if ! git diff --exit-code -- THIRD_PARTY_NOTICES.md; then \
	echo "ERROR: $(IMAGE) is not what THIRD_PARTY_NOTICES.md describes."; \
	echo "Run 'make -f deployments/container/Makefile third-party-notices' and commit the result."; \
	exit 1; \
fi
endef

.PHONY: third-party-notices
third-party-notices:
	@$(PYTHON) $(CURDIR)/hack/generate-third-party-notices.py

.PHONY: verify-third-party-notices
verify-third-party-notices: DIST = $(DEFAULT_PUSH_TARGET)
verify-third-party-notices:
	$(verify-notices-against-image)

.PHONY: verify-third-party-notices-from-registry
verify-third-party-notices-from-registry: DIST = $(DEFAULT_PUSH_TARGET)
verify-third-party-notices-from-registry:
	@$(DOCKER) pull --platform linux/amd64 $(IMAGE)
	$(verify-notices-against-image)

.PHONY: bump-commit
BUMP_COMMIT := Bump to version $(VERSION)
bump-commit:
	@git log | if [ ! -z "$$(grep -o '$(BUMP_COMMIT)' | sort -u)" ]; then \
		echo "\nERROR: '$(BUMP_COMMIT)' already committed\n"; \
		exit 1; \
	fi
	@git add versions.mk
	@git commit -m "$(BUMP_COMMIT)"
	@echo "Applied the diff:"
	@git --no-pager diff HEAD~1
