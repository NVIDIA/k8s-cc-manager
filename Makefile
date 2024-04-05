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

GO_CMD ?= go
GO_FMT ?= gofmt
GO_SRC := $(shell find . -type f -name '*.go' -not -path "./vendor/*")

BINARY_NAME ?= cc-manager

build:
	${GO_CMD} build -o ${BINARY_NAME} ./...

test:
	${GO_CMD} test ./...

vendor:
	${GO_CMD} mod tidy
	${GO_CMD} mod vendor
	${GO_CMD} mod verify

check-vendor: vendor
	git diff --quiet HEAD -- go.mod go.sum vendor

.PHONY: vendor check-vendor build test
