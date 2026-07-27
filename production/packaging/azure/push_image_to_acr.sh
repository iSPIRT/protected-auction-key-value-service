#!/bin/bash
# Copyright (C) Microsoft Corporation. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Squash the Key Value service OCI image to a single layer (CCE policy
# compatibility) and push to Azure Container Registry. Modeled after
# bidding-and-auction-servers production/packaging/azure/push_images_to_acr.sh
# and the KV squash + ACR tagging in depa-inferencing release workflows.
#
# Usage:
#   BUILD_FLAVOR=prod RELEASE_VERSION=4.8.1.2 ./push_image_to_acr.sh
#
# Required environment variables:
#   BUILD_FLAVOR     prod | nonprod (no default)
#   RELEASE_VERSION  e.g. 1.2.1.2 (no default)
#
# Optional:
#   ACR_REGISTRY     e.g. myregistry.azurecr.io (default: ispirt.azurecr.io)
#   ACR_REPO_PATH    path under the registry (default: depainferencing/azure)
#   DIST_DIR        directory containing key_value_service_image.tar
#                   (default: ${WORKSPACE}/dist, fallback: ${WORKSPACE}/dist/debian)
#   WORKSPACE       repo root (default: ../../../ from this script)
#   SKIP_PUSH       if non-empty, squash and tag locally but do not docker push

set -o pipefail
set -o errexit

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
WORKSPACE="${WORKSPACE:-$(readlink -f "${SCRIPT_DIR}/../../..")}"
DIST_DIR="${DIST_DIR:-${WORKSPACE}/dist}"
if [[ ! -f "${DIST_DIR}/key_value_service_image.tar" ]]; then
  DIST_DIR="${WORKSPACE}/dist/debian"
fi

ACR_REGISTRY="${ACR_REGISTRY:-ispirt.azurecr.io}"
ACR_REPO_PATH="${ACR_REPO_PATH:-depainferencing/azure}"
ACR_BASE="${ACR_REGISTRY}/${ACR_REPO_PATH}"

if [[ -z "${BUILD_FLAVOR:-}" ]]; then
  printf "Error: BUILD_FLAVOR must be set (e.g. prod or nonprod)\n" >&2
  exit 1
fi
if [[ -z "${RELEASE_VERSION:-}" ]]; then
  printf "Error: RELEASE_VERSION must be set\n" >&2
  exit 1
fi

SERVICE="key_value_service"
SQUASH_PREFIX="kv-squash"

printf "==== Squash and push Key Value image to ACR ====\n"
printf " BUILD_FLAVOR: %s\n" "${BUILD_FLAVOR}"
printf " RELEASE_VERSION: %s\n" "${RELEASE_VERSION}"
printf " ACR_BASE: %s\n" "${ACR_BASE}"
printf " DIST_DIR: %s\n" "${DIST_DIR}"
printf "\n"

squash_and_push_kv() {
  local -r IMAGE_TAR="${DIST_DIR}/key_value_service_image.tar"
  if [[ ! -f "${IMAGE_TAR}" ]]; then
    printf "Error: %s not found\n" "${IMAGE_TAR}" >&2
    exit 1
  fi

  local -r ACR_TAG="${ACR_BASE}/${SERVICE//_/-}:${BUILD_FLAVOR}-${RELEASE_VERSION}"

  printf -- "---- %s ----\n" "${SERVICE}"

  local LOAD_OUTPUT LOADED_IMAGE
  LOAD_OUTPUT=$(docker load -i "${IMAGE_TAR}")
  LOADED_IMAGE=$(echo "${LOAD_OUTPUT}" | grep "Loaded image:" | sed 's/Loaded image: //')
  printf "Loaded: %s\n" "${LOADED_IMAGE}"

  docker create --name "${SQUASH_PREFIX}-temp" "${LOADED_IMAGE}"
  docker export "${SQUASH_PREFIX}-temp" -o "${SQUASH_PREFIX}-squashed.tar"
  docker import "${SQUASH_PREFIX}-squashed.tar" "${SQUASH_PREFIX}-base:temp"

  local ENTRYPOINT_JSON CMD_JSON ENV_VARS WORKDIR
  ENTRYPOINT_JSON=$(docker inspect "${LOADED_IMAGE}" --format '{{json .Config.Entrypoint}}')
  CMD_JSON=$(docker inspect "${LOADED_IMAGE}" --format '{{json .Config.Cmd}}')
  ENV_VARS=$(docker inspect "${LOADED_IMAGE}" --format '{{range .Config.Env}}ENV {{.}}{{"\n"}}{{end}}')
  WORKDIR=$(docker inspect "${LOADED_IMAGE}" --format '{{.Config.WorkingDir}}')
  WORKDIR="${WORKDIR:-/}"

  {
    echo "FROM ${SQUASH_PREFIX}-base:temp"
    echo "${ENV_VARS}"
    echo "WORKDIR ${WORKDIR}"
    if [[ "${ENTRYPOINT_JSON}" != "null" ]] && [[ -n "${ENTRYPOINT_JSON}" ]]; then
      echo "ENTRYPOINT ${ENTRYPOINT_JSON}"
    fi
    if [[ "${CMD_JSON}" != "null" ]] && [[ -n "${CMD_JSON}" ]]; then
      echo "CMD ${CMD_JSON}"
    fi
  } >Dockerfile."${SQUASH_PREFIX}-squashed"

  docker build -f Dockerfile."${SQUASH_PREFIX}-squashed" -t "${SQUASH_PREFIX}-final:temp" .

  local LAYER_COUNT
  LAYER_COUNT=$(docker inspect "${SQUASH_PREFIX}-final:temp" | jq -r '.[0].RootFS.Layers | length')
  printf "Squashed image has %s layer(s)\n" "${LAYER_COUNT}"

  docker rm "${SQUASH_PREFIX}-temp"
  docker rmi "${SQUASH_PREFIX}-base:temp" 2>/dev/null || true
  rm -f "${SQUASH_PREFIX}-squashed.tar" Dockerfile."${SQUASH_PREFIX}-squashed"

  docker tag "${SQUASH_PREFIX}-final:temp" "${ACR_TAG}"
  if [[ -n "${SKIP_PUSH:-}" ]]; then
    printf "SKIP_PUSH set; not pushing. Image tagged as %s\n" "${ACR_TAG}"
    printf "Layer count: %s\n" "${LAYER_COUNT}"
  else
    printf "Pushing %s to ACR...\n" "${ACR_TAG}"
    docker push "${ACR_TAG}"
    printf "Successfully pushed %s (squashed, %s layer(s))\n" "${ACR_TAG}" "${LAYER_COUNT}"
  fi

  docker rmi "${SQUASH_PREFIX}-final:temp" 2>/dev/null || true
  docker rmi "${LOADED_IMAGE}" 2>/dev/null || true

  printf "\n"
}

cd "${WORKSPACE}"

squash_and_push_kv

printf "==== Done: Key Value image squashed%s ====\n" "${SKIP_PUSH:+ (push skipped)}"
printf " - %s/key-value-service:%s-%s\n" "${ACR_BASE}" "${BUILD_FLAVOR}" "${RELEASE_VERSION}"
