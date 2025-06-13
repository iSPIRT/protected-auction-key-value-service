# Copyright 2022 Google LLC
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

load("@io_bazel_rules_docker//container:container.bzl", "container_pull")

def container_deps():
    images = {
        "runtime-debian": {
            "arch_hashes": {
                # stable-20221004-slim
                "amd64": "a4912461baca94ca557af4e779857867a25e0215d157d2dc04f148811e7877f8",
                "arm64": "af9a018b749427a53fded449bd1fbb2cbdc7077d8922b7ebb7bd6478ed40d8e7",
            },
            "registry": "docker.io",
            "repository": "library/debian",
        },
    }

    [
        container_pull(
            name = img_name + "-" + arch,
            digest = "sha256:" + hash,
            registry = image["registry"],
            repository = image["repository"],
        )
        for img_name, image in images.items()
        for arch, hash in image["arch_hashes"].items()
    ]
