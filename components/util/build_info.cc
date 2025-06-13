/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "components/util/build_info.h"

#include <ostream>

#include "absl/log/log.h"

namespace kv_server {

void LogBuildInfo() {
  LOG(INFO) << "Build platform: " << BuildPlatform() << std::endl
            << "Build toolchain hash: " << BuildToolchainHash() << std::endl
            << "Build flavor: " << BuildFlavor() << std::endl
            << "Build version: " << BuildVersion() << std::endl;
}

std::string_view BuildPlatform() { return kVersionBuildPlatform; }

// The compiler/target-cpu combination used to compile this executable.
std::string_view BuildToolchainHash() { return kVersionBuildToolchainHash; }

// String indicating the build-time config used to build this executable.
std::string_view BuildFlavor() { return kVersionBuildFlavor; }

// semantic version of the application
std::string_view BuildVersion() { return kVersionBuildVersion; }

}  // namespace kv_server
