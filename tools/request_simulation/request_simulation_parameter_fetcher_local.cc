// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <string>

#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "tools/request_simulation/request_simulation_parameter_fetcher.h"

ABSL_FLAG(std::string, delta_dir, "", "The local directory for delta files");
ABSL_FLAG(std::string, realtime_delta_dir, "",
          "The local directory for realtime delta files");

namespace kv_server {

NotifierMetadata
RequestSimulationParameterFetcher::GetBlobStorageNotifierMetadata() const {
  std::string directory = absl::GetFlag(FLAGS_delta_dir);
  LOG(INFO) << "The local delta file directory is " << directory;
  return LocalNotifierMetadata{.local_directory = std::move(directory)};
}

NotifierMetadata
RequestSimulationParameterFetcher::GetRealtimeNotifierMetadata() const {
  std::string directory = absl::GetFlag(FLAGS_realtime_delta_dir);
  LOG(INFO) << "The local realtim delta file directory is " << directory;
  return LocalNotifierMetadata{.local_directory = std::move(directory)};
}

}  // namespace kv_server
