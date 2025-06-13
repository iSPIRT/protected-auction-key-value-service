// Copyright (c) Microsoft Corporation
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

#ifndef COMPONENTS_DATA_SERVER_MICROSOFT_ANN_INDEX_ANN_SNAPSHOT_CONFIG_H_
#define COMPONENTS_DATA_SERVER_MICROSOFT_ANN_INDEX_ANN_SNAPSHOT_CONFIG_H_

#include <string>

#include "src/logger/request_context_logger.h"

namespace kv_server {
namespace microsoft {

enum class SNAPSHOT_STATUS {
  IN_PROGRESS,
  OK,
  NOT_FRESH,
  FILESYSTEM_CREATE_FOLDER_ERROR,
  IO_CANT_OPEN_INCOMING_SNAPSHOT_FILE,
  IFSTREAM_FAILURE,
  OFSTREAM_FAILURE,
  INVALID_SNAPSHOT,
  INVALID_SNAPSHOT_CONFIG,
  INVALID_SNAPSHOT_INDEX,
  INVALID_SNAPSHOT_INDEX_DATA,
  INVALID_SNAPSHOT_MAPPING,
  SNAPSHOT_LOAD_ERROR_INVALID_MAPPING_FILE,
  SNAPSHOT_LOAD_ERROR_INVALID_INDEX
};

struct ANNSnapshotConfig {
  ANNSnapshotConfig() = delete;
  ANNSnapshotConfig(const ANNSnapshotConfig&) = delete;
  ANNSnapshotConfig(ANNSnapshotConfig&&) = delete;
  ANNSnapshotConfig& operator=(const ANNSnapshotConfig&) = delete;
  ANNSnapshotConfig& operator=(ANNSnapshotConfig&&) = delete;

  ANNSnapshotConfig(
      privacy_sandbox::server_common::log::PSLogContext& log_context)
      : log_context_(log_context) {}

  ~ANNSnapshotConfig();

  std::string SnapshotName;

  // those are the same from incoming snapshot
  uint32_t Dimension;
  uint32_t QueryNeighborsCount;
  uint32_t TopCount;
  // unfortunately we need to use str here. DiskANN lib expects it as an input.
  std::string VectorTypeStr;

  // those are filled manually
  std::string SnapshotFolder;
  std::string IndexBaseFilename;
  std::string IndexDataFilename;
  std::string MappingFilename;
  std::string ConfigJsonFilename;

  privacy_sandbox::server_common::log::PSLogContext& log_context_;
};

}  // namespace microsoft
}  // namespace kv_server

#endif  // COMPONENTS_DATA_SERVER_MICROSOFT_ANN_INDEX_ANN_SNAPSHOT_CONFIG_H_
