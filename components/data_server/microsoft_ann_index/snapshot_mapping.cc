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

#include "components/data_server/microsoft_ann_index/snapshot_mapping.h"

#include <fstream>

namespace kv_server {
namespace microsoft {

ANNSnapshotMapping::ANNSnapshotMapping(
    const std::string& path, SNAPSHOT_STATUS& status,
    privacy_sandbox::server_common::log::PSLogContext& log_context) {
  // TODO: bulk allocation and memory optimisation task.ms/56392737
  std::ifstream file(path, std::ios::binary);
  if (!file || !file.good()) {
    PS_LOG(ERROR, log_context) << "can't open mapping file: " << path;
    status = SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_MAPPING_FILE;
    return;
  }
  uint32_t totalCount;
  uint32_t strLen;
  file.read(reinterpret_cast<char*>(&totalCount), 4);
  if (!file.good()) {
    PS_LOG(ERROR, log_context) << "can't read mapping file: " << path;
    status = SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_MAPPING_FILE;
    return;
  }
  if (totalCount == 0 || totalCount > 2000000000) {
    PS_LOG(ERROR, log_context)
        << "incorrect amount of records in mapping file: " << totalCount;
    status = SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_MAPPING_FILE;
    return;
  }
  storage_.resize(totalCount, {});
  for (size_t i = 0; i < totalCount; ++i) {
    if (!file.good()) {
      PS_LOG(ERROR, log_context) << "can't read mapping file: " << path;
      status = SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_MAPPING_FILE;
      return;
    }
    file.read(reinterpret_cast<char*>(&strLen), 4);
    if (strLen == 0 || strLen > 2000000000) {
      PS_LOG(ERROR, log_context)
          << "incorrect record in mapping file: size is " << strLen;
      status = SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_MAPPING_FILE;
      return;
    }
    if (!file.good()) {
      PS_LOG(ERROR, log_context) << "can't read mapping file: " << path;
      status = SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_MAPPING_FILE;
      return;
    }
    storage_[i].resize(strLen, '\0');
    file.read(storage_[i].data(), strLen);
  }

  if (!file.good()) {
    PS_LOG(ERROR, log_context) << "can't read mapping file: " << path;
    status = SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_MAPPING_FILE;
    return;
  } else {
    // force trigger EOF
    file.read(reinterpret_cast<char*>(&strLen), 1);

    if (!file.eof()) {
      PS_LOG(ERROR, log_context) << "invalid mapping file: " << path
                                 << ", expected EOF, but more bytes found";
      status = SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_MAPPING_FILE;
      return;
    }
  }
}

const std::string& ANNSnapshotMapping::GetStr(const size_t vecId) const {
  if (vecId >= storage_.size()) {
    static const std::string emptyString;
    // technically, it should not happen, if index is correct.
    // we returning empty string in that case
    return emptyString;
  }
  return storage_[vecId];
}

const size_t ANNSnapshotMapping::GetCapacity() const { return storage_.size(); }

}  // namespace microsoft
}  // namespace kv_server
