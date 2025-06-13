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

#ifndef COMPONENTS_DATA_SERVER_MICROSOFT_ANN_INDEX_ANN_SNAPSHOT_MAPPING_H_
#define COMPONENTS_DATA_SERVER_MICROSOFT_ANN_INDEX_ANN_SNAPSHOT_MAPPING_H_

#include <string>
#include <vector>

#include "components/data_server/microsoft_ann_index/snapshot_config.h"
#include "src/logger/request_context_logger.h"

namespace kv_server {
namespace microsoft {

class ANNSnapshotMapping {
 public:
  ANNSnapshotMapping() = delete;
  ANNSnapshotMapping(const ANNSnapshotMapping&) = delete;
  ANNSnapshotMapping(ANNSnapshotMapping&&) = delete;
  ANNSnapshotMapping& operator=(const ANNSnapshotMapping&) = delete;
  ANNSnapshotMapping& operator=(ANNSnapshotMapping&&) = delete;

  ANNSnapshotMapping(
      const std::string& path, SNAPSHOT_STATUS& status,
      privacy_sandbox::server_common::log::PSLogContext& log_context);
  const std::string& GetStr(const size_t vecId) const;
  const size_t GetCapacity() const;

 private:
  std::vector<std::string> storage_;
};

}  // namespace microsoft
}  // namespace kv_server

#endif  // COMPONENTS_DATA_SERVER_MICROSOFT_ANN_INDEX_ANN_SNAPSHOT_MAPPING_H_
