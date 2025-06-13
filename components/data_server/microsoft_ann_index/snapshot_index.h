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

#ifndef COMPONENTS_DATA_SERVER_MICROSOFT_ANN_INDEX_ANN_SNAPSHOT_INDEX_H_
#define COMPONENTS_DATA_SERVER_MICROSOFT_ANN_INDEX_ANN_SNAPSHOT_INDEX_H_

#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "components/data_server/microsoft_ann_index/snapshot_config.h"

// diskann headers
#include "abstract_index.h"
// diskann headers

namespace kv_server {
namespace microsoft {

class ANNSnapshotIndex {
 public:
  ANNSnapshotIndex() = delete;
  ANNSnapshotIndex(const ANNSnapshotIndex&) = delete;
  ANNSnapshotIndex(ANNSnapshotIndex&&) = delete;
  ANNSnapshotIndex& operator=(const ANNSnapshotIndex&) = delete;
  ANNSnapshotIndex& operator=(ANNSnapshotIndex&&) = delete;

  ANNSnapshotIndex(const std::shared_ptr<ANNSnapshotConfig> config,
                   SNAPSHOT_STATUS& status);
  bool Search(const absl::flat_hash_set<std::string_view>& key_set,
              std::vector<std::vector<uint32_t>>& results,
              std::vector<std::pair<uint32_t, uint32_t>>& stats) const;

 private:
  const std::shared_ptr<ANNSnapshotConfig> config_;
  std::unique_ptr<diskann::AbstractIndex> index_ptr_;
};

}  // namespace microsoft
}  // namespace kv_server

#endif  // COMPONENTS_DATA_SERVER_MICROSOFT_ANN_INDEX_ANN_SNAPSHOT_INDEX_H_
