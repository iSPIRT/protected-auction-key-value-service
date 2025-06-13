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

#ifndef COMPONENTS_DATA_SERVER_MICROSOFT_ANN_INDEX_ANN_INDEX_H_
#define COMPONENTS_DATA_SERVER_MICROSOFT_ANN_INDEX_ANN_INDEX_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "components/data_server/microsoft_ann_index/snapshot_keeper.h"

namespace kv_server {
namespace microsoft {

class ANNIndex {
 public:
  ANNIndex() = default;

  // Do ANN search with each key from key-value set.
  std::unique_ptr<std::unordered_map<std::string, std::vector<std::string>>>
  GetKeyValueSet(const absl::flat_hash_set<std::string_view>& key_set) const;

  // adding new ANN snapshot for replacing current one (or using new one after
  // initialization).
  // snapshot cant be added in several cases - fpath is not correct, snapshot
  // files are broken, snapshot is not fresh, etc.
  //
  // fpath should point to the container file with snapshot.
  // snapshotName should be unique for each snapshot and incremental for newer
  // versions. New snapshot will be ignored if it has the lower name as the
  // current one (we are using string compare operation)
  SNAPSHOT_STATUS TryAddANNSnapshot(
      const std::string_view& snapshotName, const std::string_view& fpath,
      privacy_sandbox::server_common::log::PSLogContext& log_context);

 private:
  ANNSnapshotKeeper keeper_;
};

}  // namespace microsoft
}  // namespace kv_server

#endif  // COMPONENTS_DATA_SERVER_MICROSOFT_ANN_INDEX_ANN_INDEX_H_
