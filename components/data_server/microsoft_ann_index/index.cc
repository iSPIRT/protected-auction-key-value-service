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

#include "components/data_server/microsoft_ann_index/index.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "components/telemetry/server_definition.h"

namespace kv_server {
namespace microsoft {

std::unique_ptr<std::unordered_map<std::string, std::vector<std::string>>>
ANNIndex::GetKeyValueSet(
    const absl::flat_hash_set<std::string_view>& key_set) const {
  if (!keeper_.HasANNSnapshots()) {
    return nullptr;
  }
  auto snapshot = keeper_.GetActualANNSnapshot();

  auto capacity = snapshot->Mapping.GetCapacity();

  std::vector<std::pair<uint32_t, uint32_t>> search_statistics(
      key_set.size(), std::make_pair(0, 0));
  std::vector<std::vector<uint32_t>> results(
      key_set.size(),
      std::vector<uint32_t>(snapshot->Config->TopCount, capacity));

  // search_statistics will be need later for reporting
  snapshot->Index.Search(key_set, results, search_statistics);
  auto& map = snapshot->Mapping;

  auto retVal = std::make_unique<
      std::unordered_map<std::string, std::vector<std::string>>>();
  retVal->reserve(key_set.size());

  for (auto& key : key_set) {
    // TODO: filling of result in parallel task.ms/56392737
    auto pos = retVal->emplace(std::string(key), std::vector<std::string>());
    const auto& targetResult = results[retVal->size() - 1];
    pos.first->second.reserve(targetResult.size());
    for (const auto& embNum : targetResult) {
      if (embNum >= capacity) {
        // empty result means that something goes wrong - for example, keys were
        // wrong size. In that case, for this key we do not return anything
        pos.first->second = {};
        break;
      }
      pos.first->second.push_back(map.GetStr(embNum));
    }
  }
  return retVal;
}

SNAPSHOT_STATUS ANNIndex::TryAddANNSnapshot(
    const std::string_view& snapshotName, const std::string_view& fpath,
    privacy_sandbox::server_common::log::PSLogContext& log_context) {
  int old_capacity = keeper_.DequeCapacity();
  auto status = keeper_.TryAddANNSnapshot(snapshotName, fpath, log_context);
  if (status == SNAPSHOT_STATUS::OK) {
    LogIfError(KVServerContextMap()
                   ->SafeMetric()
                   .LogUpDownCounter<kMicrosoftAnnSnapshotLoadSuccessCount>(1));
  } else if (status == SNAPSHOT_STATUS::NOT_FRESH) {
    LogIfError(KVServerContextMap()
                   ->SafeMetric()
                   .LogUpDownCounter<kMicrosoftAnnSnapshotLoadExpiredCount>(1));
  } else {
    LogIfError(KVServerContextMap()
                   ->SafeMetric()
                   .LogUpDownCounter<kMicrosoftAnnSnapshotLoadErrorCount>(1));
  }
  keeper_.TryRemoveUnusedANNSnapshots();
  {
    int capacity = keeper_.DequeCapacity();
    auto capacity_diff = capacity - old_capacity;
    if (capacity_diff != 0) {
      LogIfError(KVServerContextMap()
                     ->SafeMetric()
                     .LogUpDownCounter<kMicrosoftAnnActiveSnapshotCount>(
                         capacity_diff));
    }
  }
  return status;
}

}  // namespace microsoft
}  // namespace kv_server
