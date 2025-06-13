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

#include "components/data_server/microsoft_ann_index/snapshot_index.h"

#include <filesystem>
#include <string>

// diskann headers
#include "abstract_index.h"
#include "distance.h"
#include "index.h"
#include "index_config.h"
#include "index_factory.h"
#include "utils.h"
// diskann headers

namespace kv_server {
namespace microsoft {

ANNSnapshotIndex::ANNSnapshotIndex(
    const std::shared_ptr<ANNSnapshotConfig> config, SNAPSHOT_STATUS& status)
    : config_(config) {
  PS_LOG(INFO, config->log_context_)
      << "Attempt to load index file: " << config_->IndexBaseFilename;
  {
    // checking that files exists
    std::error_code ec;
    auto isExists = std::filesystem::exists(config_->IndexBaseFilename, ec);
    if (ec || !isExists) {
      PS_LOG(ERROR, config->log_context_)
          << "index file not exists: " << config_->IndexBaseFilename;
      status = SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_INDEX;
      return;
    }
    isExists = std::filesystem::exists(config_->IndexDataFilename, ec);
    if (ec || !isExists) {
      PS_LOG(ERROR, config->log_context_)
          << "index.data file not exists: " << config_->IndexDataFilename;
      status = SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_INDEX;
      return;
    }
  }

  int error_code = 0;
  const size_t num_frozen_pts = diskann::get_graph_num_frozen_points(
      config_->IndexBaseFilename, error_code);
  if (error_code != 0) {
    PS_LOG(ERROR, config->log_context_)
        << "error calling get_graph_num_frozen_points: error_code="
        << error_code;
    status = SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_INDEX;
    return;
  }

  auto diskANNConfig =
      diskann::IndexConfigBuilder()
          // L2 is Euclidean distance
          .with_metric(diskann::Metric::L2)
          // embedding size; equal to  size_in_bytes/sizeof(type)
          .with_dimension(config_->Dimension)
          // no limitation on points number
          .with_max_points(0)
          // in-memory index
          .with_data_load_store_strategy(diskann::DataStoreStrategy::MEMORY)
          // in-memory index
          .with_graph_load_store_strategy(diskann::GraphStoreStrategy::MEMORY)
          // can be 1 byte (signed/unsigned, int8/uint8) or 4 bytes (float)
          .with_data_type(config_->VectorTypeStr)
          // In perfect world we should use "string" or "char[]" here.
          // Unfortunately, it supports only simple types.
          .with_label_type(diskann_type_to_name<uint32_t>())
          .with_tag_type(diskann_type_to_name<uint32_t>())
          // dynamic index - can not be modified after creation
          .is_dynamic_index(false)
          // no internal filtration, all candidates included in selection
          .is_enable_tags(false)
          .is_concurrent_consolidate(false)
          .is_pq_dist_build(false)
          .is_use_opq(false)
          .with_num_pq_chunks(0)
          .with_num_frozen_pts(num_frozen_pts)
          .build(error_code);
  if (error_code != 0) {
    PS_LOG(ERROR, config->log_context_)
        << "error creating index config builder: error_code=" << error_code;
    status = SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_INDEX;
    return;
  }

  auto index_factory = diskann::IndexFactory(diskANNConfig, error_code);
  if (error_code != 0) {
    PS_LOG(ERROR, config->log_context_)
        << "error creating index factory: error_code=" << error_code;
    status = SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_INDEX;
    return;
  }

  index_ptr_ = index_factory.create_instance();
  if (error_code != 0) {
    PS_LOG(ERROR, config->log_context_)
        << "error creating index instance: error_code=" << error_code;
    status = SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_INDEX;
    return;
  }

  index_ptr_->load(config_->IndexBaseFilename.c_str(), /*loading_threads*/ 4,
                   config_->QueryNeighborsCount, error_code);
  if (error_code != 0) {
    PS_LOG(ERROR, config->log_context_)
        << "error loading index: error_code=" << error_code;
    status = SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_INDEX;
    return;
  }
  PS_LOG(INFO, config->log_context_)
      << "Successfully loaded index file: " << config_->IndexBaseFilename;
}

bool ANNSnapshotIndex::Search(
    const absl::flat_hash_set<std::string_view>& key_set,
    std::vector<std::vector<uint32_t>>& results,
    std::vector<std::pair<uint32_t, uint32_t>>& stats) const {
  // vector_type is stable for one index.
  // TODO: Remove typename hack through unused_variable task.ms/56392737
  auto runSearch = [this, &key_set, &results,
                    &stats](auto unused_variable) -> bool {
    auto expectedResultSize = this->config_->TopCount;
    auto fillUntil = this->config_->QueryNeighborsCount;
    size_t i = 0;
    std::string view_key;
    for (auto& key : key_set) {
      // TODO: call searches in parallel task.ms/56392737
      // skipping key with wrong size
      if (key.size() == sizeof(unused_variable) * this->config_->Dimension) {
        view_key = key;
        stats[i] =
            this->index_ptr_->search<decltype(unused_variable), uint32_t>(
                reinterpret_cast<const decltype(unused_variable)*>(
                    view_key.c_str()),
                expectedResultSize, fillUntil, results[i].data());
      }
      ++i;
    }
    return true;
  };
  // TODO: not compare strings every time task.ms/56392737
  if (config_->VectorTypeStr == "uint8") {
    return runSearch((uint8_t)0);
  } else if (config_->VectorTypeStr == "int8") {
    return runSearch((int8_t)0);
  } else if (config_->VectorTypeStr == "float") {
    return runSearch((float)0);
  } else {
    // There is no 4th option - it's checked before while snapshot processing
    return false;
  }
}

}  // namespace microsoft
}  // namespace kv_server
