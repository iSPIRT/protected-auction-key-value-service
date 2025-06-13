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

#ifndef COMPONENTS_DATA_SERVER_MICROSOFT_ANN_INDEX_ANN_SNAPSHOT_H_
#define COMPONENTS_DATA_SERVER_MICROSOFT_ANN_INDEX_ANN_SNAPSHOT_H_

#include <memory>
#include <string>

#include "components/data_server/microsoft_ann_index/snapshot_config.h"
#include "components/data_server/microsoft_ann_index/snapshot_index.h"
#include "components/data_server/microsoft_ann_index/snapshot_mapping.h"
#include "src/logger/request_context_logger.h"

namespace kv_server {
namespace microsoft {

struct ANNSnapshot {
  ANNSnapshot() = delete;
  ANNSnapshot(const ANNSnapshot&) = delete;
  ANNSnapshot(ANNSnapshot&&) = delete;
  ANNSnapshot& operator=(const ANNSnapshot&) = delete;
  ANNSnapshot& operator=(ANNSnapshot&&) = delete;

  ANNSnapshot(const std::shared_ptr<ANNSnapshotConfig> config,
              SNAPSHOT_STATUS& status);
  std::string& GetSnapshotName() const;

  // config should be the first one - is is mandatory for correct destruction
  const std::shared_ptr<ANNSnapshotConfig> Config;
  ANNSnapshotMapping Mapping;
  ANNSnapshotIndex Index;
};

}  // namespace microsoft
}  // namespace kv_server

#endif  // COMPONENTS_DATA_SERVER_MICROSOFT_ANN_INDEX_ANN_SNAPSHOT_H_
