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

#include "components/data_server/microsoft_ann_index/snapshot.h"

namespace kv_server {
namespace microsoft {

ANNSnapshot::ANNSnapshot(const std::shared_ptr<ANNSnapshotConfig> config,
                         SNAPSHOT_STATUS& status)
    : Config(config),
      Mapping(Config->MappingFilename, status, config->log_context_),
      Index(Config, status) {
  if (status == SNAPSHOT_STATUS::IN_PROGRESS) {
    status = SNAPSHOT_STATUS::OK;
  }
}

std::string& ANNSnapshot::GetSnapshotName() const {
  return Config->SnapshotName;
}

}  // namespace microsoft
}  // namespace kv_server
