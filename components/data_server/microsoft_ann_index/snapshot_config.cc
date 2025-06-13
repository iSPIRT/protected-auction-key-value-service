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

#include "components/data_server/microsoft_ann_index/snapshot_config.h"

#include <filesystem>

namespace kv_server {
namespace microsoft {

ANNSnapshotConfig::~ANNSnapshotConfig() {
  // snapshot config is the first variable in ANNSnapshot class
  // because of that, when this destructor called, other objects are already
  // destroyed and we can safely remove snapshot folder
  //
  // also. there is only one config created for one snapshot
  // we are using shared_ptr everywhere
  std::filesystem::path snapshot_folder{SnapshotFolder};
  std::filesystem::remove_all(snapshot_folder);
}

}  // namespace microsoft
}  // namespace kv_server
