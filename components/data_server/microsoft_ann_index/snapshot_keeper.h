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

#ifndef COMPONENTS_DATA_SERVER_MICROSOFT_ANN_INDEX_ANN_SNAPSHOT_KEEPER_H_
#define COMPONENTS_DATA_SERVER_MICROSOFT_ANN_INDEX_ANN_SNAPSHOT_KEEPER_H_

#include <deque>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>

#include "components/data_server/microsoft_ann_index/snapshot.h"

namespace kv_server {
namespace microsoft {

const char kAnnSnapshotDefaultFolderPath[] = "ANNSNAPSHOTS_TECHNICAL_FOLDER";

class ANNSnapshotKeeper {
 public:
  ANNSnapshotKeeper();
  explicit ANNSnapshotKeeper(const std::string& str);

  static SNAPSHOT_STATUS TryCreateFolder(
      const std::filesystem::path& folderPath);
  static SNAPSHOT_STATUS CheckSnapshotIsValid(
      const std::shared_ptr<ANNSnapshotConfig>& snapshotConfig);
  static SNAPSHOT_STATUS HandleIncomingSnapshot(
      const std::string& snapshotFolderPath,
      const std::string_view& snapshotName, const std::string_view& fpath,
      std::shared_ptr<ANNSnapshotConfig>& snapshotConfig);

  SNAPSHOT_STATUS TryAddANNSnapshot(
      const std::string_view& snapshotName, const std::string_view& fpath,
      privacy_sandbox::server_common::log::PSLogContext& log_context);
  std::shared_ptr<ANNSnapshot> GetActualANNSnapshot() const;
  bool CheckNewSnapshotIsFresh(const std::string_view& snapshotName) const;
  bool HasANNSnapshots() const;
  void TryRemoveUnusedANNSnapshots();
  size_t DequeCapacity() const;

 private:
  std::string snapshots_folder_ = kAnnSnapshotDefaultFolderPath;
  std::deque<std::shared_ptr<ANNSnapshot>> ann_snapshots_;
  std::shared_ptr<ANNSnapshot> actual_ann_snapshot_;
};

}  // namespace microsoft
}  // namespace kv_server

#endif  // COMPONENTS_DATA_SERVER_MICROSOFT_ANN_INDEX_ANN_SNAPSHOT_KEEPER_H_
