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

#include "components/data_server/microsoft_ann_index/snapshot_keeper.h"

#include <cstdint>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

#include "nlohmann/json.hpp"

namespace kv_server {
namespace microsoft {

namespace {
// DiskANN expects to have several files with this prefix
constexpr std::string_view kAnnSnapshotIndexFilenamePath = "index";
// "index.data" is a file that DiskANN library expects to be same folder with
// the "index" file.
constexpr std::string_view kAnnSnapshotIndexDataFilenamePath = "index.data";
constexpr std::string_view kAnnSnapshotMappingFilenamePath = "mapping";
constexpr std::string_view kAnnSnapshotConfigJsonFilenamePath = "config.json";

// snapshot numeric restrictions
constexpr uint32_t kSnapshotMagicBytes = 0xF00DFEED;
constexpr uint32_t kMaxDimension = 100000;
constexpr uint32_t kMaxSearchDepth = 1000000000;
constexpr uint32_t kMinFileSize = 10;
constexpr uint64_t kMaxFileSize = 1000000000000;
constexpr uint32_t kMinFilesCount = 4;
constexpr uint32_t kMaxFilesCount = 100000000;
constexpr uint32_t kMaxFilenameLength = 10000;

// think about optimization to fit in L2
constexpr size_t kRWBufferSize = 10 * 1024 * 1024;
// multithreading?
char gRWBuffer[kRWBufferSize];

}  // namespace

SNAPSHOT_STATUS ANNSnapshotKeeper::TryCreateFolder(
    const std::filesystem::path& folderPath) {
  std::error_code ec;
  auto isExists = std::filesystem::exists(folderPath, ec);
  if (ec) {
    return SNAPSHOT_STATUS::FILESYSTEM_CREATE_FOLDER_ERROR;
  }
  if (!isExists) {
    auto result = std::filesystem::create_directory(folderPath, ec);
    if (ec) {
      return SNAPSHOT_STATUS::FILESYSTEM_CREATE_FOLDER_ERROR;
    }
    if (result) {
      return SNAPSHOT_STATUS::IN_PROGRESS;
    }
    return SNAPSHOT_STATUS::FILESYSTEM_CREATE_FOLDER_ERROR;
  }
  return SNAPSHOT_STATUS::IN_PROGRESS;
}

SNAPSHOT_STATUS ANNSnapshotKeeper::CheckSnapshotIsValid(
    const std::shared_ptr<ANNSnapshotConfig>& snapshotConfig) {
  std::error_code ec;
  if (snapshotConfig->Dimension == 0 ||
      snapshotConfig->Dimension > kMaxDimension) {
    PS_LOG(ERROR, snapshotConfig->log_context_)
        << "Invalid incoming snapshot - invalid Dimension parameter";
    return SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG;
  }
  if (snapshotConfig->QueryNeighborsCount == 0 ||
      snapshotConfig->QueryNeighborsCount > kMaxSearchDepth) {
    PS_LOG(ERROR, snapshotConfig->log_context_)
        << "Invalid incoming snapshot - invalid QueryNeighborsCount parameter";
    return SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG;
  }
  if (snapshotConfig->TopCount == 0 ||
      snapshotConfig->TopCount > kMaxSearchDepth) {
    PS_LOG(ERROR, snapshotConfig->log_context_)
        << "Invalid incoming snapshot - invalid TopCount parameter";
    return SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG;
  }
  if (snapshotConfig->VectorTypeStr != "int8" &&
      snapshotConfig->VectorTypeStr != "uint8" &&
      snapshotConfig->VectorTypeStr != "float") {
    PS_LOG(ERROR, snapshotConfig->log_context_)
        << "Invalid incoming snapshot - invalid VectorTypeStr parameter";
    return SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG;
  }
  {
    // checking for "index" file
    if (snapshotConfig->IndexBaseFilename == "") {
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "Invalid incoming snapshot - no index file inside!";
      return SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG;
    }
    auto isExists =
        std::filesystem::exists(snapshotConfig->IndexBaseFilename, ec);
    if (ec || !isExists) {
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "Invalid incoming snapshot - invalid index file inside!";
      return SNAPSHOT_STATUS::INVALID_SNAPSHOT_INDEX;
    }
    auto fileSize =
        std::filesystem::file_size(snapshotConfig->IndexBaseFilename, ec);
    if (ec || fileSize < kMinFileSize || fileSize > kMaxFileSize) {
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "Invalid incoming snapshot - invalid index file size!";
      return SNAPSHOT_STATUS::INVALID_SNAPSHOT_INDEX;
    }
  }
  {
    // checking for "index.data" file
    if (snapshotConfig->IndexDataFilename == "") {
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "Invalid incoming snapshot - invalid index.data file size!";
      return SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG;
    }
    auto isExists =
        std::filesystem::exists(snapshotConfig->IndexDataFilename, ec);
    if (ec || !isExists) {
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "Invalid incoming snapshot - invalid index.data file inside!";
      return SNAPSHOT_STATUS::INVALID_SNAPSHOT_INDEX_DATA;
    }
    auto fileSize =
        std::filesystem::file_size(snapshotConfig->IndexDataFilename, ec);
    if (ec || fileSize < kMinFileSize || fileSize > kMaxFileSize) {
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "Invalid incoming snapshot - invalid index.data file size!";
      return SNAPSHOT_STATUS::INVALID_SNAPSHOT_INDEX_DATA;
    }
  }
  {
    // checking for "mapping" file
    if (snapshotConfig->MappingFilename == "") {
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "Invalid incoming snapshot - invalid mapping file size!";
      return SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG;
    }
    auto isExists =
        std::filesystem::exists(snapshotConfig->MappingFilename, ec);
    if (ec || !isExists) {
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "Invalid incoming snapshot - invalid mapping file inside!";
      return SNAPSHOT_STATUS::INVALID_SNAPSHOT_MAPPING;
    }
    auto fileSize =
        std::filesystem::file_size(snapshotConfig->MappingFilename, ec);
    if (ec || fileSize < kMinFileSize || fileSize > kMaxFileSize) {
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "Invalid incoming snapshot - invalid mapping file size!";
      return SNAPSHOT_STATUS::INVALID_SNAPSHOT_MAPPING;
    }
  }
  return SNAPSHOT_STATUS::IN_PROGRESS;
}

SNAPSHOT_STATUS ANNSnapshotKeeper::HandleIncomingSnapshot(
    const std::string& snapshotFolderPath, const std::string_view& snapshotName,
    const std::string_view& fpath,
    std::shared_ptr<ANNSnapshotConfig>& snapshotConfig) {
  auto progressStatus = SNAPSHOT_STATUS::IN_PROGRESS;
  snapshotConfig->SnapshotName = snapshotName;

  const std::filesystem::path annsnapshotsFolder{snapshotFolderPath};
  progressStatus = TryCreateFolder(annsnapshotsFolder);
  if (progressStatus != SNAPSHOT_STATUS::IN_PROGRESS) {
    PS_LOG(ERROR, snapshotConfig->log_context_)
        << "Can't create snapshot folder: " << snapshotFolderPath
        << ", status is " << (size_t)progressStatus;
    return progressStatus;
  }
  const std::filesystem::path destinationFolderPath{annsnapshotsFolder /
                                                    snapshotName};
  progressStatus = TryCreateFolder(destinationFolderPath);
  if (progressStatus != SNAPSHOT_STATUS::IN_PROGRESS) {
    PS_LOG(ERROR, snapshotConfig->log_context_)
        << "Can't create folder for snapshot: " << snapshotName
        << ", status is " << (size_t)progressStatus;
    return progressStatus;
  }
  snapshotConfig->SnapshotFolder = destinationFolderPath.string();
  const std::filesystem::path incomingSnapshotPath{fpath};
  std::ifstream fpInput{incomingSnapshotPath, std::ios::binary};
  if (!fpInput) {
    PS_LOG(ERROR, snapshotConfig->log_context_)
        << "Cant open snapshot using " << fpath << " path";
    return SNAPSHOT_STATUS::IO_CANT_OPEN_INCOMING_SNAPSHOT_FILE;
  }

  {
    uint32_t magic_bytes;
    fpInput.read(reinterpret_cast<char*>(&magic_bytes), 4);
    if (!fpInput.good()) {
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "Cant read magic bytes from " << fpath << " path";
      return SNAPSHOT_STATUS::IFSTREAM_FAILURE;
    }
    if (magic_bytes != kSnapshotMagicBytes) {
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "Snapshot magic bytes does not match!";
      return SNAPSHOT_STATUS::INVALID_SNAPSHOT;
    }

    uint32_t filesCount;
    uint32_t filenameLen;
    uint64_t fileSize;
    std::vector<std::string> files;
    fpInput.read(reinterpret_cast<char*>(&filesCount), 4);
    if (!fpInput.good()) {
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "Can't read file count from snapshot!";
      return SNAPSHOT_STATUS::IFSTREAM_FAILURE;
    }
    if (filesCount < kMinFilesCount || filesCount > kMaxFilesCount) {
      // there should be at least four files - index, index.data, mapping and
      // config.json. Technically, there can be more files in the future, but
      // there is no way that can be more than 100M files.
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "Incorrect file count in snapshot: " << filesCount;
      return SNAPSHOT_STATUS::INVALID_SNAPSHOT;
    }
    files.resize(filesCount, {});
    for (uint32_t i = 0; i < filesCount; ++i) {
      // expected file has specific format.
      // TODO: task.ms/56392785 add link to the document with format
      // description.
      //
      // each expected file is a container which contains several files
      // inside. each files are written one after another, with the following
      // format:
      // 1. uint32_t - length of the filename
      // 2. char[] - filename
      // 3. uint64_t - size of the file
      // 4. char[] - file content
      fpInput.read(reinterpret_cast<char*>(&filenameLen), 4);
      if (!fpInput.good()) {
        PS_LOG(ERROR, snapshotConfig->log_context_)
            << "Can't read filename from snapshot";
        return SNAPSHOT_STATUS::IFSTREAM_FAILURE;
      }
      if (filenameLen == 0 || filenameLen > kMaxFilenameLength) {
        PS_LOG(ERROR, snapshotConfig->log_context_)
            << "Invalid filename in snapshot";
        return SNAPSHOT_STATUS::INVALID_SNAPSHOT;
      }
      files[i].resize(filenameLen, '\0');
      fpInput.read(files[i].data(), filenameLen);
      if (!fpInput.good()) {
        PS_LOG(ERROR, snapshotConfig->log_context_)
            << "Can't read filename from snapshot";
        return SNAPSHOT_STATUS::IFSTREAM_FAILURE;
      }
      if (files[i] == kAnnSnapshotIndexFilenamePath) {
        snapshotConfig->IndexBaseFilename =
            (destinationFolderPath / kAnnSnapshotIndexFilenamePath).string();
      } else if (files[i] == kAnnSnapshotIndexDataFilenamePath) {
        snapshotConfig->IndexDataFilename =
            (destinationFolderPath / kAnnSnapshotIndexDataFilenamePath)
                .string();
      } else if (files[i] == kAnnSnapshotMappingFilenamePath) {
        snapshotConfig->MappingFilename =
            (destinationFolderPath / kAnnSnapshotMappingFilenamePath).string();
      } else if (files[i] == kAnnSnapshotConfigJsonFilenamePath) {
        snapshotConfig->ConfigJsonFilename =
            (destinationFolderPath / kAnnSnapshotConfigJsonFilenamePath)
                .string();
      } else {
        // let us keep unknown files - later we will check only that expected
        // files are here. So we proceed here.
        PS_LOG(WARNING, snapshotConfig->log_context_)
            << "Unexpected file arrived in snapshot " << snapshotName << ": "
            << files[i];
      }
      fpInput.read(reinterpret_cast<char*>(&fileSize), 8);
      if (!fpInput.good()) {
        PS_LOG(ERROR, snapshotConfig->log_context_)
            << "Can't read file size from snapshot";
        return SNAPSHOT_STATUS::IFSTREAM_FAILURE;
      }
      if (fileSize == 0 || fileSize > kMaxFileSize) {
        // more than 1TB is definitely not expected
        PS_LOG(ERROR, snapshotConfig->log_context_)
            << "Invalid file size from snapshot";
        return SNAPSHOT_STATUS::INVALID_SNAPSHOT;
      }
      std::ofstream fpOutput{destinationFolderPath / files[i],
                             std::ios::binary};
      if (!fpOutput || !fpOutput.good()) {
        PS_LOG(ERROR, snapshotConfig->log_context_)
            << "Can't write file from snapshot to disk";
        return SNAPSHOT_STATUS::OFSTREAM_FAILURE;
      }
      while (fileSize > kRWBufferSize) {
        fpInput.read(gRWBuffer, kRWBufferSize);
        if (!fpInput.good()) {
          PS_LOG(ERROR, snapshotConfig->log_context_)
              << "Can't read file from snapshot";
          return SNAPSHOT_STATUS::IFSTREAM_FAILURE;
        }
        fpOutput.write(gRWBuffer, kRWBufferSize);
        if (!fpOutput.good()) {
          PS_LOG(ERROR, snapshotConfig->log_context_)
              << "Can't write file from snapshot to disk";
          return SNAPSHOT_STATUS::OFSTREAM_FAILURE;
        }
        fileSize -= kRWBufferSize;
      }
      if (fileSize > 0) {
        fpInput.read(gRWBuffer, fileSize);
        if (!fpInput.good()) {
          PS_LOG(ERROR, snapshotConfig->log_context_)
              << "Can't read file from snapshot";
          return SNAPSHOT_STATUS::IFSTREAM_FAILURE;
        }
        fpOutput.write(gRWBuffer, fileSize);
        if (!fpOutput.good()) {
          PS_LOG(ERROR, snapshotConfig->log_context_)
              << "Can't write file from snapshot to disk";
          return SNAPSHOT_STATUS::OFSTREAM_FAILURE;
        }
      }
      fpOutput.close();
    }
    {
      // force trigger EOF
      fpInput.read(reinterpret_cast<char*>(&fileSize), 1);

      if (!fpInput.eof()) {
        PS_LOG(ERROR, snapshotConfig->log_context_)
            << "Invalid snapshot - snapshot is expected to end, but more bytes "
               "found";
        return SNAPSHOT_STATUS::IFSTREAM_FAILURE;
      }
    }
    fpInput.close();
  }

  {
    // parsing config.json
    std::ifstream fpConfig{snapshotConfig->ConfigJsonFilename,
                           std::ios::binary};
    if (!fpConfig) {
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "Can't find config.json - it should be part of snapshot";
      return SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG;
    }
    nlohmann::json jconfig = nlohmann::json::parse(fpConfig, nullptr,
                                                   /*allow_exceptions=*/false,
                                                   /*ignore_comments=*/true);
    if (jconfig.is_null()) {
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "Invalid json in config.json";
      return SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG;
    }
    if (jconfig.is_discarded()) {
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "Invalid json in config.json";
      return SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG;
    }
    if (jconfig.contains("Dimension") &&
        jconfig["Dimension"].is_number_unsigned()) {
      snapshotConfig->Dimension = jconfig["Dimension"].get<uint32_t>();
    } else {
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "No Dimension parameter in config.json or Dimension parameter is "
             "wrong type";
      return SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG;
    }
    if (jconfig.contains("QueryNeighborsCount") &&
        jconfig["QueryNeighborsCount"].is_number_unsigned()) {
      snapshotConfig->QueryNeighborsCount =
          jconfig["QueryNeighborsCount"].get<uint32_t>();
    } else {
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "No QueryNeighborsCount parameter in config.json or "
             "QueryNeighborsCount parameter is "
             "wrong type";
      return SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG;
    }
    if (jconfig.contains("TopCount") &&
        jconfig["TopCount"].is_number_unsigned()) {
      snapshotConfig->TopCount = jconfig["TopCount"].get<uint32_t>();
    } else {
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "No TopCount parameter in config.json or TopCount parameter is "
             "wrong type";
      return SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG;
    }
    if (jconfig.contains("VectorTypeStr") &&
        jconfig["VectorTypeStr"].is_string()) {
      snapshotConfig->VectorTypeStr =
          jconfig["VectorTypeStr"].get<std::string>();
    } else {
      PS_LOG(ERROR, snapshotConfig->log_context_)
          << "No VectorTypeStr parameter in config.json or VectorTypeStr "
             "parameter is wrong type";
      return SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG;
    }
  }
  return CheckSnapshotIsValid(snapshotConfig);
}

ANNSnapshotKeeper::ANNSnapshotKeeper() {
  // removing all old snapshots on start to not create trash
  std::filesystem::path snapshot_folder{snapshots_folder_};
  std::filesystem::remove_all(snapshot_folder);
}

ANNSnapshotKeeper::ANNSnapshotKeeper(const std::string& str)
    : snapshots_folder_(str) {
  // removing all old snapshots on start to not create trash
  std::filesystem::path snapshot_folder{snapshots_folder_};
  std::filesystem::remove_all(snapshot_folder);
}

bool ANNSnapshotKeeper::CheckNewSnapshotIsFresh(
    const std::string_view& snapshotName) const {
  if (ann_snapshots_.empty()) {
    // if there are no snapshots, then the new one is always fresh
    return true;
  }
  return snapshotName > actual_ann_snapshot_->GetSnapshotName();
}

SNAPSHOT_STATUS ANNSnapshotKeeper::TryAddANNSnapshot(
    const std::string_view& snapshotName, const std::string_view& fpath,
    privacy_sandbox::server_common::log::PSLogContext& log_context) {
  if (!CheckNewSnapshotIsFresh(snapshotName)) {
    PS_LOG(INFO, log_context)
        << "Snapshot " << snapshotName << " from " << fpath
        << " skipped - snapshot is not fresh comparing to the current one";
    return SNAPSHOT_STATUS::NOT_FRESH;
  }
  std::shared_ptr<ANNSnapshotConfig> config =
      std::make_shared<ANNSnapshotConfig>(log_context);
  auto status =
      HandleIncomingSnapshot(snapshots_folder_, snapshotName, fpath, config);
  if (status == SNAPSHOT_STATUS::IN_PROGRESS) {
    auto smart_ptr = std::make_shared<ANNSnapshot>(config, status);
    if (status != SNAPSHOT_STATUS::OK) {
      PS_LOG(ERROR, log_context)
          << "Snapshot " << snapshotName << " from " << fpath
          << " skipped: status error " << (size_t)status;
      return status;
    } else {
      PS_LOG(INFO, log_context) << "Snapshot " << snapshotName << " from "
                                << fpath << " loaded succesfully";
      ann_snapshots_.push_back(smart_ptr);
      actual_ann_snapshot_ = ann_snapshots_.back();
    }
  }
  return status;
}

std::shared_ptr<ANNSnapshot> ANNSnapshotKeeper::GetActualANNSnapshot() const {
  return actual_ann_snapshot_;
}

bool ANNSnapshotKeeper::HasANNSnapshots() const {
  return !ann_snapshots_.empty() && actual_ann_snapshot_;
}

void ANNSnapshotKeeper::TryRemoveUnusedANNSnapshots() {
  if (ann_snapshots_.empty()) {
    return;
  }
  PS_LOG(INFO, GetActualANNSnapshot()->Config->log_context_)
      << "Trying to remove unused snapshot, current capacity is "
      << DequeCapacity();
  while (!ann_snapshots_.empty()) {
    // this method is considered to be called once in a few seconds
    // the logic behind this is simple. In most cases, we will have only one
    // snapshot in the deque - the current one. Sometimes we will have more
    // than one snapshot in the deque - that happens after we add a new
    // snapshot. In this case, all new selections will happen with the new
    // snapshot, but there is possibility that there are still some selections
    // which are using the old one. Because of this, we can't just remove the
    // old snapshot, but we can track that it is not used anymore (because if
    // this snapshot is used, shared_ptr will not be unique in the deque). So,
    // we are trying to remove snapshots from the front of the deque, because
    // they are the oldest ones.
    if (!ann_snapshots_.front().unique()) {
      // last snapshot is never unique - we have it in actual_ann_snapshot_
      PS_LOG(INFO, GetActualANNSnapshot()->Config->log_context_)
          << "Removed all unused snapshots, current capacity is "
          << DequeCapacity();
      return;
    }
    // if unique, the only place where  we have it is in the deque.
    ann_snapshots_.pop_front();
    PS_LOG(INFO, GetActualANNSnapshot()->Config->log_context_)
        << "Successfully removed unused snapshot";
  }
}

size_t ANNSnapshotKeeper::DequeCapacity() const {
  return ann_snapshots_.size();
}
}  // namespace microsoft
}  // namespace kv_server
