// Copyright 2022 Google LLC
// Copyright (C) Microsoft Corporation. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "public/data_loading/filename_utils.h"

#include <cctype>
#include <regex>  // NOLINT

#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "public/constants.h"

namespace kv_server {
namespace {
template <FileType::Enum file_type>
std::string GetFilename(int64_t logical_commit_time) {
  return absl::StrFormat("%s%s%0*d", FilePrefix<file_type>(),
                         kFileComponentDelimiter, kLogicalTimeDigits,
                         logical_commit_time);
}
}  // namespace

// Right now we use simple logic to validate, as the format is simple and we
// hope this is faster than doing a regex match.
bool IsDeltaFilename(std::string_view basename) {
  static const std::regex re(DeltaFileFormatRegex().data(),
                             DeltaFileFormatRegex().size());
  return std::regex_match(basename.begin(), basename.end(), re);
}

absl::StatusOr<std::string> ToDeltaFileName(uint64_t logical_commit_time) {
  const std::string result = GetFilename<FileType::DELTA>(logical_commit_time);
  if (!IsDeltaFilename(result)) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Unable to build delta file name with logical commit time: ",
        logical_commit_time, " which makes a file name: ", result));
  }
  return result;
}

bool IsSnapshotFilename(std::string_view basename) {
  return std::regex_match(basename.begin(), basename.end(),
                          SnapshotFileFormatRegex());
}

#if defined(MICROSOFT_AD_SELECTION_BUILD)
bool MicrosoftIsAnnSnapshotFilename(std::string_view basename) {
  return std::regex_match(basename.begin(), basename.end(),
                          MicrosoftAnnSnapshotFileFormatRegex());
}
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)

absl::StatusOr<std::string> ToSnapshotFileName(uint64_t logical_commit_time) {
  const std::string result =
      GetFilename<FileType::SNAPSHOT>(logical_commit_time);
  if (!IsSnapshotFilename(result)) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Unable to build a valid snapshot file name with logical commit time: ",
        logical_commit_time, " which makes a file name: ", result));
  }
  return result;
}

bool IsLogicalShardingConfigFilename(std::string_view basename) {
  return std::regex_match(basename.begin(), basename.end(),
                          LogicalShardingConfigFileFormatRegex());
}

absl::StatusOr<std::string> ToLogicalShardingConfigFilename(
    uint64_t logical_commit_time) {
  const std::string result =
      GetFilename<FileType::LOGICAL_SHARDING_CONFIG>(logical_commit_time);
  if (!IsLogicalShardingConfigFilename(result)) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Unable to build a valid logical sharding config file name with "
        "logical commit time: ",
        logical_commit_time, " which makes a file name: ", result));
  }
  return result;
}

bool IsFileGroupFileName(std::string_view filename) {
  return std::regex_match(filename.begin(), filename.end(),
                          FileGroupFilenameFormatRegex());
}

absl::StatusOr<std::string> ToFileGroupFileName(FileType::Enum file_type,
                                                uint64_t logical_commit_time,
                                                uint64_t file_index,
                                                uint64_t file_group_size) {
#if defined(MICROSOFT_AD_SELECTION_BUILD)
  if (file_type != FileType::DELTA && file_type != FileType::SNAPSHOT &&
      file_type != FileType::ANNSNAPSHOT) {
#else
  if (file_type != FileType::DELTA && file_type != FileType::SNAPSHOT) {
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
    return absl::InvalidArgumentError(
        absl::StrCat("File groups are not supported for file type: ",
                     FileType_Enum_Name(file_type)));
  }
  if (file_index >= file_group_size) {
    return absl::InvalidArgumentError(
        absl::StrCat("file index: ", file_index,
                     " must be less than file group size: ", file_group_size));
  }
  return absl::StrFormat(
      "%s%s%0*d%s%0*d%sOF%s%0*d", FileType::Enum_Name(file_type),
      kFileComponentDelimiter, kLogicalTimeDigits, logical_commit_time,
      kFileComponentDelimiter, kFileGroupFileIndexDigits, file_index,
      kFileComponentDelimiter, kFileComponentDelimiter, kFileGroupSizeDigits,
      file_group_size);
}

}  // namespace kv_server
