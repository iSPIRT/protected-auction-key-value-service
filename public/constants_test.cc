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

#include "public/constants.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace kv_server {
namespace {

TEST(IsDeltaFilename, IsDeltaFilename) {
  EXPECT_EQ(DeltaFileFormatRegex(), R"(DELTA_\d{16})");
}

TEST(FilePrefix, Delta) { EXPECT_EQ(FilePrefix<FileType::DELTA>(), "DELTA"); }

TEST(FilePrefix, Snapshot) {
  EXPECT_EQ(FilePrefix<FileType::SNAPSHOT>(), "SNAPSHOT");
}

TEST(FilePrefix, LogicalShardingConfig) {
  EXPECT_EQ(FilePrefix<FileType::LOGICAL_SHARDING_CONFIG>(),
            "LOGICAL_SHARDING_CONFIG");
}

#if defined(MICROSOFT_AD_SELECTION_BUILD)
TEST(FilePrefix, MICROSOFT_AnnSnapshot) {
  EXPECT_EQ(FilePrefix<FileType::ANNSNAPSHOT>(), "ANNSNAPSHOT");
}
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)

}  // namespace
}  // namespace kv_server
