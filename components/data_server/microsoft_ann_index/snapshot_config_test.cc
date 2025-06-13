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
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace kv_server {
namespace microsoft {
namespace {

class SnapshotConfig : public testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}

  privacy_sandbox::server_common::log::NoOpContext log_context_;
};

TEST_F(SnapshotConfig, EmptyTest) {
  std::error_code ec;
  EXPECT_FALSE(std::filesystem::exists("", ec));
  {
    auto config = std::make_shared<ANNSnapshotConfig>(log_context_);
    EXPECT_EQ(config->SnapshotFolder, "");
  }
  EXPECT_FALSE(std::filesystem::exists("", ec));
  EXPECT_TRUE(std::filesystem::exists(".", ec));
}

TEST_F(SnapshotConfig, RemoveEmptyDirTest) {
  std::error_code ec;
  std::string folder = "tst_dir";
  {
    auto config = std::make_shared<ANNSnapshotConfig>(log_context_);
    EXPECT_EQ(config->SnapshotFolder, "");
    config->SnapshotFolder = folder;
    std::filesystem::create_directory(config->SnapshotFolder);
    EXPECT_TRUE(std::filesystem::exists(folder, ec));
  }
  EXPECT_FALSE(std::filesystem::exists(folder, ec));
  EXPECT_TRUE(std::filesystem::exists(".", ec));
}

TEST_F(SnapshotConfig, RemoveDirWithFilesTest) {
  std::error_code ec;
  std::string folder = "tst_dir";
  {
    auto config = std::make_shared<ANNSnapshotConfig>(log_context_);
    EXPECT_EQ(config->SnapshotFolder, "");
    config->SnapshotFolder = folder;
    std::filesystem::create_directory(config->SnapshotFolder);
    EXPECT_TRUE(std::filesystem::exists(folder, ec));
    std::ofstream file1(folder + "/file1.txt");
    file1 << "test1\n";
    file1.close();
    std::ofstream file2(folder + "/file2.txt");
    file2 << "test1\n";
    file2.close();
    std::ofstream file3(folder + "/file3.txt");
    file3 << "test1\n";
    file3.close();
    EXPECT_TRUE(std::filesystem::exists(folder + "/file1.txt", ec));
    EXPECT_TRUE(std::filesystem::exists(folder + "/file2.txt", ec));
    EXPECT_TRUE(std::filesystem::exists(folder + "/file3.txt", ec));
  }
  EXPECT_FALSE(std::filesystem::exists(folder, ec));
  EXPECT_FALSE(std::filesystem::exists(folder + "/file1.txt", ec));
  EXPECT_FALSE(std::filesystem::exists(folder + "/file2.txt", ec));
  EXPECT_FALSE(std::filesystem::exists(folder + "/file3.txt", ec));
  EXPECT_TRUE(std::filesystem::exists(".", ec));
}

TEST_F(SnapshotConfig, RemoveNotExistFolderTest) {
  std::error_code ec;
  std::string folder = "tst_dir";
  {
    auto config = std::make_shared<ANNSnapshotConfig>(log_context_);
    EXPECT_EQ(config->SnapshotFolder, "");
    config->SnapshotFolder = folder;
    EXPECT_FALSE(std::filesystem::exists(folder, ec));
  }
  EXPECT_FALSE(std::filesystem::exists(folder, ec));
  EXPECT_TRUE(std::filesystem::exists(".", ec));
}

}  // namespace
}  // namespace microsoft
}  // namespace kv_server
