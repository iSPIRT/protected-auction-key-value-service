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

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace kv_server {
namespace microsoft {
namespace {

void CopyConfigFields(const std::shared_ptr<ANNSnapshotConfig>& src,
                      std::shared_ptr<ANNSnapshotConfig>& dst) {
  dst->IndexBaseFilename = src->IndexBaseFilename;
  dst->SnapshotName = src->SnapshotName;
  dst->Dimension = src->Dimension;
  dst->QueryNeighborsCount = src->QueryNeighborsCount;
  dst->TopCount = src->TopCount;
  dst->VectorTypeStr = src->VectorTypeStr;
  dst->SnapshotFolder = src->SnapshotFolder;
  dst->IndexBaseFilename = src->IndexBaseFilename;
  dst->IndexDataFilename = src->IndexDataFilename;
  dst->MappingFilename = src->MappingFilename;
  dst->ConfigJsonFilename = src->ConfigJsonFilename;
}

class SnapshotKeeperTest : public testing::Test {
 protected:
  void SetUp() override {
    unique_test_folder_ = "Test_Folder_" + rand_string();
    std::filesystem::create_directory(unique_test_folder_);
    unique_snapshot_path_ = unique_test_folder_ + "/" + unique_snapshot_name_;
    snapshot_folder_ =
        unique_test_folder_ + "/" + kAnnSnapshotDefaultFolderPath;
  }

  void DumpPreparedSnapshot() {
    DumpFile(unique_snapshot_path_, snapshot_bytes_);
  }

  void TearDown() override { std::filesystem::remove_all(unique_test_folder_); }

  std::string rand_string(const int len = 10) {
    static const char alphanum[] = "0123456789ABCDEF";
    std::string tmp_s;
    tmp_s.reserve(len);
    for (int i = 0; i < len; ++i) {
      tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    return tmp_s;
  }

  void DumpFile(const std::string& filename,
                const std::vector<unsigned char>& bytes) {
    std::ofstream file(filename, std::ios::binary);
    file.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    file.close();
  }

  std::string unique_test_folder_;
  std::string unique_snapshot_name_ = "ANNSNAPSHOT_00000000000000001";
  std::string unique_snapshot_path_;
  std::string snapshot_folder_;
  std::vector<unsigned char> snapshot_bytes_ = {
      237, 254, 13,  240, 4,   0,   0,   0,   5,   0,   0,   0,   105, 110, 100,
      101, 120, 224, 0,   0,   0,   0,   0,   0,   0,   224, 0,   0,   0,   0,
      0,   0,   0,   6,   0,   0,   0,   9,   0,   0,   0,   0,   0,   0,   0,
      0,   0,   0,   0,   4,   0,   0,   0,   9,   0,   0,   0,   4,   0,   0,
      0,   5,   0,   0,   0,   8,   0,   0,   0,   5,   0,   0,   0,   9,   0,
      0,   0,   2,   0,   0,   0,   4,   0,   0,   0,   6,   0,   0,   0,   8,
      0,   0,   0,   3,   0,   0,   0,   1,   0,   0,   0,   6,   0,   0,   0,
      8,   0,   0,   0,   3,   0,   0,   0,   9,   0,   0,   0,   4,   0,   0,
      0,   7,   0,   0,   0,   5,   0,   0,   0,   3,   0,   0,   0,   1,   0,
      0,   0,   0,   0,   0,   0,   7,   0,   0,   0,   8,   0,   0,   0,   2,
      0,   0,   0,   9,   0,   0,   0,   0,   0,   0,   0,   3,   0,   0,   0,
      9,   0,   0,   0,   2,   0,   0,   0,   1,   0,   0,   0,   4,   0,   0,
      0,   4,   0,   0,   0,   3,   0,   0,   0,   9,   0,   0,   0,   8,   0,
      0,   0,   6,   0,   0,   0,   9,   0,   0,   0,   0,   0,   0,   0,   2,
      0,   0,   0,   4,   0,   0,   0,   1,   0,   0,   0,   7,   0,   0,   0,
      5,   0,   0,   0,   5,   0,   0,   0,   8,   0,   0,   0,   6,   0,   0,
      0,   1,   0,   0,   0,   7,   0,   0,   0,   10,  0,   0,   0,   105, 110,
      100, 101, 120, 46,  100, 97,  116, 97,  48,  0,   0,   0,   0,   0,   0,
      0,   10,  0,   0,   0,   4,   0,   0,   0,   45,  234, 191, 190, 203, 89,
      104, 228, 191, 23,  170, 182, 69,  148, 8,   50,  6,   69,  15,  195, 104,
      235, 159, 146, 251, 180, 204, 137, 18,  211, 13,  194, 87,  169, 177, 190,
      126, 205, 135, 152, 7,   0,   0,   0,   109, 97,  112, 112, 105, 110, 103,
      174, 0,   0,   0,   0,   0,   0,   0,   10,  0,   0,   0,   13,  0,   0,
      0,   98,  97,  110, 110, 101, 114, 95,  54,  53,  50,  50,  52,  56,  13,
      0,   0,   0,   98,  97,  110, 110, 101, 114, 95,  54,  50,  57,  51,  57,
      52,  13,  0,   0,   0,   98,  97,  110, 110, 101, 114, 95,  52,  48,  50,
      53,  56,  57,  13,  0,   0,   0,   98,  97,  110, 110, 101, 114, 95,  51,
      49,  52,  48,  52,  52,  13,  0,   0,   0,   98,  97,  110, 110, 101, 114,
      95,  50,  53,  49,  53,  57,  55,  13,  0,   0,   0,   98,  97,  110, 110,
      101, 114, 95,  56,  51,  57,  53,  54,  48,  13,  0,   0,   0,   98,  97,
      110, 110, 101, 114, 95,  50,  49,  53,  55,  50,  57,  13,  0,   0,   0,
      98,  97,  110, 110, 101, 114, 95,  55,  48,  55,  56,  50,  52,  13,  0,
      0,   0,   98,  97,  110, 110, 101, 114, 95,  52,  56,  48,  55,  56,  49,
      13,  0,   0,   0,   98,  97,  110, 110, 101, 114, 95,  51,  56,  55,  55,
      55,  49,  11,  0,   0,   0,   99,  111, 110, 102, 105, 103, 46,  106, 115,
      111, 110, 83,  0,   0,   0,   0,   0,   0,   0,   123, 34,  68,  105, 109,
      101, 110, 115, 105, 111, 110, 34,  58,  32,  52,  44,  32,  34,  81,  117,
      101, 114, 121, 78,  101, 105, 103, 104, 98,  111, 114, 115, 67,  111, 117,
      110, 116, 34,  58,  32,  56,  44,  32,  34,  84,  111, 112, 67,  111, 117,
      110, 116, 34,  58,  32,  52,  44,  32,  34,  86,  101, 99,  116, 111, 114,
      84,  121, 112, 101, 83,  116, 114, 34,  58,  32,  34,  117, 105, 110, 116,
      56,  34,  125,
  };
  privacy_sandbox::server_common::log::NoOpContext log_context_;
};

TEST_F(SnapshotKeeperTest, EmptyTest) {
  ANNSnapshotKeeper keeper(snapshot_folder_);
  EXPECT_FALSE(keeper.HasANNSnapshots());
  EXPECT_FALSE(keeper.GetActualANNSnapshot());
  EXPECT_EQ(keeper.DequeCapacity(), 0);
  // no snapshots, should work without failing
  keeper.TryRemoveUnusedANNSnapshots();
  // should return true for any snapshot name
  EXPECT_TRUE(keeper.CheckNewSnapshotIsFresh(""));
  EXPECT_TRUE(keeper.CheckNewSnapshotIsFresh("ANNSNAPSHOT_00000000000000001"));
  EXPECT_EQ(keeper.DequeCapacity(), 0);
}

TEST_F(SnapshotKeeperTest, TryCreateFolderFunctionTest) {
  std::error_code ec;
  std::string folder1 = unique_test_folder_ + "/" + "folder1";
  std::string folder2 = unique_test_folder_ + "/" + "folder2";
  EXPECT_FALSE(std::filesystem::exists(folder1, ec));
  EXPECT_FALSE(ec);
  EXPECT_FALSE(std::filesystem::exists(folder2, ec));
  EXPECT_FALSE(ec);
  // creating folders
  auto status = ANNSnapshotKeeper::TryCreateFolder(folder1);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);
  status = ANNSnapshotKeeper::TryCreateFolder(folder2);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);
  EXPECT_TRUE(std::filesystem::exists(folder1, ec));
  EXPECT_FALSE(ec);
  EXPECT_TRUE(std::filesystem::exists(folder2, ec));
  EXPECT_FALSE(ec);
  // try to create existed
  status = ANNSnapshotKeeper::TryCreateFolder(folder1);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);
  EXPECT_TRUE(std::filesystem::exists(folder1, ec));
  EXPECT_FALSE(ec);
  // error creating folder
  std::string errFolder = unique_test_folder_ + "/1/1";
  status = ANNSnapshotKeeper::TryCreateFolder(errFolder);
  EXPECT_EQ(status, SNAPSHOT_STATUS::FILESYSTEM_CREATE_FOLDER_ERROR);
  EXPECT_FALSE(std::filesystem::exists(errFolder, ec));
  EXPECT_FALSE(ec);
  // error broken path
  std::string brokenFolder = "\0";
  status = ANNSnapshotKeeper::TryCreateFolder(brokenFolder);
  EXPECT_EQ(status, SNAPSHOT_STATUS::FILESYSTEM_CREATE_FOLDER_ERROR);
  EXPECT_FALSE(std::filesystem::exists(brokenFolder, ec));
  EXPECT_FALSE(ec);
}

TEST_F(SnapshotKeeperTest, CheckSnapshotIsValidFunctionTest) {
  DumpPreparedSnapshot();
  std::shared_ptr<ANNSnapshotConfig> config =
      std::make_shared<ANNSnapshotConfig>(log_context_);
  auto status = ANNSnapshotKeeper::HandleIncomingSnapshot(
      snapshot_folder_, unique_snapshot_name_, unique_snapshot_path_, config);
  ASSERT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);
  status = ANNSnapshotKeeper::CheckSnapshotIsValid(config);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);

  {
    std::shared_ptr<ANNSnapshotConfig> configCopy =
        std::make_shared<ANNSnapshotConfig>(log_context_);
    CopyConfigFields(config, configCopy);
    configCopy->SnapshotFolder = "1";  // hack
    configCopy->Dimension = 0;
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG);
  }
  {
    std::shared_ptr<ANNSnapshotConfig> configCopy =
        std::make_shared<ANNSnapshotConfig>(log_context_);
    CopyConfigFields(config, configCopy);
    configCopy->SnapshotFolder = "1";  // hack
    configCopy->Dimension = 100001;
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG);
  }
  {
    std::shared_ptr<ANNSnapshotConfig> configCopy =
        std::make_shared<ANNSnapshotConfig>(log_context_);
    CopyConfigFields(config, configCopy);
    configCopy->SnapshotFolder = "1";  // hack
    configCopy->QueryNeighborsCount = 0;
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG);
  }
  {
    std::shared_ptr<ANNSnapshotConfig> configCopy =
        std::make_shared<ANNSnapshotConfig>(log_context_);
    CopyConfigFields(config, configCopy);
    configCopy->SnapshotFolder = "1";  // hack
    configCopy->QueryNeighborsCount = 1000000001;
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG);
  }
  {
    std::shared_ptr<ANNSnapshotConfig> configCopy =
        std::make_shared<ANNSnapshotConfig>(log_context_);
    CopyConfigFields(config, configCopy);
    configCopy->SnapshotFolder = "1";  // hack
    configCopy->TopCount = 0;
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG);
  }
  {
    std::shared_ptr<ANNSnapshotConfig> configCopy =
        std::make_shared<ANNSnapshotConfig>(log_context_);
    CopyConfigFields(config, configCopy);
    configCopy->SnapshotFolder = "1";  // hack
    configCopy->TopCount = 1000000001;
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG);
  }
  {
    std::shared_ptr<ANNSnapshotConfig> configCopy =
        std::make_shared<ANNSnapshotConfig>(log_context_);
    CopyConfigFields(config, configCopy);
    configCopy->SnapshotFolder = "1";      // hack
    configCopy->VectorTypeStr = "uint16";  // not supported type
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG);
  }
  {
    std::shared_ptr<ANNSnapshotConfig> configCopy =
        std::make_shared<ANNSnapshotConfig>(log_context_);
    CopyConfigFields(config, configCopy);
    configCopy->SnapshotFolder = "1";  // hack
    configCopy->VectorTypeStr = "";
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG);
  }
  {
    std::shared_ptr<ANNSnapshotConfig> configCopy =
        std::make_shared<ANNSnapshotConfig>(log_context_);
    CopyConfigFields(config, configCopy);
    configCopy->SnapshotFolder = "1";  // hack
    configCopy->IndexBaseFilename = "";
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG);
  }
  {
    std::shared_ptr<ANNSnapshotConfig> configCopy =
        std::make_shared<ANNSnapshotConfig>(log_context_);
    CopyConfigFields(config, configCopy);
    configCopy->SnapshotFolder = "1";  // hack
    configCopy->IndexDataFilename = "";
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG);
  }
  {
    std::shared_ptr<ANNSnapshotConfig> configCopy =
        std::make_shared<ANNSnapshotConfig>(log_context_);
    CopyConfigFields(config, configCopy);
    configCopy->SnapshotFolder = "1";  // hack
    configCopy->MappingFilename = "";
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG);
  }
  {
    std::string fake_path = snapshot_folder_ + "/" + unique_snapshot_name_ +
                            "/" + "fake_path_index";
    std::shared_ptr<ANNSnapshotConfig> configCopy =
        std::make_shared<ANNSnapshotConfig>(log_context_);
    CopyConfigFields(config, configCopy);
    configCopy->SnapshotFolder = "1";  // hack
    configCopy->IndexBaseFilename = fake_path;
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT_INDEX);

    std::ofstream file(fake_path, std::ios::binary);
    file.write(reinterpret_cast<const char*>("123"), 3);
    file.close();
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT_INDEX);

    std::ofstream nfile(fake_path, std::ios::binary);
    nfile.write(reinterpret_cast<const char*>("0123456789ABCDE"), 16);
    nfile.close();
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);
  }
  {
    std::string fake_path = snapshot_folder_ + "/" + unique_snapshot_name_ +
                            "/" + "fake_path_index.data";
    std::shared_ptr<ANNSnapshotConfig> configCopy =
        std::make_shared<ANNSnapshotConfig>(log_context_);
    CopyConfigFields(config, configCopy);
    configCopy->SnapshotFolder = "1";  // hack
    configCopy->IndexDataFilename = fake_path;
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT_INDEX_DATA);

    std::ofstream file(fake_path, std::ios::binary);
    file.write(reinterpret_cast<const char*>("123"), 3);
    file.close();
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT_INDEX_DATA);

    std::ofstream nfile(fake_path, std::ios::binary);
    nfile.write(reinterpret_cast<const char*>("0123456789ABCDE"), 16);
    nfile.close();
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);
  }
  {
    std::string fake_path = snapshot_folder_ + "/" + unique_snapshot_name_ +
                            "/" + "fake_path_mapping";
    std::shared_ptr<ANNSnapshotConfig> configCopy =
        std::make_shared<ANNSnapshotConfig>(log_context_);
    CopyConfigFields(config, configCopy);
    configCopy->SnapshotFolder = "1";  // hack
    configCopy->MappingFilename = fake_path;
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT_MAPPING);

    std::ofstream file(fake_path, std::ios::binary);
    file.write(reinterpret_cast<const char*>("123"), 3);
    file.close();
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT_MAPPING);

    std::ofstream nfile(fake_path, std::ios::binary);
    nfile.write(reinterpret_cast<const char*>("0123456789ABCDE"), 16);
    nfile.close();
    status = ANNSnapshotKeeper::CheckSnapshotIsValid(configCopy);
    EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);
  }
}

TEST_F(SnapshotKeeperTest, HandleIncomingSnapshotFunctionBasicTest) {
  DumpPreparedSnapshot();
  std::shared_ptr<ANNSnapshotConfig> config =
      std::make_shared<ANNSnapshotConfig>(log_context_);
  auto status = ANNSnapshotKeeper::HandleIncomingSnapshot(
      snapshot_folder_, unique_snapshot_name_, unique_snapshot_path_, config);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);
  EXPECT_EQ(config->SnapshotName, "ANNSNAPSHOT_00000000000000001");
  EXPECT_EQ(config->Dimension, 4);
  EXPECT_EQ(config->QueryNeighborsCount, 8);
  EXPECT_EQ(config->TopCount, 4);
  EXPECT_EQ(config->VectorTypeStr, "uint8");
  EXPECT_EQ(config->SnapshotFolder,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000001");
  EXPECT_EQ(config->IndexBaseFilename,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000001/index");
  EXPECT_EQ(config->IndexDataFilename,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000001/index.data");
  EXPECT_EQ(config->MappingFilename,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000001/mapping");

  status = ANNSnapshotKeeper::CheckSnapshotIsValid(config);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);
}

TEST_F(SnapshotKeeperTest, HandleIncomingSnapshotFunctionOtherSnapshotTest) {
  snapshot_bytes_ = {
      237, 254, 13,  240, 4,   0,   0,   0,   5,   0,   0,   0,   105, 110, 100,
      101, 120, 152, 3,   0,   0,   0,   0,   0,   0,   152, 3,   0,   0,   0,
      0,   0,   0,   14,  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
      0,   0,   0,   0,   14,  0,   0,   0,   1,   0,   0,   0,   3,   0,   0,
      0,   4,   0,   0,   0,   5,   0,   0,   0,   6,   0,   0,   0,   7,   0,
      0,   0,   8,   0,   0,   0,   9,   0,   0,   0,   10,  0,   0,   0,   11,
      0,   0,   0,   12,  0,   0,   0,   13,  0,   0,   0,   14,  0,   0,   0,
      15,  0,   0,   0,   2,   0,   0,   0,   0,   0,   0,   0,   2,   0,   0,
      0,   14,  0,   0,   0,   1,   0,   0,   0,   3,   0,   0,   0,   4,   0,
      0,   0,   5,   0,   0,   0,   6,   0,   0,   0,   7,   0,   0,   0,   8,
      0,   0,   0,   9,   0,   0,   0,   10,  0,   0,   0,   11,  0,   0,   0,
      12,  0,   0,   0,   13,  0,   0,   0,   14,  0,   0,   0,   15,  0,   0,
      0,   14,  0,   0,   0,   2,   0,   0,   0,   0,   0,   0,   0,   4,   0,
      0,   0,   5,   0,   0,   0,   6,   0,   0,   0,   7,   0,   0,   0,   8,
      0,   0,   0,   9,   0,   0,   0,   10,  0,   0,   0,   11,  0,   0,   0,
      12,  0,   0,   0,   13,  0,   0,   0,   14,  0,   0,   0,   15,  0,   0,
      0,   14,  0,   0,   0,   2,   0,   0,   0,   0,   0,   0,   0,   3,   0,
      0,   0,   5,   0,   0,   0,   6,   0,   0,   0,   7,   0,   0,   0,   8,
      0,   0,   0,   9,   0,   0,   0,   10,  0,   0,   0,   11,  0,   0,   0,
      12,  0,   0,   0,   13,  0,   0,   0,   14,  0,   0,   0,   15,  0,   0,
      0,   14,  0,   0,   0,   2,   0,   0,   0,   0,   0,   0,   0,   3,   0,
      0,   0,   4,   0,   0,   0,   6,   0,   0,   0,   7,   0,   0,   0,   8,
      0,   0,   0,   9,   0,   0,   0,   10,  0,   0,   0,   11,  0,   0,   0,
      12,  0,   0,   0,   13,  0,   0,   0,   14,  0,   0,   0,   15,  0,   0,
      0,   14,  0,   0,   0,   2,   0,   0,   0,   0,   0,   0,   0,   3,   0,
      0,   0,   4,   0,   0,   0,   5,   0,   0,   0,   7,   0,   0,   0,   8,
      0,   0,   0,   9,   0,   0,   0,   10,  0,   0,   0,   11,  0,   0,   0,
      12,  0,   0,   0,   13,  0,   0,   0,   14,  0,   0,   0,   15,  0,   0,
      0,   14,  0,   0,   0,   2,   0,   0,   0,   0,   0,   0,   0,   3,   0,
      0,   0,   4,   0,   0,   0,   5,   0,   0,   0,   6,   0,   0,   0,   8,
      0,   0,   0,   9,   0,   0,   0,   10,  0,   0,   0,   11,  0,   0,   0,
      12,  0,   0,   0,   13,  0,   0,   0,   14,  0,   0,   0,   15,  0,   0,
      0,   14,  0,   0,   0,   2,   0,   0,   0,   0,   0,   0,   0,   3,   0,
      0,   0,   4,   0,   0,   0,   5,   0,   0,   0,   6,   0,   0,   0,   7,
      0,   0,   0,   9,   0,   0,   0,   10,  0,   0,   0,   11,  0,   0,   0,
      12,  0,   0,   0,   13,  0,   0,   0,   14,  0,   0,   0,   15,  0,   0,
      0,   14,  0,   0,   0,   2,   0,   0,   0,   0,   0,   0,   0,   3,   0,
      0,   0,   4,   0,   0,   0,   5,   0,   0,   0,   6,   0,   0,   0,   7,
      0,   0,   0,   8,   0,   0,   0,   10,  0,   0,   0,   11,  0,   0,   0,
      12,  0,   0,   0,   13,  0,   0,   0,   14,  0,   0,   0,   15,  0,   0,
      0,   14,  0,   0,   0,   2,   0,   0,   0,   0,   0,   0,   0,   3,   0,
      0,   0,   4,   0,   0,   0,   5,   0,   0,   0,   6,   0,   0,   0,   7,
      0,   0,   0,   8,   0,   0,   0,   9,   0,   0,   0,   11,  0,   0,   0,
      12,  0,   0,   0,   13,  0,   0,   0,   14,  0,   0,   0,   15,  0,   0,
      0,   14,  0,   0,   0,   2,   0,   0,   0,   0,   0,   0,   0,   3,   0,
      0,   0,   4,   0,   0,   0,   5,   0,   0,   0,   6,   0,   0,   0,   7,
      0,   0,   0,   8,   0,   0,   0,   9,   0,   0,   0,   10,  0,   0,   0,
      12,  0,   0,   0,   13,  0,   0,   0,   14,  0,   0,   0,   15,  0,   0,
      0,   12,  0,   0,   0,   2,   0,   0,   0,   9,   0,   0,   0,   0,   0,
      0,   0,   3,   0,   0,   0,   4,   0,   0,   0,   5,   0,   0,   0,   6,
      0,   0,   0,   7,   0,   0,   0,   8,   0,   0,   0,   10,  0,   0,   0,
      11,  0,   0,   0,   15,  0,   0,   0,   13,  0,   0,   0,   2,   0,   0,
      0,   0,   0,   0,   0,   3,   0,   0,   0,   4,   0,   0,   0,   5,   0,
      0,   0,   6,   0,   0,   0,   7,   0,   0,   0,   8,   0,   0,   0,   9,
      0,   0,   0,   10,  0,   0,   0,   11,  0,   0,   0,   14,  0,   0,   0,
      15,  0,   0,   0,   13,  0,   0,   0,   2,   0,   0,   0,   0,   0,   0,
      0,   3,   0,   0,   0,   4,   0,   0,   0,   5,   0,   0,   0,   6,   0,
      0,   0,   7,   0,   0,   0,   8,   0,   0,   0,   9,   0,   0,   0,   10,
      0,   0,   0,   11,  0,   0,   0,   13,  0,   0,   0,   15,  0,   0,   0,
      14,  0,   0,   0,   2,   0,   0,   0,   12,  0,   0,   0,   0,   0,   0,
      0,   3,   0,   0,   0,   4,   0,   0,   0,   5,   0,   0,   0,   6,   0,
      0,   0,   7,   0,   0,   0,   8,   0,   0,   0,   10,  0,   0,   0,   11,
      0,   0,   0,   13,  0,   0,   0,   14,  0,   0,   0,   9,   0,   0,   0,
      10,  0,   0,   0,   105, 110, 100, 101, 120, 46,  100, 97,  116, 97,  136,
      1,   0,   0,   0,   0,   0,   0,   16,  0,   0,   0,   6,   0,   0,   0,
      1,   65,  185, 120, 84,  253, 101, 186, 225, 18,  69,  123, 74,  40,  83,
      167, 191, 137, 182, 246, 25,  196, 104, 255, 135, 68,  158, 127, 36,  28,
      131, 137, 113, 7,   28,  4,   147, 117, 53,  77,  251, 68,  180, 151, 37,
      95,  107, 85,  8,   59,  81,  242, 205, 30,  21,  39,  16,  18,  101, 219,
      61,  185, 114, 99,  73,  248, 88,  167, 124, 14,  254, 73,  149, 156, 235,
      166, 142, 211, 143, 96,  81,  233, 207, 125, 148, 15,  117, 175, 38,  199,
      53,  92,  144, 236, 1,   216, 155, 2,   217, 33,  237, 36,  95,  46,  32,
      237, 229, 83,  197, 53,  106, 59,  49,  75,  196, 103, 176, 125, 135, 55,
      151, 91,  94,  209, 77,  178, 163, 70,  13,  227, 108, 156, 61,  132, 28,
      37,  120, 234, 22,  111, 152, 218, 172, 105, 41,  39,  46,  147, 94,  163,
      38,  237, 158, 177, 193, 154, 16,  155, 23,  218, 46,  83,  77,  158, 224,
      142, 109, 193, 147, 79,  124, 155, 59,  192, 156, 116, 34,  90,  3,   175,
      248, 253, 172, 5,   5,   255, 55,  222, 103, 214, 97,  144, 136, 8,   225,
      167, 134, 178, 127, 167, 44,  81,  37,  78,  117, 133, 211, 205, 9,   171,
      126, 7,   31,  131, 20,  243, 103, 238, 41,  88,  63,  158, 113, 193, 254,
      76,  244, 173, 85,  192, 133, 217, 18,  246, 22,  28,  255, 103, 219, 221,
      232, 15,  238, 100, 18,  67,  206, 223, 138, 12,  29,  216, 62,  139, 172,
      81,  249, 67,  90,  97,  101, 30,  46,  145, 14,  231, 208, 229, 62,  88,
      42,  22,  197, 102, 2,   192, 161, 211, 204, 211, 96,  57,  232, 228, 101,
      255, 196, 99,  57,  60,  90,  60,  153, 195, 234, 47,  169, 56,  207, 138,
      103, 81,  9,   39,  26,  3,   15,  72,  65,  212, 244, 213, 152, 30,  8,
      109, 197, 148, 191, 199, 238, 167, 152, 253, 118, 64,  37,  23,  160, 255,
      213, 44,  55,  99,  108, 190, 234, 34,  107, 21,  173, 126, 159, 130, 130,
      70,  178, 54,  80,  74,  165, 43,  205, 38,  211, 45,  183, 11,  251, 95,
      235, 89,  150, 208, 134, 162, 40,  185, 241, 179, 166, 130, 125, 90,  235,
      199, 66,  57,  39,  166, 124, 243, 189, 183, 7,   0,   0,   0,   109, 97,
      112, 112, 105, 110, 103, 18,  1,   0,   0,   0,   0,   0,   0,   16,  0,
      0,   0,   13,  0,   0,   0,   98,  97,  110, 110, 101, 114, 95,  52,  48,
      49,  48,  55,  56,  13,  0,   0,   0,   98,  97,  110, 110, 101, 114, 95,
      57,  55,  48,  56,  57,  52,  11,  0,   0,   0,   98,  97,  110, 110, 101,
      114, 95,  55,  55,  51,  52,  13,  0,   0,   0,   98,  97,  110, 110, 101,
      114, 95,  52,  52,  50,  51,  53,  54,  13,  0,   0,   0,   98,  97,  110,
      110, 101, 114, 95,  56,  52,  48,  55,  50,  49,  13,  0,   0,   0,   98,
      97,  110, 110, 101, 114, 95,  53,  51,  56,  55,  56,  55,  13,  0,   0,
      0,   98,  97,  110, 110, 101, 114, 95,  57,  53,  48,  53,  50,  57,  13,
      0,   0,   0,   98,  97,  110, 110, 101, 114, 95,  56,  54,  50,  50,  49,
      49,  13,  0,   0,   0,   98,  97,  110, 110, 101, 114, 95,  55,  52,  48,
      56,  49,  50,  13,  0,   0,   0,   98,  97,  110, 110, 101, 114, 95,  51,
      54,  51,  55,  52,  57,  13,  0,   0,   0,   98,  97,  110, 110, 101, 114,
      95,  53,  51,  56,  49,  49,  50,  13,  0,   0,   0,   98,  97,  110, 110,
      101, 114, 95,  50,  53,  50,  56,  53,  55,  13,  0,   0,   0,   98,  97,
      110, 110, 101, 114, 95,  56,  50,  55,  57,  57,  53,  13,  0,   0,   0,
      98,  97,  110, 110, 101, 114, 95,  52,  48,  48,  51,  55,  49,  13,  0,
      0,   0,   98,  97,  110, 110, 101, 114, 95,  53,  53,  50,  55,  48,  51,
      13,  0,   0,   0,   98,  97,  110, 110, 101, 114, 95,  49,  51,  48,  53,
      51,  52,  11,  0,   0,   0,   99,  111, 110, 102, 105, 103, 46,  106, 115,
      111, 110, 85,  0,   0,   0,   0,   0,   0,   0,   123, 34,  68,  105, 109,
      101, 110, 115, 105, 111, 110, 34,  58,  32,  54,  44,  32,  34,  81,  117,
      101, 114, 121, 78,  101, 105, 103, 104, 98,  111, 114, 115, 67,  111, 117,
      110, 116, 34,  58,  32,  49,  50,  44,  32,  34,  84,  111, 112, 67,  111,
      117, 110, 116, 34,  58,  32,  49,  48,  44,  32,  34,  86,  101, 99,  116,
      111, 114, 84,  121, 112, 101, 83,  116, 114, 34,  58,  32,  34,  102, 108,
      111, 97,  116, 34,  125,
  };
  unique_snapshot_name_ = "ANNSNAPSHOT_00000000000000002";
  unique_snapshot_path_ = unique_test_folder_ + "/" + unique_snapshot_name_;
  DumpPreparedSnapshot();
  std::shared_ptr<ANNSnapshotConfig> config =
      std::make_shared<ANNSnapshotConfig>(log_context_);
  auto status = ANNSnapshotKeeper::HandleIncomingSnapshot(
      snapshot_folder_, unique_snapshot_name_, unique_snapshot_path_, config);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);
  EXPECT_EQ(config->SnapshotName, "ANNSNAPSHOT_00000000000000002");
  EXPECT_EQ(config->Dimension, 6);
  EXPECT_EQ(config->QueryNeighborsCount, 12);
  EXPECT_EQ(config->TopCount, 10);
  EXPECT_EQ(config->VectorTypeStr, "float");
  EXPECT_EQ(config->SnapshotFolder,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000002");
  EXPECT_EQ(config->IndexBaseFilename,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000002/index");
  EXPECT_EQ(config->IndexDataFilename,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000002/index.data");
  EXPECT_EQ(config->MappingFilename,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000002/mapping");

  status = ANNSnapshotKeeper::CheckSnapshotIsValid(config);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);
}

TEST_F(SnapshotKeeperTest, HandleIncomingSnapshotFunctionTwoSnapshotsTest) {
  DumpPreparedSnapshot();
  std::shared_ptr<ANNSnapshotConfig> config1 =
      std::make_shared<ANNSnapshotConfig>(log_context_);
  std::shared_ptr<ANNSnapshotConfig> config2 =
      std::make_shared<ANNSnapshotConfig>(log_context_);
  auto status = ANNSnapshotKeeper::HandleIncomingSnapshot(
      snapshot_folder_, unique_snapshot_name_, unique_snapshot_path_, config1);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);

  snapshot_bytes_ = {
      237, 254, 13,  240, 4,   0,   0,   0,   5,   0,   0,   0,   105, 110, 100,
      101, 120, 32,  1,   0,   0,   0,   0,   0,   0,   32,  1,   0,   0,   0,
      0,   0,   0,   7,   0,   0,   0,   9,   0,   0,   0,   0,   0,   0,   0,
      0,   0,   0,   0,   7,   0,   0,   0,   9,   0,   0,   0,   1,   0,   0,
      0,   2,   0,   0,   0,   3,   0,   0,   0,   4,   0,   0,   0,   5,   0,
      0,   0,   6,   0,   0,   0,   7,   0,   0,   0,   0,   0,   0,   0,   9,
      0,   0,   0,   3,   0,   0,   0,   4,   0,   0,   0,   5,   0,   0,   0,
      7,   0,   0,   0,   8,   0,   0,   0,   2,   0,   0,   0,   0,   0,   0,
      0,   6,   0,   0,   0,   5,   0,   0,   0,   1,   0,   0,   0,   9,   0,
      0,   0,   0,   0,   0,   0,   8,   0,   0,   0,   10,  0,   0,   0,   4,
      0,   0,   0,   9,   0,   0,   0,   0,   0,   0,   0,   1,   0,   0,   0,
      5,   0,   0,   0,   6,   0,   0,   0,   4,   0,   0,   0,   0,   0,   0,
      0,   9,   0,   0,   0,   1,   0,   0,   0,   8,   0,   0,   0,   11,  0,
      0,   0,   5,   0,   0,   0,   2,   0,   0,   0,   9,   0,   0,   0,   0,
      0,   0,   0,   10,  0,   0,   0,   11,  0,   0,   0,   3,   0,   0,   0,
      9,   0,   0,   0,   1,   0,   0,   0,   8,   0,   0,   0,   5,   0,   0,
      0,   1,   0,   0,   0,   7,   0,   0,   0,   5,   0,   0,   0,   3,   0,
      0,   0,   9,   0,   0,   0,   5,   0,   0,   0,   5,   0,   0,   0,   7,
      0,   0,   0,   6,   0,   0,   0,   3,   0,   0,   0,   11,  0,   0,   0,
      2,   0,   0,   0,   6,   0,   0,   0,   3,   0,   0,   0,   3,   0,   0,
      0,   6,   0,   0,   0,   9,   0,   0,   0,   5,   0,   0,   0,   10,  0,
      0,   0,   105, 110, 100, 101, 120, 46,  100, 97,  116, 97,  44,  0,   0,
      0,   0,   0,   0,   0,   12,  0,   0,   0,   3,   0,   0,   0,   199, 233,
      172, 195, 38,  124, 99,  148, 189, 182, 12,  84,  209, 90,  172, 229, 51,
      181, 87,  227, 213, 47,  74,  106, 212, 47,  102, 16,  61,  0,   125, 195,
      54,  43,  15,  201, 7,   0,   0,   0,   109, 97,  112, 112, 105, 110, 103,
      207, 0,   0,   0,   0,   0,   0,   0,   12,  0,   0,   0,   13,  0,   0,
      0,   98,  97,  110, 110, 101, 114, 95,  53,  54,  50,  49,  51,  54,  13,
      0,   0,   0,   98,  97,  110, 110, 101, 114, 95,  54,  56,  55,  55,  56,
      48,  13,  0,   0,   0,   98,  97,  110, 110, 101, 114, 95,  50,  55,  48,
      55,  51,  51,  13,  0,   0,   0,   98,  97,  110, 110, 101, 114, 95,  57,
      49,  53,  48,  53,  50,  13,  0,   0,   0,   98,  97,  110, 110, 101, 114,
      95,  56,  49,  56,  52,  49,  57,  13,  0,   0,   0,   98,  97,  110, 110,
      101, 114, 95,  54,  52,  56,  51,  57,  55,  13,  0,   0,   0,   98,  97,
      110, 110, 101, 114, 95,  56,  48,  48,  57,  48,  57,  13,  0,   0,   0,
      98,  97,  110, 110, 101, 114, 95,  53,  49,  49,  48,  56,  49,  13,  0,
      0,   0,   98,  97,  110, 110, 101, 114, 95,  49,  51,  56,  56,  52,  53,
      13,  0,   0,   0,   98,  97,  110, 110, 101, 114, 95,  51,  53,  50,  54,
      54,  51,  12,  0,   0,   0,   98,  97,  110, 110, 101, 114, 95,  51,  52,
      55,  54,  55,  13,  0,   0,   0,   98,  97,  110, 110, 101, 114, 95,  49,
      56,  48,  56,  48,  48,  11,  0,   0,   0,   99,  111, 110, 102, 105, 103,
      46,  106, 115, 111, 110, 82,  0,   0,   0,   0,   0,   0,   0,   123, 34,
      68,  105, 109, 101, 110, 115, 105, 111, 110, 34,  58,  32,  51,  44,  32,
      34,  81,  117, 101, 114, 121, 78,  101, 105, 103, 104, 98,  111, 114, 115,
      67,  111, 117, 110, 116, 34,  58,  32,  54,  44,  32,  34,  84,  111, 112,
      67,  111, 117, 110, 116, 34,  58,  32,  53,  44,  32,  34,  86,  101, 99,
      116, 111, 114, 84,  121, 112, 101, 83,  116, 114, 34,  58,  32,  34,  105,
      110, 116, 56,  34,  125,
  };
  unique_snapshot_name_ = "ANNSNAPSHOT_00000000000000003";
  unique_snapshot_path_ = unique_test_folder_ + "/" + unique_snapshot_name_;
  DumpPreparedSnapshot();
  status = ANNSnapshotKeeper::HandleIncomingSnapshot(
      snapshot_folder_, unique_snapshot_name_, unique_snapshot_path_, config2);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);
  EXPECT_EQ(config1->SnapshotName, "ANNSNAPSHOT_00000000000000001");
  EXPECT_EQ(config1->Dimension, 4);
  EXPECT_EQ(config1->QueryNeighborsCount, 8);
  EXPECT_EQ(config1->TopCount, 4);
  EXPECT_EQ(config1->VectorTypeStr, "uint8");
  EXPECT_EQ(config1->SnapshotFolder,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000001");
  EXPECT_EQ(config1->IndexBaseFilename,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000001/index");
  EXPECT_EQ(config1->IndexDataFilename,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000001/index.data");
  EXPECT_EQ(config1->MappingFilename,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000001/mapping");

  EXPECT_EQ(config2->SnapshotName, "ANNSNAPSHOT_00000000000000003");
  EXPECT_EQ(config2->Dimension, 3);
  EXPECT_EQ(config2->QueryNeighborsCount, 6);
  EXPECT_EQ(config2->TopCount, 5);
  EXPECT_EQ(config2->VectorTypeStr, "int8");
  EXPECT_EQ(config2->SnapshotFolder,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000003");
  EXPECT_EQ(config2->IndexBaseFilename,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000003/index");
  EXPECT_EQ(config2->IndexDataFilename,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000003/index.data");
  EXPECT_EQ(config2->MappingFilename,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000003/mapping");

  status = ANNSnapshotKeeper::CheckSnapshotIsValid(config1);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);

  status = ANNSnapshotKeeper::CheckSnapshotIsValid(config2);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);
}

TEST_F(SnapshotKeeperTest, NoSnapshotTest) {
  std::shared_ptr<ANNSnapshotConfig> config =
      std::make_shared<ANNSnapshotConfig>(log_context_);
  auto status = ANNSnapshotKeeper::HandleIncomingSnapshot(
      snapshot_folder_, unique_snapshot_name_, unique_snapshot_path_, config);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IO_CANT_OPEN_INCOMING_SNAPSHOT_FILE);
}

TEST_F(SnapshotKeeperTest, EmptySnapshotTest) {
  snapshot_bytes_ = {};
  DumpPreparedSnapshot();
  std::shared_ptr<ANNSnapshotConfig> config =
      std::make_shared<ANNSnapshotConfig>(log_context_);
  auto status = ANNSnapshotKeeper::HandleIncomingSnapshot(
      snapshot_folder_, unique_snapshot_name_, unique_snapshot_path_, config);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IFSTREAM_FAILURE);
}

TEST_F(SnapshotKeeperTest, CorruptedSnapshotTest) {
  snapshot_bytes_ = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  DumpPreparedSnapshot();
  std::shared_ptr<ANNSnapshotConfig> config =
      std::make_shared<ANNSnapshotConfig>(log_context_);
  auto status = ANNSnapshotKeeper::HandleIncomingSnapshot(
      snapshot_folder_, unique_snapshot_name_, unique_snapshot_path_, config);
  EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT);
}

TEST_F(SnapshotKeeperTest, CorruptedSnapshotNoMagicNumbersTest) {
  snapshot_bytes_[0] = 0;
  snapshot_bytes_[1] = 0;
  snapshot_bytes_[2] = 0;
  snapshot_bytes_[3] = 0;
  DumpPreparedSnapshot();
  std::shared_ptr<ANNSnapshotConfig> config =
      std::make_shared<ANNSnapshotConfig>(log_context_);
  auto status = ANNSnapshotKeeper::HandleIncomingSnapshot(
      snapshot_folder_, unique_snapshot_name_, unique_snapshot_path_, config);
  EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT);
}

TEST_F(SnapshotKeeperTest, InvalidTailSnapshotTest) {
  snapshot_bytes_.push_back(137);
  DumpPreparedSnapshot();
  std::shared_ptr<ANNSnapshotConfig> config =
      std::make_shared<ANNSnapshotConfig>(log_context_);
  auto status = ANNSnapshotKeeper::HandleIncomingSnapshot(
      snapshot_folder_, unique_snapshot_name_, unique_snapshot_path_, config);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IFSTREAM_FAILURE);
}

TEST_F(SnapshotKeeperTest, NoFilesSnapshotTest) {
  snapshot_bytes_ = {237, 254, 13, 240, 0, 0, 0, 0};
  DumpPreparedSnapshot();
  std::shared_ptr<ANNSnapshotConfig> config =
      std::make_shared<ANNSnapshotConfig>(log_context_);
  auto status = ANNSnapshotKeeper::HandleIncomingSnapshot(
      snapshot_folder_, unique_snapshot_name_, unique_snapshot_path_, config);
  EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT);
}

TEST_F(SnapshotKeeperTest, NoFilesCorruptedSnapshotTest) {
  snapshot_bytes_ = {
      237,
      254,
      13,
      240,
  };
  DumpPreparedSnapshot();
  std::shared_ptr<ANNSnapshotConfig> config =
      std::make_shared<ANNSnapshotConfig>(log_context_);
  auto status = ANNSnapshotKeeper::HandleIncomingSnapshot(
      snapshot_folder_, unique_snapshot_name_, unique_snapshot_path_, config);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IFSTREAM_FAILURE);
}

TEST_F(SnapshotKeeperTest, SnapshotWithEmptyFileTest) {
  snapshot_bytes_ = {237, 254, 13,  240, 4, 0, 0, 0, 5, 0, 0, 0, 101, 109,
                     112, 116, 121, 0,   0, 0, 0, 0, 0, 0, 0, 1, 0,   0,
                     0,   65,  1,   0,   0, 0, 0, 0, 0, 0, 0, 1, 0,   0,
                     0,   66,  1,   0,   0, 0, 0, 0, 0, 0, 0, 1, 0,   0,
                     0,   67,  1,   0,   0, 0, 0, 0, 0, 0, 0};
  DumpPreparedSnapshot();
  std::shared_ptr<ANNSnapshotConfig> config =
      std::make_shared<ANNSnapshotConfig>(log_context_);
  auto status = ANNSnapshotKeeper::HandleIncomingSnapshot(
      snapshot_folder_, unique_snapshot_name_, unique_snapshot_path_, config);
  EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT);
}

TEST_F(SnapshotKeeperTest, TooManyFilesSnapshotTest) {
  snapshot_bytes_ = {237, 254, 13, 240, 0, 0, 0, 127};
  DumpPreparedSnapshot();
  std::shared_ptr<ANNSnapshotConfig> config =
      std::make_shared<ANNSnapshotConfig>(log_context_);
  auto status = ANNSnapshotKeeper::HandleIncomingSnapshot(
      snapshot_folder_, unique_snapshot_name_, unique_snapshot_path_, config);
  EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT);
}

TEST_F(SnapshotKeeperTest, SnapshotWithoutMandatoryFilesTest) {
  snapshot_bytes_ = {
      237, 254, 13,  240, 4,   0,   0,   0,   11,  0,   0,   0,   99,  111, 110,
      102, 105, 103, 46,  106, 115, 111, 110, 82,  0,   0,   0,   0,   0,   0,
      0,   123, 34,  68,  105, 109, 101, 110, 115, 105, 111, 110, 34,  58,  32,
      51,  44,  32,  34,  81,  117, 101, 114, 121, 78,  101, 105, 103, 104, 98,
      111, 114, 115, 67,  111, 117, 110, 116, 34,  58,  32,  54,  44,  32,  34,
      84,  111, 112, 67,  111, 117, 110, 116, 34,  58,  32,  53,  44,  32,  34,
      86,  101, 99,  116, 111, 114, 84,  121, 112, 101, 83,  116, 114, 34,  58,
      32,  34,  105, 110, 116, 56,  34,  125, 1,   0,   0,   0,   65,  1,   0,
      0,   0,   0,   0,   0,   0,   0,   1,   0,   0,   0,   66,  1,   0,   0,
      0,   0,   0,   0,   0,   0,   1,   0,   0,   0,   67,  1,   0,   0,   0,
      0,   0,   0,   0,   0};
  DumpPreparedSnapshot();
  std::shared_ptr<ANNSnapshotConfig> config =
      std::make_shared<ANNSnapshotConfig>(log_context_);
  auto status = ANNSnapshotKeeper::HandleIncomingSnapshot(
      snapshot_folder_, unique_snapshot_name_, unique_snapshot_path_, config);
  EXPECT_EQ(status, SNAPSHOT_STATUS::INVALID_SNAPSHOT_CONFIG);
}

TEST_F(SnapshotKeeperTest, SnapshotWithInvalidFilenameTest) {
  // 47 is '/', file cannot be called with '/'
  snapshot_bytes_ = {237, 254, 13, 240, 4, 0, 0,  0, 1,  0, 0,  0, 47, 1, 0, 0,
                     0,   0,   0,  0,   0, 0, 1,  0, 0,  0, 65, 1, 0,  0, 0, 0,
                     0,   0,   0,  0,   1, 0, 0,  0, 66, 1, 0,  0, 0,  0, 0, 0,
                     0,   0,   1,  0,   0, 0, 67, 1, 0,  0, 0,  0, 0,  0, 0, 0};
  DumpPreparedSnapshot();
  std::shared_ptr<ANNSnapshotConfig> config =
      std::make_shared<ANNSnapshotConfig>(log_context_);
  auto status = ANNSnapshotKeeper::HandleIncomingSnapshot(
      snapshot_folder_, unique_snapshot_name_, unique_snapshot_path_, config);
  EXPECT_EQ(status, SNAPSHOT_STATUS::OFSTREAM_FAILURE);
}

TEST_F(SnapshotKeeperTest, KeeperKeepsLastStateTest) {
  DumpPreparedSnapshot();
  ANNSnapshotKeeper keeper(snapshot_folder_);
  auto status = keeper.TryAddANNSnapshot(unique_snapshot_name_,
                                         unique_snapshot_path_, log_context_);
  ASSERT_EQ(status, SNAPSHOT_STATUS::OK);
  ASSERT_TRUE(keeper.HasANNSnapshots());
  ASSERT_TRUE(keeper.GetActualANNSnapshot());
  EXPECT_EQ(keeper.DequeCapacity(), 1);
  // should not remove the only one
  keeper.TryRemoveUnusedANNSnapshots();
  ASSERT_TRUE(keeper.HasANNSnapshots());
  ASSERT_TRUE(keeper.GetActualANNSnapshot());
  EXPECT_EQ(keeper.DequeCapacity(), 1);
}

TEST_F(SnapshotKeeperTest, KeeperBasicTest) {
  DumpPreparedSnapshot();
  ANNSnapshotKeeper keeper(snapshot_folder_);
  auto status = keeper.TryAddANNSnapshot(unique_snapshot_name_,
                                         unique_snapshot_path_, log_context_);
  ASSERT_EQ(status, SNAPSHOT_STATUS::OK);
  ASSERT_TRUE(keeper.HasANNSnapshots());
  ASSERT_TRUE(keeper.GetActualANNSnapshot());
  EXPECT_EQ(keeper.DequeCapacity(), 1);
  EXPECT_EQ(keeper.GetActualANNSnapshot()->Config->SnapshotName,
            "ANNSNAPSHOT_00000000000000001");
  EXPECT_EQ(keeper.GetActualANNSnapshot()->Config->Dimension, 4);
  EXPECT_EQ(keeper.GetActualANNSnapshot()->Config->QueryNeighborsCount, 8);
  EXPECT_EQ(keeper.GetActualANNSnapshot()->Config->TopCount, 4);
  EXPECT_EQ(keeper.GetActualANNSnapshot()->Config->VectorTypeStr, "uint8");
  EXPECT_EQ(keeper.GetActualANNSnapshot()->Config->SnapshotFolder,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000001");
  EXPECT_EQ(keeper.GetActualANNSnapshot()->Config->IndexBaseFilename,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000001/index");
  EXPECT_EQ(keeper.GetActualANNSnapshot()->Config->IndexDataFilename,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000001/index.data");
  EXPECT_EQ(keeper.GetActualANNSnapshot()->Config->MappingFilename,
            snapshot_folder_ + "/ANNSNAPSHOT_00000000000000001/mapping");
}

TEST_F(SnapshotKeeperTest, KeeperNotFreshAttemptTest) {
  DumpPreparedSnapshot();
  ANNSnapshotKeeper keeper(snapshot_folder_);
  auto status = keeper.TryAddANNSnapshot(unique_snapshot_name_,
                                         unique_snapshot_path_, log_context_);
  ASSERT_EQ(status, SNAPSHOT_STATUS::OK);
  ASSERT_TRUE(keeper.HasANNSnapshots());
  EXPECT_EQ(keeper.DequeCapacity(), 1);

  unique_snapshot_name_ = "ANNSNAPSHOT_00000000000000000";
  unique_snapshot_path_ = unique_test_folder_ + "/" + unique_snapshot_name_;
  DumpPreparedSnapshot();
  status = keeper.TryAddANNSnapshot(unique_snapshot_name_,
                                    unique_snapshot_path_, log_context_);
  ASSERT_EQ(status, SNAPSHOT_STATUS::NOT_FRESH);
  ASSERT_TRUE(keeper.HasANNSnapshots());
  EXPECT_EQ(keeper.DequeCapacity(), 1);
  EXPECT_EQ(keeper.GetActualANNSnapshot()->Config->SnapshotName,
            "ANNSNAPSHOT_00000000000000001");
  keeper.TryRemoveUnusedANNSnapshots();
  ASSERT_TRUE(keeper.HasANNSnapshots());
  EXPECT_EQ(keeper.DequeCapacity(), 1);
  EXPECT_EQ(keeper.GetActualANNSnapshot()->Config->SnapshotName,
            "ANNSNAPSHOT_00000000000000001");
}

TEST_F(SnapshotKeeperTest, KeeperNewOneTest) {
  DumpPreparedSnapshot();
  ANNSnapshotKeeper keeper(snapshot_folder_);
  auto status = keeper.TryAddANNSnapshot(unique_snapshot_name_,
                                         unique_snapshot_path_, log_context_);
  ASSERT_EQ(status, SNAPSHOT_STATUS::OK);
  ASSERT_TRUE(keeper.HasANNSnapshots());
  EXPECT_EQ(keeper.DequeCapacity(), 1);

  unique_snapshot_name_ = "ANNSNAPSHOT_00000000000000002";
  unique_snapshot_path_ = unique_test_folder_ + "/" + unique_snapshot_name_;
  DumpPreparedSnapshot();
  status = keeper.TryAddANNSnapshot(unique_snapshot_name_,
                                    unique_snapshot_path_, log_context_);
  EXPECT_EQ(keeper.DequeCapacity(), 2);
  ASSERT_EQ(status, SNAPSHOT_STATUS::OK);
  ASSERT_TRUE(keeper.HasANNSnapshots());
  EXPECT_EQ(keeper.GetActualANNSnapshot()->Config->SnapshotName,
            "ANNSNAPSHOT_00000000000000002");
  keeper.TryRemoveUnusedANNSnapshots();
  EXPECT_EQ(keeper.DequeCapacity(), 1);
  ASSERT_TRUE(keeper.HasANNSnapshots());
  EXPECT_EQ(keeper.GetActualANNSnapshot()->Config->SnapshotName,
            "ANNSNAPSHOT_00000000000000002");
}

TEST_F(SnapshotKeeperTest, KeeperNotRemoveUsedTest) {
  DumpPreparedSnapshot();
  ANNSnapshotKeeper keeper(snapshot_folder_);
  auto status = keeper.TryAddANNSnapshot(unique_snapshot_name_,
                                         unique_snapshot_path_, log_context_);
  ASSERT_EQ(status, SNAPSHOT_STATUS::OK);
  ASSERT_TRUE(keeper.HasANNSnapshots());

  unique_snapshot_name_ = "ANNSNAPSHOT_00000000000000002";
  unique_snapshot_path_ = unique_test_folder_ + "/" + unique_snapshot_name_;
  DumpPreparedSnapshot();

  {
    EXPECT_EQ(keeper.DequeCapacity(), 1);
    // we having it in local context to preserve removing
    auto statePtr = keeper.GetActualANNSnapshot();
    status = keeper.TryAddANNSnapshot(unique_snapshot_name_,
                                      unique_snapshot_path_, log_context_);
    ASSERT_EQ(status, SNAPSHOT_STATUS::OK);
    ASSERT_TRUE(keeper.HasANNSnapshots());
    EXPECT_EQ(keeper.DequeCapacity(), 2);
    keeper.TryRemoveUnusedANNSnapshots();
    EXPECT_EQ(keeper.GetActualANNSnapshot()->Config->SnapshotName,
              "ANNSNAPSHOT_00000000000000002");
    keeper.TryRemoveUnusedANNSnapshots();
    EXPECT_EQ(statePtr->Config->SnapshotName, "ANNSNAPSHOT_00000000000000001");
    EXPECT_FALSE(statePtr.unique());
    EXPECT_EQ(keeper.DequeCapacity(), 2);
  }
  EXPECT_EQ(keeper.DequeCapacity(), 2);
  keeper.TryRemoveUnusedANNSnapshots();
  EXPECT_EQ(keeper.DequeCapacity(), 1);
}

TEST_F(SnapshotKeeperTest, KeeperRemoveAllUnusedTest) {
  DumpPreparedSnapshot();
  ANNSnapshotKeeper keeper(snapshot_folder_);
  auto status = keeper.TryAddANNSnapshot(unique_snapshot_name_,
                                         unique_snapshot_path_, log_context_);
  ASSERT_EQ(status, SNAPSHOT_STATUS::OK);
  ASSERT_TRUE(keeper.HasANNSnapshots());

  unique_snapshot_name_ = "ANNSNAPSHOT_00000000000000002";
  unique_snapshot_path_ = unique_test_folder_ + "/" + unique_snapshot_name_;
  DumpPreparedSnapshot();

  {
    EXPECT_EQ(keeper.DequeCapacity(), 1);
    auto statePtr = keeper.GetActualANNSnapshot();
    status = keeper.TryAddANNSnapshot(unique_snapshot_name_,
                                      unique_snapshot_path_, log_context_);
    ASSERT_EQ(status, SNAPSHOT_STATUS::OK);
    ASSERT_TRUE(keeper.HasANNSnapshots());
    auto statePtrNew = keeper.GetActualANNSnapshot();
    status = keeper.TryAddANNSnapshot(unique_snapshot_name_ + "1",
                                      unique_snapshot_path_, log_context_);
    ASSERT_EQ(status, SNAPSHOT_STATUS::OK);
    ASSERT_TRUE(keeper.HasANNSnapshots());
    EXPECT_EQ(keeper.DequeCapacity(), 3);
    keeper.TryRemoveUnusedANNSnapshots();
    EXPECT_EQ(keeper.GetActualANNSnapshot()->Config->SnapshotName,
              "ANNSNAPSHOT_000000000000000021");
    keeper.TryRemoveUnusedANNSnapshots();
    EXPECT_EQ(statePtr->Config->SnapshotName, "ANNSNAPSHOT_00000000000000001");
    EXPECT_FALSE(statePtr.unique());
    EXPECT_EQ(keeper.DequeCapacity(), 3);
    keeper.TryRemoveUnusedANNSnapshots();
    EXPECT_EQ(statePtrNew->Config->SnapshotName,
              "ANNSNAPSHOT_00000000000000002");
    EXPECT_FALSE(statePtrNew.unique());
    EXPECT_EQ(keeper.DequeCapacity(), 3);
  }
  EXPECT_EQ(keeper.DequeCapacity(), 3);
  keeper.TryRemoveUnusedANNSnapshots();
  EXPECT_EQ(keeper.DequeCapacity(), 1);
}

TEST_F(SnapshotKeeperTest, CheckFreshFunctionTest) {
  DumpPreparedSnapshot();
  ANNSnapshotKeeper keeper(snapshot_folder_);
  auto status = keeper.TryAddANNSnapshot(unique_snapshot_name_,
                                         unique_snapshot_path_, log_context_);
  EXPECT_EQ(status, SNAPSHOT_STATUS::OK);
  EXPECT_TRUE(keeper.HasANNSnapshots());
  EXPECT_TRUE(keeper.GetActualANNSnapshot());

  EXPECT_FALSE(keeper.CheckNewSnapshotIsFresh(""));
  EXPECT_FALSE(keeper.CheckNewSnapshotIsFresh(unique_snapshot_name_));
  EXPECT_FALSE(keeper.CheckNewSnapshotIsFresh("ANNSNAPSHOT_00000000000000000"));
  EXPECT_FALSE(keeper.CheckNewSnapshotIsFresh("ANNSNAPSHOT_00000000000000001"));
  EXPECT_TRUE(keeper.CheckNewSnapshotIsFresh("ANNSNAPSHOT_00000000000000002"));
  EXPECT_TRUE(keeper.CheckNewSnapshotIsFresh("ANNSNAPSHOT_00000000000000003"));
  EXPECT_TRUE(keeper.CheckNewSnapshotIsFresh("B"));
  EXPECT_FALSE(keeper.CheckNewSnapshotIsFresh("0"));
}

}  // namespace
}  // namespace microsoft
}  // namespace kv_server
