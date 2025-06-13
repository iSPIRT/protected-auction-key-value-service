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

#include "components/data_server/microsoft_ann_index/snapshot_mapping.h"

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace kv_server {
namespace microsoft {
namespace {

class MappingTest : public testing::Test {
 protected:
  void SetUp() override {
    unique_test_folder_ = "Test_Folder_" + rand_string();
    std::filesystem::create_directory(unique_test_folder_);
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

  std::string unique_test_folder_;
  privacy_sandbox::server_common::log::NoOpContext log_context_;
  std::vector<std::string> url_keys_ = {
      "http://banners.test/banner_6", "http://banners.test/banner_8",
      "http://banners.test/banner_11", "http://banners.test/banner_5",
      "http://banners.test/banner_0"};
};

TEST_F(MappingTest, BasicTest) {
  auto status = SNAPSHOT_STATUS::IN_PROGRESS;
  std::string test_file = unique_test_folder_ + "/mapping";
  std::ofstream file(test_file, std::ios::binary);
  uint32_t lenVar;
  lenVar = url_keys_.size();
  file.write(reinterpret_cast<char*>(&lenVar), 4);
  for (const auto& key : url_keys_) {
    lenVar = key.size();
    file.write(reinterpret_cast<char*>(&lenVar), 4);
    file.write(key.c_str(), lenVar);
  }
  file.close();

  ANNSnapshotMapping mapping(test_file, status, log_context_);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);  // not changed
  EXPECT_EQ(mapping.GetStr(1), "http://banners.test/banner_8");
  EXPECT_EQ(mapping.GetStr(0), "http://banners.test/banner_6");
  EXPECT_EQ(mapping.GetStr(3), "http://banners.test/banner_5");
  EXPECT_EQ(mapping.GetStr(2), "http://banners.test/banner_11");
  EXPECT_EQ(mapping.GetStr(4), "http://banners.test/banner_0");
  EXPECT_EQ(mapping.GetStr(0), "http://banners.test/banner_6");
  EXPECT_EQ(mapping.GetStr(4), "http://banners.test/banner_0");
  EXPECT_EQ(mapping.GetStr(3), "http://banners.test/banner_5");
}

TEST_F(MappingTest, UnknownKeysTest) {
  auto status = SNAPSHOT_STATUS::IN_PROGRESS;
  std::string test_file = unique_test_folder_ + "/mapping";
  std::ofstream file(test_file, std::ios::binary);
  uint32_t lenVar;
  lenVar = url_keys_.size();
  file.write(reinterpret_cast<char*>(&lenVar), 4);
  for (const auto& key : url_keys_) {
    lenVar = key.size();
    file.write(reinterpret_cast<char*>(&lenVar), 4);
    file.write(key.c_str(), lenVar);
  }
  file.close();

  ANNSnapshotMapping mapping(test_file, status, log_context_);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);  // not changed
  EXPECT_EQ(mapping.GetStr((size_t)-2), "");
  EXPECT_EQ(mapping.GetStr((size_t)-1), "");
  EXPECT_EQ(mapping.GetStr(5), "");
  EXPECT_EQ(mapping.GetStr(6), "");
  EXPECT_EQ(mapping.GetStr(0), "http://banners.test/banner_6");
}

TEST_F(MappingTest, InvalidPathTest) {
  auto status = SNAPSHOT_STATUS::IN_PROGRESS;
  std::string test_file = unique_test_folder_ + "/mapping";

  ANNSnapshotMapping mapping(test_file, status, log_context_);
  EXPECT_EQ(status, SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_MAPPING_FILE);
}

TEST_F(MappingTest, InvalidFileTest) {
  auto status = SNAPSHOT_STATUS::IN_PROGRESS;
  std::string test_file1 = unique_test_folder_ + "/mapping1";
  std::string test_file2 = unique_test_folder_ + "/mapping2";
  std::string test_file3 = unique_test_folder_ + "/mapping3";
  std::string test_file4 = unique_test_folder_ + "/mapping4";
  {
    unsigned char lenVar = 1;
    std::ofstream file(test_file1, std::ios::binary);
    file.write(reinterpret_cast<char*>(&lenVar), 1);
    file.close();
  }
  {
    uint32_t lenVar = 1;
    std::ofstream file(test_file2, std::ios::binary);
    file.write(reinterpret_cast<char*>(&lenVar), 4);
    file.close();
  }
  {
    uint32_t lenVar = 1;
    std::ofstream file(test_file3, std::ios::binary);
    file.write(reinterpret_cast<char*>(&lenVar), 4);
    file.write(reinterpret_cast<char*>(&lenVar), 4);
    file.close();
  }
  {
    uint32_t lenVar = 1;
    std::ofstream file(test_file4, std::ios::binary);
    file.write(reinterpret_cast<char*>(&lenVar), 4);
    lenVar = 2;
    file.write(reinterpret_cast<char*>(&lenVar), 4);
    file.write(reinterpret_cast<char*>(&lenVar), 1);
    file.close();
  }

  {
    status = SNAPSHOT_STATUS::IN_PROGRESS;
    ANNSnapshotMapping mapping(test_file1, status, log_context_);
    EXPECT_EQ(status,
              SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_MAPPING_FILE);
  }
  {
    status = SNAPSHOT_STATUS::IN_PROGRESS;
    ANNSnapshotMapping mapping(test_file2, status, log_context_);
    EXPECT_EQ(status,
              SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_MAPPING_FILE);
  }
  {
    status = SNAPSHOT_STATUS::IN_PROGRESS;
    ANNSnapshotMapping mapping(test_file3, status, log_context_);
    EXPECT_EQ(status,
              SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_MAPPING_FILE);
  }
  {
    status = SNAPSHOT_STATUS::IN_PROGRESS;
    ANNSnapshotMapping mapping(test_file4, status, log_context_);
    EXPECT_EQ(status,
              SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_MAPPING_FILE);
  }
}

TEST_F(MappingTest, InvalidLenTest) {
  auto status = SNAPSHOT_STATUS::IN_PROGRESS;
  std::string test_file1 = unique_test_folder_ + "/mapping1";
  std::string test_file2 = unique_test_folder_ + "/mapping2";
  std::string test_file3 = unique_test_folder_ + "/mapping3";
  std::string test_file4 = unique_test_folder_ + "/mapping3";
  uint32_t lenVar;
  {
    std::ofstream file(test_file1, std::ios::binary);
    lenVar = 0;  // trick is here
    file.write(reinterpret_cast<char*>(&lenVar), 4);
    for (const auto& key : url_keys_) {
      lenVar = key.size();
      file.write(reinterpret_cast<char*>(&lenVar), 4);
      file.write(key.c_str(), lenVar);
    }
    file.close();
  }
  {
    std::ofstream file(test_file2, std::ios::binary);
    lenVar = 2000000000 + 1;  // trick is here
    file.write(reinterpret_cast<char*>(&lenVar), 4);
    for (const auto& key : url_keys_) {
      lenVar = key.size();
      file.write(reinterpret_cast<char*>(&lenVar), 4);
      file.write(key.c_str(), lenVar);
    }
    file.close();
  }
  {
    std::ofstream file(test_file3, std::ios::binary);
    lenVar = url_keys_.size();
    file.write(reinterpret_cast<char*>(&lenVar), 4);
    lenVar = 0;
    for (const auto& key : url_keys_) {
      if (lenVar == 0) {
        lenVar = 2000000000 + 1;  // trick is here
      } else {
        lenVar = key.size();
      }
      file.write(reinterpret_cast<char*>(&lenVar), 4);
      file.write(key.c_str(), lenVar);
    }
    file.close();
  }
  {
    std::ofstream file(test_file4, std::ios::binary);
    lenVar = url_keys_.size();
    file.write(reinterpret_cast<char*>(&lenVar), 4);
    lenVar = 0;
    for (const auto& key : url_keys_) {
      if (lenVar == 0) {
        lenVar = 0;  // trick is here
      } else {
        lenVar = key.size();
      }
      file.write(reinterpret_cast<char*>(&lenVar), 4);
      file.write(key.c_str(), lenVar);
      lenVar = 1;  // to make error unrepeatable
    }
    file.close();
  }

  {
    status = SNAPSHOT_STATUS::IN_PROGRESS;
    ANNSnapshotMapping mapping(test_file1, status, log_context_);
    EXPECT_EQ(status,
              SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_MAPPING_FILE);
  }

  {
    status = SNAPSHOT_STATUS::IN_PROGRESS;
    ANNSnapshotMapping mapping(test_file2, status, log_context_);
    EXPECT_EQ(status,
              SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_MAPPING_FILE);
  }

  {
    status = SNAPSHOT_STATUS::IN_PROGRESS;
    ANNSnapshotMapping mapping(test_file3, status, log_context_);
    EXPECT_EQ(status,
              SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_MAPPING_FILE);
  }

  {
    status = SNAPSHOT_STATUS::IN_PROGRESS;
    ANNSnapshotMapping mapping(test_file4, status, log_context_);
    EXPECT_EQ(status,
              SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_MAPPING_FILE);
  }
}

}  // namespace
}  // namespace microsoft
}  // namespace kv_server
