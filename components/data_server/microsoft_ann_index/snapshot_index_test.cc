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

#include "components/data_server/microsoft_ann_index/snapshot_index.h"

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace kv_server {
namespace microsoft {
namespace {

class IndexTest : public testing::Test {
 protected:
  void SetUp() override {
    unique_test_folder_ = "Test_Folder_" + rand_string();
    std::filesystem::create_directory(unique_test_folder_);
    default_config_ = std::make_shared<ANNSnapshotConfig>(log_context_);
    default_config_->Dimension = 16;
    default_config_->QueryNeighborsCount = 8;
    default_config_->TopCount = 4;
    default_config_->VectorTypeStr = "uint8";
    default_config_->SnapshotFolder = unique_test_folder_;
    default_config_->IndexBaseFilename = unique_test_folder_ + "/index";
    default_config_->IndexDataFilename = unique_test_folder_ + "/index.data";
    default_config_->MappingFilename = unique_test_folder_ + "/mapping";
  }

  void TearDown() override { std::filesystem::remove_all(unique_test_folder_); }

  void DumpFile(const std::string& filename,
                const std::vector<unsigned char>& bytes) {
    std::ofstream file(filename, std::ios::binary);
    file.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    file.close();
  }

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
  std::shared_ptr<ANNSnapshotConfig> default_config_;
};

TEST_F(IndexTest, BaseTest) {
  auto status = SNAPSHOT_STATUS::IN_PROGRESS;
  std::vector<unsigned char> indexBytes = {
      224, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0,   7, 0, 0, 0, 7, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 5, 0, 0, 0, 6, 0,
      0,   0, 8, 0, 0, 0, 9, 0, 0, 0, 4, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 6,
      0,   0, 0, 9, 0, 0, 0, 3, 0, 0, 0, 7, 0, 0, 0, 3, 0, 0, 0, 8, 0, 0, 0,
      5,   0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0,
      0,   3, 0, 0, 0, 7, 0, 0, 0, 3, 0, 0, 0, 9, 0, 0, 0, 3, 0, 0, 0, 7, 0,
      0,   0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 3,
      0,   0, 0, 9, 0, 0, 0, 5, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0,
      2,   0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0,
      0,   0, 0, 0, 0, 1, 0, 0, 0, 6, 0, 0, 0, 4, 0, 0, 0};
  std::vector<unsigned char> indexDataBytes = {
      10,  0,   0,   0,   16,  0,   0,   0,   210, 36,  200, 100, 86,  96,
      167, 107, 106, 144, 88,  82,  182, 98,  84,  131, 148, 190, 45,  108,
      215, 40,  189, 150, 52,  181, 213, 57,  116, 247, 4,   203, 175, 198,
      110, 50,  252, 104, 174, 248, 193, 162, 45,  131, 46,  220, 206, 255,
      165, 140, 63,  2,   77,  201, 148, 246, 62,  168, 21,  194, 245, 100,
      237, 21,  32,  230, 49,  173, 75,  188, 226, 109, 211, 126, 163, 73,
      179, 218, 126, 36,  61,  226, 114, 107, 59,  100, 127, 135, 216, 162,
      247, 236, 199, 85,  118, 233, 191, 189, 4,   47,  48,  10,  66,  225,
      53,  98,  138, 58,  78,  65,  56,  7,   36,  219, 101, 135, 145, 74,
      207, 207, 129, 222, 124, 96,  157, 238, 34,  197, 101, 87,  232, 253,
      232, 29,  51,  85,  60,  86,  14,  175, 69,  162, 203, 97,  175, 60,
      4,   109, 9,   85,  123, 38,  65,  83,  41,  89,  136, 190, 3,   176};
  DumpFile(default_config_->IndexBaseFilename, indexBytes);
  DumpFile(default_config_->IndexDataFilename, indexDataBytes);

  ANNSnapshotIndex index(default_config_, status);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);
  {
    // checking selections are working correctly
    absl::flat_hash_set<std::string_view> key_set = {};
    key_set.insert(std::string_view("0123456789ABCDEF"));
    key_set.insert(std::string_view("aaaaaaaaaaaaaaaa"));
    key_set.insert(std::string_view("asdfhuohasdif320"));
    std::vector<std::pair<uint32_t, uint32_t>> stats(key_set.size(),
                                                     std::make_pair(0, 0));
    std::vector<std::vector<uint32_t>> results(
        key_set.size(), std::vector<uint32_t>(default_config_->TopCount, 10));
    index.Search(key_set, results, stats);
    ASSERT_EQ(results.size(), key_set.size());
    ASSERT_EQ(results[0].size(), default_config_->TopCount);
    ASSERT_EQ(results[1].size(), default_config_->TopCount);
    ASSERT_EQ(results[2].size(), default_config_->TopCount);
    {
      size_t ind = 0;
      for (auto key : key_set) {
        if (key == "0123456789ABCDEF") {
          EXPECT_EQ(results[ind][0], 9);
          EXPECT_EQ(results[ind][1], 6);
          EXPECT_EQ(results[ind][2], 0);
          EXPECT_EQ(results[ind][3], 8);
        } else if (key == "aaaaaaaaaaaaaaaa") {
          EXPECT_EQ(results[ind][0], 0);
          EXPECT_EQ(results[ind][1], 9);
          EXPECT_EQ(results[ind][2], 6);
          EXPECT_EQ(results[ind][3], 8);
        } else if (key == "asdfhuohasdif320") {
          EXPECT_EQ(results[ind][0], 0);
          EXPECT_EQ(results[ind][1], 6);
          EXPECT_EQ(results[ind][2], 9);
          EXPECT_EQ(results[ind][3], 4);
        } else {
          FAIL() << "Unknown key: " << key;
        }
        ++ind;
      }
    }
  }
}

TEST_F(IndexTest, SkippedKeysTest) {
  auto status = SNAPSHOT_STATUS::IN_PROGRESS;
  default_config_->Dimension = 4;
  default_config_->VectorTypeStr = "uint8";
  std::vector<unsigned char> indexBytes = {
      184, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0,   3, 0, 0, 0, 4, 0, 0, 0, 7, 0, 0, 0, 9, 0, 0, 0, 2, 0, 0, 0, 4, 0,
      0,   0, 3, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 8, 0, 0, 0, 4,
      0,   0, 0, 1, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 9, 0, 0, 0, 6, 0, 0, 0,
      1,   0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 6, 0, 0, 0, 7, 0, 0, 0, 8, 0, 0,
      0,   2, 0, 0, 0, 3, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 3, 0,
      0,   0, 7, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 2,
      0,   0, 0, 2, 0, 0, 0, 4, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0,
  };
  std::vector<unsigned char> indexDataBytes = {
      10,  0,   0,   0,   4,   0,  0,   0,   117, 174, 76,  229,
      254, 136, 38,  109, 236, 31, 19,  201, 222, 183, 133, 32,
      220, 139, 87,  191, 132, 65, 161, 11,  249, 227, 198, 166,
      229, 216, 101, 207, 178, 57, 35,  247, 86,  236, 92,  147,
  };
  DumpFile(default_config_->IndexBaseFilename, indexBytes);
  DumpFile(default_config_->IndexDataFilename, indexDataBytes);

  ANNSnapshotIndex index(default_config_, status);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);
  {
    // checking selections are working correctly
    absl::flat_hash_set<std::string_view> key_set = {};
    key_set.insert(std::string_view("1234"));
    key_set.insert(std::string_view(""));
    key_set.insert(std::string_view("1"));
    key_set.insert(std::string_view("22"));
    key_set.insert(std::string_view("333"));
    key_set.insert(std::string_view("AAAA"));
    key_set.insert(std::string_view("666666"));
    key_set.insert(std::string_view("1k0k"));
    key_set.insert(std::string_view("55555"));
    std::vector<std::pair<uint32_t, uint32_t>> stats(key_set.size(),
                                                     std::make_pair(0, 0));
    std::vector<std::vector<uint32_t>> results(
        key_set.size(), std::vector<uint32_t>(default_config_->TopCount, 10));
    index.Search(key_set, results, stats);
    ASSERT_EQ(results.size(), key_set.size());
    for (const auto& result : results) {
      ASSERT_EQ(result.size(), default_config_->TopCount);
    }
    {
      size_t ind = 0;
      for (auto key : key_set) {
        if (key == "1234") {
          EXPECT_EQ(results[ind][0], 5);
          EXPECT_EQ(results[ind][1], 9);
          EXPECT_EQ(results[ind][2], 0);
          EXPECT_EQ(results[ind][3], 1);
        } else if (key == "AAAA") {
          EXPECT_EQ(results[ind][0], 5);
          EXPECT_EQ(results[ind][1], 9);
          EXPECT_EQ(results[ind][2], 0);
          EXPECT_EQ(results[ind][3], 1);
        } else if (key == "1k0k") {
          EXPECT_EQ(results[ind][0], 9);
          EXPECT_EQ(results[ind][1], 0);
          EXPECT_EQ(results[ind][2], 5);
          EXPECT_EQ(results[ind][3], 4);
        } else if (key.size() != 4) {
          EXPECT_EQ(results[ind][0], 10);
          EXPECT_EQ(results[ind][1], 10);
          EXPECT_EQ(results[ind][2], 10);
          EXPECT_EQ(results[ind][3], 10);
        } else {
          FAIL() << "Unknown key: " << key;
        }
        ++ind;
      }
    }
  }
}

TEST_F(IndexTest, Uint8Test) {
  auto status = SNAPSHOT_STATUS::IN_PROGRESS;
  default_config_->Dimension = 4;
  default_config_->VectorTypeStr = "uint8";
  std::vector<unsigned char> indexBytes = {
      184, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0,   3, 0, 0, 0, 4, 0, 0, 0, 7, 0, 0, 0, 9, 0, 0, 0, 2, 0, 0, 0, 4, 0,
      0,   0, 3, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 8, 0, 0, 0, 4,
      0,   0, 0, 1, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 9, 0, 0, 0, 6, 0, 0, 0,
      1,   0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 6, 0, 0, 0, 7, 0, 0, 0, 8, 0, 0,
      0,   2, 0, 0, 0, 3, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 3, 0,
      0,   0, 7, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 2,
      0,   0, 0, 2, 0, 0, 0, 4, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0,
  };
  std::vector<unsigned char> indexDataBytes = {
      10,  0,   0,   0,   4,   0,  0,   0,   117, 174, 76,  229,
      254, 136, 38,  109, 236, 31, 19,  201, 222, 183, 133, 32,
      220, 139, 87,  191, 132, 65, 161, 11,  249, 227, 198, 166,
      229, 216, 101, 207, 178, 57, 35,  247, 86,  236, 92,  147,
  };
  DumpFile(default_config_->IndexBaseFilename, indexBytes);
  DumpFile(default_config_->IndexDataFilename, indexDataBytes);

  ANNSnapshotIndex index(default_config_, status);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);
  {
    // checking selections are working correctly
    absl::flat_hash_set<std::string_view> key_set = {};
    key_set.insert(std::string_view("1234"));
    key_set.insert(std::string_view("AAAA"));
    key_set.insert(std::string_view("1k0k"));
    std::vector<std::pair<uint32_t, uint32_t>> stats(key_set.size(),
                                                     std::make_pair(0, 0));
    std::vector<std::vector<uint32_t>> results(
        key_set.size(), std::vector<uint32_t>(default_config_->TopCount, 10));
    index.Search(key_set, results, stats);
    ASSERT_EQ(results.size(), key_set.size());
    ASSERT_EQ(results[0].size(), default_config_->TopCount);
    ASSERT_EQ(results[1].size(), default_config_->TopCount);
    ASSERT_EQ(results[2].size(), default_config_->TopCount);
    {
      size_t ind = 0;
      for (auto key : key_set) {
        if (key == "1234") {
          EXPECT_EQ(results[ind][0], 5);
          EXPECT_EQ(results[ind][1], 9);
          EXPECT_EQ(results[ind][2], 0);
          EXPECT_EQ(results[ind][3], 1);
        } else if (key == "AAAA") {
          EXPECT_EQ(results[ind][0], 5);
          EXPECT_EQ(results[ind][1], 9);
          EXPECT_EQ(results[ind][2], 0);
          EXPECT_EQ(results[ind][3], 1);
        } else if (key == "1k0k") {
          EXPECT_EQ(results[ind][0], 9);
          EXPECT_EQ(results[ind][1], 0);
          EXPECT_EQ(results[ind][2], 5);
          EXPECT_EQ(results[ind][3], 4);
        } else {
          FAIL() << "Unknown key: " << key;
        }
        ++ind;
      }
    }
  }
}

TEST_F(IndexTest, Int8Test) {
  auto status = SNAPSHOT_STATUS::IN_PROGRESS;
  default_config_->Dimension = 4;
  default_config_->VectorTypeStr = "int8";
  std::vector<unsigned char> indexBytes = {
      248, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0,   5, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 6, 0,
      0,   0, 5, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 7,
      0,   0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0,
      5,   0, 0, 0, 6, 0, 0, 0, 6, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0,
      0,   5, 0, 0, 0, 6, 0, 0, 0, 9, 0, 0, 0, 5, 0, 0, 0, 2, 0, 0, 0, 0, 0,
      0,   0, 1, 0, 0, 0, 7, 0, 0, 0, 8, 0, 0, 0, 4, 0, 0, 0, 2, 0, 0, 0, 3,
      0,   0, 0, 7, 0, 0, 0, 8, 0, 0, 0, 5, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
      3,   0, 0, 0, 8, 0, 0, 0, 9, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 4, 0, 0,
      0,   1, 0, 0, 0, 9, 0, 0, 0, 3, 0, 0, 0, 6, 0, 0, 0, 4, 0, 0, 0, 5, 0,
      0,   0, 3, 0, 0, 0, 6, 0, 0, 0, 3, 0, 0, 0, 7, 0, 0, 0,
  };
  std::vector<unsigned char> indexDataBytes = {
      10,  0,   0,   0,   4,   0,   0,   0,   125, 129, 100, 208,
      244, 78,  143, 217, 9,   197, 254, 111, 229, 121, 246, 210,
      75,  246, 108, 76,  4,   115, 8,   116, 177, 186, 40,  71,
      36,  93,  82,  90,  180, 14,  97,  99,  147, 5,   247, 38,
  };
  DumpFile(default_config_->IndexBaseFilename, indexBytes);
  DumpFile(default_config_->IndexDataFilename, indexDataBytes);

  ANNSnapshotIndex index(default_config_, status);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);
  {
    // checking selections are working correctly
    absl::flat_hash_set<std::string_view> key_set = {};
    key_set.insert(std::string_view("1234"));
    key_set.insert(std::string_view("AAAA"));
    key_set.insert(std::string_view("1k0k"));
    std::vector<std::pair<uint32_t, uint32_t>> stats(key_set.size(),
                                                     std::make_pair(0, 0));
    std::vector<std::vector<uint32_t>> results(
        key_set.size(), std::vector<uint32_t>(default_config_->TopCount, 10));
    index.Search(key_set, results, stats);
    ASSERT_EQ(results.size(), key_set.size());
    ASSERT_EQ(results[0].size(), default_config_->TopCount);
    ASSERT_EQ(results[1].size(), default_config_->TopCount);
    ASSERT_EQ(results[2].size(), default_config_->TopCount);
    {
      size_t ind = 0;
      for (auto key : key_set) {
        if (key == "1234") {
          EXPECT_EQ(results[ind][0], 7);
          EXPECT_EQ(results[ind][1], 4);
          EXPECT_EQ(results[ind][2], 5);
          EXPECT_EQ(results[ind][3], 2);
        } else if (key == "AAAA") {
          EXPECT_EQ(results[ind][0], 7);
          EXPECT_EQ(results[ind][1], 4);
          EXPECT_EQ(results[ind][2], 5);
          EXPECT_EQ(results[ind][3], 8);
        } else if (key == "1k0k") {
          EXPECT_EQ(results[ind][0], 7);
          EXPECT_EQ(results[ind][1], 5);
          EXPECT_EQ(results[ind][2], 4);
          EXPECT_EQ(results[ind][3], 8);
        } else {
          FAIL() << "Unknown key: " << key;
        }
        ++ind;
      }
    }
  }
}

TEST_F(IndexTest, FloatTest) {
  auto status = SNAPSHOT_STATUS::IN_PROGRESS;
  default_config_->Dimension = 4;
  default_config_->VectorTypeStr = "float";
  std::vector<unsigned char> indexBytes = {
      56, 1, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      9,  0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0,
      6,  0, 0, 0, 7, 0, 0, 0, 8, 0, 0, 0, 9, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
      8,  0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0,
      7,  0, 0, 0, 8, 0, 0, 0, 9, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0,
      4,  0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 7, 0, 0, 0, 8, 0, 0, 0, 9, 0, 0, 0,
      8,  0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0,
      7,  0, 0, 0, 8, 0, 0, 0, 9, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0,
      3,  0, 0, 0, 4, 0, 0, 0, 6, 0, 0, 0, 5, 0, 0, 0, 5, 0, 0, 0, 2, 0, 0, 0,
      3,  0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0,
      3,  0, 0, 0, 4, 0, 0, 0, 8, 0, 0, 0, 9, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0,
      2,  0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 7, 0, 0, 0, 9, 0, 0, 0, 6, 0, 0, 0,
      0,  0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 7, 0, 0, 0, 8, 0, 0, 0,
  };
  std::vector<unsigned char> indexDataBytes = {
      10,  0,   0,   0,   4,   0,   0,   0,   141, 108, 126, 5,   147, 104,
      189, 93,  184, 250, 182, 91,  174, 37,  213, 59,  7,   207, 79,  17,
      72,  209, 62,  95,  129, 21,  58,  43,  36,  76,  142, 208, 74,  71,
      178, 102, 127, 151, 197, 100, 57,  133, 42,  221, 53,  21,  42,  97,
      171, 28,  45,  126, 42,  79,  137, 145, 7,   254, 196, 52,  101, 66,
      148, 150, 253, 127, 193, 110, 170, 150, 66,  147, 137, 203, 172, 253,
      186, 205, 22,  105, 139, 95,  185, 53,  172, 28,  136, 90,  63,  165,
      245, 17,  8,   188, 130, 12,  92,  235, 107, 80,  50,  237, 12,  32,
      238, 54,  195, 67,  41,  153, 26,  192, 255, 214, 143, 147, 162, 16,
      65,  126, 48,  200, 47,  5,   39,  13,  153, 131, 152, 151, 53,  31,
      112, 87,  188, 115, 26,  189, 80,  114, 83,  84,  114, 36,  85,  227,
      48,  243, 0,   247, 143, 18,  92,  142, 87,  66,  227, 142, 111, 148,
  };
  DumpFile(default_config_->IndexBaseFilename, indexBytes);
  DumpFile(default_config_->IndexDataFilename, indexDataBytes);

  ANNSnapshotIndex index(default_config_, status);
  EXPECT_EQ(status, SNAPSHOT_STATUS::IN_PROGRESS);
  {
    // checking selections are working correctly
    absl::flat_hash_set<std::string_view> key_set = {};
    key_set.insert(std::string_view("1111222233334444"));
    key_set.insert(std::string_view("AAAAAAAAAAAAAAAA"));
    key_set.insert(std::string_view("fjadf9jafjdas98j"));
    std::vector<std::pair<uint32_t, uint32_t>> stats(key_set.size(),
                                                     std::make_pair(0, 0));
    std::vector<std::vector<uint32_t>> results(
        key_set.size(), std::vector<uint32_t>(default_config_->TopCount, 10));
    index.Search(key_set, results, stats);
    ASSERT_EQ(results.size(), key_set.size());
    ASSERT_EQ(results[0].size(), default_config_->TopCount);
    ASSERT_EQ(results[1].size(), default_config_->TopCount);
    ASSERT_EQ(results[2].size(), default_config_->TopCount);
    {
      size_t ind = 0;
      for (auto key : key_set) {
        if (key == "1111222233334444") {
          EXPECT_EQ(results[ind][0], 6);
          EXPECT_EQ(results[ind][1], 5);
          EXPECT_EQ(results[ind][2], 0);
          EXPECT_EQ(results[ind][3], 1);
        } else if (key == "AAAAAAAAAAAAAAAA") {
          EXPECT_EQ(results[ind][0], 6);
          EXPECT_EQ(results[ind][1], 5);
          EXPECT_EQ(results[ind][2], 0);
          EXPECT_EQ(results[ind][3], 1);
        } else if (key == "fjadf9jafjdas98j") {
          EXPECT_EQ(results[ind][0], 0);
          EXPECT_EQ(results[ind][1], 1);
          EXPECT_EQ(results[ind][2], 2);
          EXPECT_EQ(results[ind][3], 3);
        } else {
          FAIL() << "Unknown key: " << key;
        }
        ++ind;
      }
    }
  }
}

TEST_F(IndexTest, EmptyIndexTest) {
  auto status = SNAPSHOT_STATUS::IN_PROGRESS;
  std::vector<unsigned char> indexBytes = {};
  std::vector<unsigned char> indexDataBytes = {};
  DumpFile(default_config_->IndexBaseFilename, indexBytes);
  DumpFile(default_config_->IndexDataFilename, indexDataBytes);

  ANNSnapshotIndex index(default_config_, status);
  EXPECT_EQ(status, SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_INDEX);
}

TEST_F(IndexTest, InvalidIndexTest) {
  auto status = SNAPSHOT_STATUS::IN_PROGRESS;
  std::vector<unsigned char> indexBytes = {
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  std::vector<unsigned char> indexDataBytes = {
      10,  0,   0,   0,   16,  0,   0,   0,   210, 36,  200, 100, 86,  96,
      167, 107, 106, 144, 88,  82,  182, 98,  84,  131, 148, 190, 45,  108,
      215, 40,  189, 150, 52,  181, 213, 57,  116, 247, 4,   203, 175, 198,
      110, 50,  252, 104, 174, 248, 193, 162, 45,  131, 46,  220, 206, 255,
      165, 140, 63,  2,   77,  201, 148, 246, 62,  168, 21,  194, 245, 100,
      237, 21,  32,  230, 49,  173, 75,  188, 226, 109, 211, 126, 163, 73,
      179, 218, 126, 36,  61,  226, 114, 107, 59,  100, 127, 135, 216, 162,
      247, 236, 199, 85,  118, 233, 191, 189, 4,   47,  48,  10,  66,  225,
      53,  98,  138, 58,  78,  65,  56,  7,   36,  219, 101, 135, 145, 74,
      207, 207, 129, 222, 124, 96,  157, 238, 34,  197, 101, 87,  232, 253,
      232, 29,  51,  85,  60,  86,  14,  175, 69,  162, 203, 97,  175, 60,
      4,   109, 9,   85,  123, 38,  65,  83,  41,  89,  136, 190, 3,   176};
  DumpFile(default_config_->IndexBaseFilename, indexBytes);
  DumpFile(default_config_->IndexDataFilename, indexDataBytes);

  ANNSnapshotIndex index(default_config_, status);
  EXPECT_EQ(status, SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_INDEX);
}

TEST_F(IndexTest, InvalidIndexDataTest) {
  auto status = SNAPSHOT_STATUS::IN_PROGRESS;
  std::vector<unsigned char> indexBytes = {
      224, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0,   7, 0, 0, 0, 7, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 5, 0, 0, 0, 6, 0,
      0,   0, 8, 0, 0, 0, 9, 0, 0, 0, 4, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 6,
      0,   0, 0, 9, 0, 0, 0, 3, 0, 0, 0, 7, 0, 0, 0, 3, 0, 0, 0, 8, 0, 0, 0,
      5,   0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0,
      0,   3, 0, 0, 0, 7, 0, 0, 0, 3, 0, 0, 0, 9, 0, 0, 0, 3, 0, 0, 0, 7, 0,
      0,   0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 3,
      0,   0, 0, 9, 0, 0, 0, 5, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0,
      2,   0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0,
      0,   0, 0, 0, 0, 1, 0, 0, 0, 6, 0, 0, 0, 4, 0, 0, 0};
  std::vector<unsigned char> indexDataBytes = {
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  DumpFile(default_config_->IndexBaseFilename, indexBytes);
  DumpFile(default_config_->IndexDataFilename, indexDataBytes);

  ANNSnapshotIndex index(default_config_, status);
  EXPECT_EQ(status, SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_INDEX);
}

}  // namespace
}  // namespace microsoft
}  // namespace kv_server
