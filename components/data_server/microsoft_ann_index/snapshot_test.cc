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

#include <filesystem>
#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace kv_server {
namespace microsoft {
namespace {

class SnapshotTest : public testing::Test {
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

TEST_F(SnapshotTest, BasicTest) {
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
  std::vector<unsigned char> mappingBytes = {
      10,  0,   0,   0,   36,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,
      98,  97,  110, 110, 101, 114, 115, 46,  99,  111, 109, 47,  114, 101, 110,
      100, 101, 114, 95,  117, 114, 108, 95,  50,  52,  50,  55,  55,  51,  36,
      0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  98,  97,  110, 110, 101,
      114, 115, 46,  99,  111, 109, 47,  114, 101, 110, 100, 101, 114, 95,  117,
      114, 108, 95,  55,  53,  56,  55,  48,  55,  36,  0,   0,   0,   104, 116,
      116, 112, 58,  47,  47,  98,  97,  110, 110, 101, 114, 115, 46,  99,  111,
      109, 47,  114, 101, 110, 100, 101, 114, 95,  117, 114, 108, 95,  53,  53,
      56,  55,  54,  53,  36,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,
      98,  97,  110, 110, 101, 114, 115, 46,  99,  111, 109, 47,  114, 101, 110,
      100, 101, 114, 95,  117, 114, 108, 95,  53,  49,  55,  51,  48,  56,  35,
      0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  98,  97,  110, 110, 101,
      114, 115, 46,  99,  111, 109, 47,  114, 101, 110, 100, 101, 114, 95,  117,
      114, 108, 95,  53,  54,  55,  49,  49,  36,  0,   0,   0,   104, 116, 116,
      112, 58,  47,  47,  98,  97,  110, 110, 101, 114, 115, 46,  99,  111, 109,
      47,  114, 101, 110, 100, 101, 114, 95,  117, 114, 108, 95,  55,  55,  54,
      49,  53,  56,  36,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  98,
      97,  110, 110, 101, 114, 115, 46,  99,  111, 109, 47,  114, 101, 110, 100,
      101, 114, 95,  117, 114, 108, 95,  52,  48,  48,  50,  57,  52,  36,  0,
      0,   0,   104, 116, 116, 112, 58,  47,  47,  98,  97,  110, 110, 101, 114,
      115, 46,  99,  111, 109, 47,  114, 101, 110, 100, 101, 114, 95,  117, 114,
      108, 95,  50,  52,  54,  56,  51,  50,  36,  0,   0,   0,   104, 116, 116,
      112, 58,  47,  47,  98,  97,  110, 110, 101, 114, 115, 46,  99,  111, 109,
      47,  114, 101, 110, 100, 101, 114, 95,  117, 114, 108, 95,  53,  53,  57,
      50,  57,  53,  36,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  98,
      97,  110, 110, 101, 114, 115, 46,  99,  111, 109, 47,  114, 101, 110, 100,
      101, 114, 95,  117, 114, 108, 95,  50,  53,  56,  55,  50,  50};
  DumpFile(default_config_->IndexBaseFilename, indexBytes);
  DumpFile(default_config_->IndexDataFilename, indexDataBytes);
  DumpFile(default_config_->MappingFilename, mappingBytes);

  ANNSnapshot snapshot(default_config_, status);
  // changed to "OK" because everything is successfully initialized
  EXPECT_EQ(status, SNAPSHOT_STATUS::OK);
  {
    // checking mapping is working correctly
    auto& mapping = snapshot.Mapping;
    EXPECT_EQ(mapping.GetStr(0), "http://banners.com/render_url_242773");
    EXPECT_EQ(mapping.GetStr(1), "http://banners.com/render_url_758707");
    EXPECT_EQ(mapping.GetStr(2), "http://banners.com/render_url_558765");
    EXPECT_EQ(mapping.GetStr(3), "http://banners.com/render_url_517308");
    EXPECT_EQ(mapping.GetStr(4), "http://banners.com/render_url_56711");
    EXPECT_EQ(mapping.GetStr(5), "http://banners.com/render_url_776158");
    EXPECT_EQ(mapping.GetStr(6), "http://banners.com/render_url_400294");
    EXPECT_EQ(mapping.GetStr(7), "http://banners.com/render_url_246832");
    EXPECT_EQ(mapping.GetStr(8), "http://banners.com/render_url_559295");
    EXPECT_EQ(mapping.GetStr(9), "http://banners.com/render_url_258722");
    EXPECT_EQ(mapping.GetStr(10), "");  // unknown key
  }
  {
    // checking selections are working correctly
    absl::flat_hash_set<std::string_view> key_set = {};
    key_set.insert(std::string_view("0123456789ABCDEF"));
    std::vector<std::pair<uint32_t, uint32_t>> stats(key_set.size(),
                                                     std::make_pair(0, 0));
    std::vector<std::vector<uint32_t>> results(
        key_set.size(), std::vector<uint32_t>(default_config_->TopCount,
                                              snapshot.Mapping.GetCapacity()));
    snapshot.Index.Search(key_set, results, stats);
    ASSERT_EQ(results.size(), key_set.size());
    ASSERT_EQ(results[0].size(), default_config_->TopCount);
    {
      std::vector<std::vector<uint32_t>> desiredResults = {{9, 6, 0, 8}};
      for (auto i = 0; i < results.size(); i++) {
        for (auto j = 0; j < results[i].size(); j++) {
          EXPECT_EQ(results[i][j], desiredResults[i][j]);
        }
      }
    }
  }
}

TEST_F(SnapshotTest, GetSnapshotNameTest) {
  default_config_->SnapshotName = "randomNameToCheck";
  auto status = SNAPSHOT_STATUS::IN_PROGRESS;
  ANNSnapshot snapshot(default_config_, status);
  EXPECT_EQ(snapshot.GetSnapshotName(), "randomNameToCheck");
}

TEST_F(SnapshotTest, TotalFailTest) {
  auto status = SNAPSHOT_STATUS::IN_PROGRESS;
  ANNSnapshot snapshot(default_config_, status);

  EXPECT_NE(status, SNAPSHOT_STATUS::OK);
  EXPECT_NE(status, SNAPSHOT_STATUS::IN_PROGRESS);
}

TEST_F(SnapshotTest, InternalIndexFailTest) {
  auto status = SNAPSHOT_STATUS::IN_PROGRESS;
  std::vector<unsigned char> mappingBytes = {
      10,  0,   0,   0,   36,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,
      98,  97,  110, 110, 101, 114, 115, 46,  99,  111, 109, 47,  114, 101, 110,
      100, 101, 114, 95,  117, 114, 108, 95,  50,  52,  50,  55,  55,  51,  36,
      0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  98,  97,  110, 110, 101,
      114, 115, 46,  99,  111, 109, 47,  114, 101, 110, 100, 101, 114, 95,  117,
      114, 108, 95,  55,  53,  56,  55,  48,  55,  36,  0,   0,   0,   104, 116,
      116, 112, 58,  47,  47,  98,  97,  110, 110, 101, 114, 115, 46,  99,  111,
      109, 47,  114, 101, 110, 100, 101, 114, 95,  117, 114, 108, 95,  53,  53,
      56,  55,  54,  53,  36,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,
      98,  97,  110, 110, 101, 114, 115, 46,  99,  111, 109, 47,  114, 101, 110,
      100, 101, 114, 95,  117, 114, 108, 95,  53,  49,  55,  51,  48,  56,  35,
      0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  98,  97,  110, 110, 101,
      114, 115, 46,  99,  111, 109, 47,  114, 101, 110, 100, 101, 114, 95,  117,
      114, 108, 95,  53,  54,  55,  49,  49,  36,  0,   0,   0,   104, 116, 116,
      112, 58,  47,  47,  98,  97,  110, 110, 101, 114, 115, 46,  99,  111, 109,
      47,  114, 101, 110, 100, 101, 114, 95,  117, 114, 108, 95,  55,  55,  54,
      49,  53,  56,  36,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  98,
      97,  110, 110, 101, 114, 115, 46,  99,  111, 109, 47,  114, 101, 110, 100,
      101, 114, 95,  117, 114, 108, 95,  52,  48,  48,  50,  57,  52,  36,  0,
      0,   0,   104, 116, 116, 112, 58,  47,  47,  98,  97,  110, 110, 101, 114,
      115, 46,  99,  111, 109, 47,  114, 101, 110, 100, 101, 114, 95,  117, 114,
      108, 95,  50,  52,  54,  56,  51,  50,  36,  0,   0,   0,   104, 116, 116,
      112, 58,  47,  47,  98,  97,  110, 110, 101, 114, 115, 46,  99,  111, 109,
      47,  114, 101, 110, 100, 101, 114, 95,  117, 114, 108, 95,  53,  53,  57,
      50,  57,  53,  36,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  98,
      97,  110, 110, 101, 114, 115, 46,  99,  111, 109, 47,  114, 101, 110, 100,
      101, 114, 95,  117, 114, 108, 95,  50,  53,  56,  55,  50,  50};
  DumpFile(default_config_->MappingFilename, mappingBytes);

  ANNSnapshot snapshot(default_config_, status);
  EXPECT_EQ(status, SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_INDEX);
}

TEST_F(SnapshotTest, InternalMappingFailTest) {
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

  ANNSnapshot snapshot(default_config_, status);
  EXPECT_EQ(status, SNAPSHOT_STATUS::SNAPSHOT_LOAD_ERROR_INVALID_MAPPING_FILE);
}

}  // namespace
}  // namespace microsoft
}  // namespace kv_server
