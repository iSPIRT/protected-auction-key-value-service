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

#include "components/data_server/microsoft_ann_index/index.h"

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "components/telemetry/server_definition.h"
#include "gtest/gtest.h"

namespace kv_server {
namespace microsoft {
namespace {

class ANNIndexTest : public testing::Test {
 protected:
  void SetUp() override {
    unique_test_folder_ = "Test_Folder_" + rand_string();
    std::filesystem::create_directory(unique_test_folder_);
    std::vector<unsigned char> snapshotBytes = {
        237, 254, 13,  240, 4,   0,   0,   0,   5,   0,   0,   0,   105, 110,
        100, 101, 120, 80,  2,   0,   0,   0,   0,   0,   0,   80,  2,   0,
        0,   0,   0,   0,   0,   9,   0,   0,   0,   13,  0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   6,   0,   0,   0,   13,  0,   0,
        0,   5,   0,   0,   0,   9,   0,   0,   0,   12,  0,   0,   0,   17,
        0,   0,   0,   19,  0,   0,   0,   6,   0,   0,   0,   13,  0,   0,
        0,   2,   0,   0,   0,   4,   0,   0,   0,   6,   0,   0,   0,   11,
        0,   0,   0,   18,  0,   0,   0,   8,   0,   0,   0,   1,   0,   0,
        0,   3,   0,   0,   0,   4,   0,   0,   0,   6,   0,   0,   0,   7,
        0,   0,   0,   11,  0,   0,   0,   14,  0,   0,   0,   15,  0,   0,
        0,   6,   0,   0,   0,   13,  0,   0,   0,   2,   0,   0,   0,   7,
        0,   0,   0,   11,  0,   0,   0,   14,  0,   0,   0,   17,  0,   0,
        0,   7,   0,   0,   0,   13,  0,   0,   0,   2,   0,   0,   0,   1,
        0,   0,   0,   5,   0,   0,   0,   12,  0,   0,   0,   16,  0,   0,
        0,   19,  0,   0,   0,   6,   0,   0,   0,   4,   0,   0,   0,   0,
        0,   0,   0,   6,   0,   0,   0,   10,  0,   0,   0,   18,  0,   0,
        0,   19,  0,   0,   0,   6,   0,   0,   0,   5,   0,   0,   0,   1,
        0,   0,   0,   2,   0,   0,   0,   9,   0,   0,   0,   11,  0,   0,
        0,   16,  0,   0,   0,   4,   0,   0,   0,   2,   0,   0,   0,   3,
        0,   0,   0,   9,   0,   0,   0,   14,  0,   0,   0,   4,   0,   0,
        0,   13,  0,   0,   0,   9,   0,   0,   0,   12,  0,   0,   0,   19,
        0,   0,   0,   9,   0,   0,   0,   8,   0,   0,   0,   7,   0,   0,
        0,   0,   0,   0,   0,   6,   0,   0,   0,   10,  0,   0,   0,   11,
        0,   0,   0,   14,  0,   0,   0,   17,  0,   0,   0,   18,  0,   0,
        0,   7,   0,   0,   0,   5,   0,   0,   0,   13,  0,   0,   0,   9,
        0,   0,   0,   11,  0,   0,   0,   14,  0,   0,   0,   15,  0,   0,
        0,   16,  0,   0,   0,   9,   0,   0,   0,   1,   0,   0,   0,   2,
        0,   0,   0,   6,   0,   0,   0,   9,   0,   0,   0,   3,   0,   0,
        0,   10,  0,   0,   0,   12,  0,   0,   0,   15,  0,   0,   0,   16,
        0,   0,   0,   6,   0,   0,   0,   11,  0,   0,   0,   0,   0,   0,
        0,   4,   0,   0,   0,   8,   0,   0,   0,   15,  0,   0,   0,   17,
        0,   0,   0,   7,   0,   0,   0,   4,   0,   0,   0,   1,   0,   0,
        0,   3,   0,   0,   0,   0,   0,   0,   0,   10,  0,   0,   0,   8,
        0,   0,   0,   17,  0,   0,   0,   7,   0,   0,   0,   3,   0,   0,
        0,   9,   0,   0,   0,   2,   0,   0,   0,   7,   0,   0,   0,   10,
        0,   0,   0,   15,  0,   0,   0,   17,  0,   0,   0,   5,   0,   0,
        0,   11,  0,   0,   0,   14,  0,   0,   0,   12,  0,   0,   0,   2,
        0,   0,   0,   10,  0,   0,   0,   4,   0,   0,   0,   11,  0,   0,
        0,   4,   0,   0,   0,   10,  0,   0,   0,   6,   0,   0,   0,   7,
        0,   0,   0,   0,   0,   0,   0,   12,  0,   0,   0,   9,   0,   0,
        0,   3,   0,   0,   0,   13,  0,   0,   0,   14,  0,   0,   0,   18,
        0,   0,   0,   4,   0,   0,   0,   17,  0,   0,   0,   9,   0,   0,
        0,   1,   0,   0,   0,   5,   0,   0,   0,   4,   0,   0,   0,   4,
        0,   0,   0,   0,   0,   0,   0,   5,   0,   0,   0,   8,   0,   0,
        0,   10,  0,   0,   0,   105, 110, 100, 101, 120, 46,  100, 97,  116,
        97,  168, 0,   0,   0,   0,   0,   0,   0,   20,  0,   0,   0,   8,
        0,   0,   0,   242, 6,   59,  90,  153, 146, 158, 226, 163, 161, 179,
        208, 220, 222, 207, 56,  152, 201, 198, 174, 38,  80,  232, 0,   182,
        162, 190, 251, 189, 8,   42,  112, 127, 172, 87,  150, 243, 74,  216,
        52,  55,  200, 65,  80,  90,  0,   236, 175, 17,  224, 54,  90,  89,
        130, 62,  32,  247, 239, 253, 161, 31,  110, 91,  19,  228, 168, 240,
        20,  255, 188, 6,   212, 226, 211, 115, 76,  37,  210, 33,  197, 41,
        81,  157, 141, 102, 40,  181, 245, 136, 119, 205, 169, 116, 223, 152,
        48,  208, 23,  79,  108, 179, 186, 126, 36,  203, 143, 148, 182, 237,
        83,  193, 153, 245, 167, 71,  244, 3,   11,  87,  149, 160, 23,  179,
        138, 15,  109, 107, 1,   14,  73,  133, 194, 179, 174, 233, 51,  219,
        5,   78,  148, 134, 188, 92,  184, 224, 181, 46,  152, 107, 253, 223,
        182, 214, 119, 81,  9,   224, 86,  249, 130, 7,   0,   0,   0,   109,
        97,  112, 112, 105, 110, 103, 36,  3,   0,   0,   0,   0,   0,   0,
        20,  0,   0,   0,   36,  0,   0,   0,   104, 116, 116, 112, 58,  47,
        47,  98,  97,  110, 110, 101, 114, 115, 46,  99,  111, 109, 47,  114,
        101, 110, 100, 101, 114, 95,  117, 114, 108, 95,  55,  55,  51,  51,
        49,  54,  36,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  98,
        97,  110, 110, 101, 114, 115, 46,  99,  111, 109, 47,  114, 101, 110,
        100, 101, 114, 95,  117, 114, 108, 95,  55,  48,  56,  49,  48,  52,
        36,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  98,  97,  110,
        110, 101, 114, 115, 46,  99,  111, 109, 47,  114, 101, 110, 100, 101,
        114, 95,  117, 114, 108, 95,  57,  50,  52,  53,  54,  57,  36,  0,
        0,   0,   104, 116, 116, 112, 58,  47,  47,  98,  97,  110, 110, 101,
        114, 115, 46,  99,  111, 109, 47,  114, 101, 110, 100, 101, 114, 95,
        117, 114, 108, 95,  54,  52,  50,  50,  52,  54,  36,  0,   0,   0,
        104, 116, 116, 112, 58,  47,  47,  98,  97,  110, 110, 101, 114, 115,
        46,  99,  111, 109, 47,  114, 101, 110, 100, 101, 114, 95,  117, 114,
        108, 95,  56,  48,  51,  57,  54,  53,  36,  0,   0,   0,   104, 116,
        116, 112, 58,  47,  47,  98,  97,  110, 110, 101, 114, 115, 46,  99,
        111, 109, 47,  114, 101, 110, 100, 101, 114, 95,  117, 114, 108, 95,
        55,  54,  51,  57,  53,  49,  36,  0,   0,   0,   104, 116, 116, 112,
        58,  47,  47,  98,  97,  110, 110, 101, 114, 115, 46,  99,  111, 109,
        47,  114, 101, 110, 100, 101, 114, 95,  117, 114, 108, 95,  57,  48,
        51,  55,  48,  56,  36,  0,   0,   0,   104, 116, 116, 112, 58,  47,
        47,  98,  97,  110, 110, 101, 114, 115, 46,  99,  111, 109, 47,  114,
        101, 110, 100, 101, 114, 95,  117, 114, 108, 95,  52,  48,  53,  53,
        49,  49,  36,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  98,
        97,  110, 110, 101, 114, 115, 46,  99,  111, 109, 47,  114, 101, 110,
        100, 101, 114, 95,  117, 114, 108, 95,  52,  51,  51,  53,  57,  48,
        36,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  98,  97,  110,
        110, 101, 114, 115, 46,  99,  111, 109, 47,  114, 101, 110, 100, 101,
        114, 95,  117, 114, 108, 95,  53,  53,  56,  48,  54,  57,  36,  0,
        0,   0,   104, 116, 116, 112, 58,  47,  47,  98,  97,  110, 110, 101,
        114, 115, 46,  99,  111, 109, 47,  114, 101, 110, 100, 101, 114, 95,
        117, 114, 108, 95,  51,  56,  57,  56,  51,  52,  36,  0,   0,   0,
        104, 116, 116, 112, 58,  47,  47,  98,  97,  110, 110, 101, 114, 115,
        46,  99,  111, 109, 47,  114, 101, 110, 100, 101, 114, 95,  117, 114,
        108, 95,  53,  53,  48,  52,  51,  53,  36,  0,   0,   0,   104, 116,
        116, 112, 58,  47,  47,  98,  97,  110, 110, 101, 114, 115, 46,  99,
        111, 109, 47,  114, 101, 110, 100, 101, 114, 95,  117, 114, 108, 95,
        56,  48,  49,  48,  48,  48,  36,  0,   0,   0,   104, 116, 116, 112,
        58,  47,  47,  98,  97,  110, 110, 101, 114, 115, 46,  99,  111, 109,
        47,  114, 101, 110, 100, 101, 114, 95,  117, 114, 108, 95,  57,  50,
        55,  56,  52,  54,  36,  0,   0,   0,   104, 116, 116, 112, 58,  47,
        47,  98,  97,  110, 110, 101, 114, 115, 46,  99,  111, 109, 47,  114,
        101, 110, 100, 101, 114, 95,  117, 114, 108, 95,  52,  51,  52,  51,
        57,  56,  36,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  98,
        97,  110, 110, 101, 114, 115, 46,  99,  111, 109, 47,  114, 101, 110,
        100, 101, 114, 95,  117, 114, 108, 95,  49,  57,  55,  53,  52,  51,
        36,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  98,  97,  110,
        110, 101, 114, 115, 46,  99,  111, 109, 47,  114, 101, 110, 100, 101,
        114, 95,  117, 114, 108, 95,  52,  50,  49,  52,  55,  50,  36,  0,
        0,   0,   104, 116, 116, 112, 58,  47,  47,  98,  97,  110, 110, 101,
        114, 115, 46,  99,  111, 109, 47,  114, 101, 110, 100, 101, 114, 95,
        117, 114, 108, 95,  56,  51,  49,  51,  54,  54,  36,  0,   0,   0,
        104, 116, 116, 112, 58,  47,  47,  98,  97,  110, 110, 101, 114, 115,
        46,  99,  111, 109, 47,  114, 101, 110, 100, 101, 114, 95,  117, 114,
        108, 95,  53,  50,  52,  51,  48,  53,  36,  0,   0,   0,   104, 116,
        116, 112, 58,  47,  47,  98,  97,  110, 110, 101, 114, 115, 46,  99,
        111, 109, 47,  114, 101, 110, 100, 101, 114, 95,  117, 114, 108, 95,
        56,  54,  57,  56,  51,  52,  11,  0,   0,   0,   99,  111, 110, 102,
        105, 103, 46,  106, 115, 111, 110, 83,  0,   0,   0,   0,   0,   0,
        0,   123, 34,  68,  105, 109, 101, 110, 115, 105, 111, 110, 34,  58,
        32,  56,  44,  32,  34,  81,  117, 101, 114, 121, 78,  101, 105, 103,
        104, 98,  111, 114, 115, 67,  111, 117, 110, 116, 34,  58,  32,  56,
        44,  32,  34,  84,  111, 112, 67,  111, 117, 110, 116, 34,  58,  32,
        52,  44,  32,  34,  86,  101, 99,  116, 111, 114, 84,  121, 112, 101,
        83,  116, 114, 34,  58,  32,  34,  117, 105, 110, 116, 56,  34,  125,
    };
    unique_snapshot_path_ = unique_test_folder_ + "/" + unique_snapshot_name_;
    DumpFile(unique_snapshot_path_, snapshotBytes);
    kv_server::InitMetricsContextMap();
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
  privacy_sandbox::server_common::log::NoOpContext log_context_;
};

TEST_F(ANNIndexTest, ThreeKeysOneRequestTest) {
  auto annIndex = ANNIndex();
  auto status = annIndex.TryAddANNSnapshot(unique_snapshot_name_,
                                           unique_snapshot_path_, log_context_);
  ASSERT_EQ(status, SNAPSHOT_STATUS::OK);
  {
    absl::flat_hash_set<std::string_view> key_set = {};
    key_set.insert(std::string_view("01234567"));
    key_set.insert(std::string_view("00000000"));
    key_set.insert(std::string_view("ABCAIJSD"));
    auto result = annIndex.GetKeyValueSet(key_set);
    ASSERT_EQ(result->size(), key_set.size());
    for (const auto& pair : *result) {
      ASSERT_EQ(pair.second.size(), 4);
    }
    {
      {
        EXPECT_EQ((*result)["01234567"][0],
                  "http://banners.com/render_url_903708");
        EXPECT_EQ((*result)["01234567"][1],
                  "http://banners.com/render_url_197543");
        EXPECT_EQ((*result)["01234567"][2],
                  "http://banners.com/render_url_801000");
        EXPECT_EQ((*result)["01234567"][3],
                  "http://banners.com/render_url_389834");
      }
      {
        EXPECT_EQ((*result)["00000000"][0],
                  "http://banners.com/render_url_903708");
        EXPECT_EQ((*result)["00000000"][1],
                  "http://banners.com/render_url_197543");
        EXPECT_EQ((*result)["00000000"][2],
                  "http://banners.com/render_url_801000");
        EXPECT_EQ((*result)["00000000"][3],
                  "http://banners.com/render_url_763951");
      }
      {
        EXPECT_EQ((*result)["ABCAIJSD"][0],
                  "http://banners.com/render_url_903708");
        EXPECT_EQ((*result)["ABCAIJSD"][1],
                  "http://banners.com/render_url_197543");
        EXPECT_EQ((*result)["ABCAIJSD"][2],
                  "http://banners.com/render_url_801000");
        EXPECT_EQ((*result)["ABCAIJSD"][3],
                  "http://banners.com/render_url_389834");
      }
    }
  }
}

TEST_F(ANNIndexTest, NotInitTest) {
  auto annIndex = ANNIndex();
  {
    absl::flat_hash_set<std::string_view> key_set = {};
    key_set.insert(std::string_view("01234567"));
    key_set.insert(std::string_view("00000000"));
    auto result = annIndex.GetKeyValueSet(key_set);
    ASSERT_FALSE(result);
  }
}

TEST_F(ANNIndexTest, OneEmptyRequestTest) {
  auto annIndex = ANNIndex();
  auto status = annIndex.TryAddANNSnapshot(unique_snapshot_name_,
                                           unique_snapshot_path_, log_context_);
  ASSERT_EQ(status, SNAPSHOT_STATUS::OK);
  {
    absl::flat_hash_set<std::string_view> key_set = {};
    auto result = annIndex.GetKeyValueSet(key_set);
    ASSERT_EQ(result->size(), key_set.size());
  }
}

TEST_F(ANNIndexTest, OneKeyOneRequestTest) {
  auto annIndex = ANNIndex();
  auto status = annIndex.TryAddANNSnapshot(unique_snapshot_name_,
                                           unique_snapshot_path_, log_context_);
  ASSERT_EQ(status, SNAPSHOT_STATUS::OK);
  {
    absl::flat_hash_set<std::string_view> key_set = {};
    key_set.insert(std::string_view("01234567"));
    auto result = annIndex.GetKeyValueSet(key_set);
    ASSERT_EQ(result->size(), key_set.size());
    for (const auto& pair : *result) {
      ASSERT_EQ(pair.second.size(), 4);
    }
    {
      EXPECT_EQ((*result)["01234567"][0],
                "http://banners.com/render_url_903708");
      EXPECT_EQ((*result)["01234567"][1],
                "http://banners.com/render_url_197543");
      EXPECT_EQ((*result)["01234567"][2],
                "http://banners.com/render_url_801000");
      EXPECT_EQ((*result)["01234567"][3],
                "http://banners.com/render_url_389834");
    }
  }
}

TEST_F(ANNIndexTest, ManyKeysWithInvalidTest) {
  auto annIndex = ANNIndex();
  auto status = annIndex.TryAddANNSnapshot(unique_snapshot_name_,
                                           unique_snapshot_path_, log_context_);
  ASSERT_EQ(status, SNAPSHOT_STATUS::OK);
  {
    absl::flat_hash_set<std::string_view> key_set = {};
    key_set.insert(std::string_view("01234567"));
    key_set.insert(std::string_view("00000000"));
    key_set.insert(std::string_view("12"));
    key_set.insert(std::string_view("ABCAIJSD"));
    key_set.insert(std::string_view("ABCAIJSD12312312312"));
    key_set.insert(std::string_view("00004321"));
    key_set.insert(std::string_view("32174812"));
    auto result = annIndex.GetKeyValueSet(key_set);
    ASSERT_EQ(result->size(), key_set.size());
    for (const auto& pair : *result) {
      if (pair.first.size() == 8) {
        ASSERT_EQ(pair.second.size(), 4);
      } else {
        ASSERT_EQ(pair.second.size(), 0);
      }
    }
    {
      EXPECT_EQ((*result)["12"].size(), 0);
      EXPECT_EQ((*result)["ABCAIJSD12312312312"].size(), 0);
    }
    {
      EXPECT_EQ((*result)["01234567"][0],
                "http://banners.com/render_url_903708");
      EXPECT_EQ((*result)["01234567"][1],
                "http://banners.com/render_url_197543");
      EXPECT_EQ((*result)["01234567"][2],
                "http://banners.com/render_url_801000");
      EXPECT_EQ((*result)["01234567"][3],
                "http://banners.com/render_url_389834");
    }
    {
      EXPECT_EQ((*result)["00000000"][0],
                "http://banners.com/render_url_903708");
      EXPECT_EQ((*result)["00000000"][1],
                "http://banners.com/render_url_197543");
      EXPECT_EQ((*result)["00000000"][2],
                "http://banners.com/render_url_801000");
      EXPECT_EQ((*result)["00000000"][3],
                "http://banners.com/render_url_763951");
    }
    {
      EXPECT_EQ((*result)["ABCAIJSD"][0],
                "http://banners.com/render_url_903708");
      EXPECT_EQ((*result)["ABCAIJSD"][1],
                "http://banners.com/render_url_197543");
      EXPECT_EQ((*result)["ABCAIJSD"][2],
                "http://banners.com/render_url_801000");
      EXPECT_EQ((*result)["ABCAIJSD"][3],
                "http://banners.com/render_url_389834");
    }
    {
      EXPECT_EQ((*result)["00004321"][0],
                "http://banners.com/render_url_903708");
      EXPECT_EQ((*result)["00004321"][1],
                "http://banners.com/render_url_197543");
      EXPECT_EQ((*result)["00004321"][2],
                "http://banners.com/render_url_801000");
      EXPECT_EQ((*result)["00004321"][3],
                "http://banners.com/render_url_763951");
    }
    {
      EXPECT_EQ((*result)["32174812"][0],
                "http://banners.com/render_url_903708");
      EXPECT_EQ((*result)["32174812"][1],
                "http://banners.com/render_url_197543");
      EXPECT_EQ((*result)["32174812"][2],
                "http://banners.com/render_url_801000");
      EXPECT_EQ((*result)["32174812"][3],
                "http://banners.com/render_url_389834");
    }
  }
}

TEST_F(ANNIndexTest, ManySequentialRequestsTest) {
  auto annIndex = ANNIndex();
  auto status = annIndex.TryAddANNSnapshot(unique_snapshot_name_,
                                           unique_snapshot_path_, log_context_);
  ASSERT_EQ(status, SNAPSHOT_STATUS::OK);
  {
    absl::flat_hash_set<std::string_view> key_set = {};
    key_set.insert(std::string_view("01234567"));
    auto result = annIndex.GetKeyValueSet(key_set);
    ASSERT_EQ(result->size(), key_set.size());
    for (const auto& pair : *result) {
      ASSERT_EQ(pair.second.size(), 4);
    }
    {
      EXPECT_EQ((*result)["01234567"][0],
                "http://banners.com/render_url_903708");
      EXPECT_EQ((*result)["01234567"][1],
                "http://banners.com/render_url_197543");
      EXPECT_EQ((*result)["01234567"][2],
                "http://banners.com/render_url_801000");
      EXPECT_EQ((*result)["01234567"][3],
                "http://banners.com/render_url_389834");
    }
  }
  {
    absl::flat_hash_set<std::string_view> key_set = {};
    key_set.insert(std::string_view("034j05kl"));
    auto result = annIndex.GetKeyValueSet(key_set);
    ASSERT_EQ(result->size(), key_set.size());
    for (const auto& pair : *result) {
      ASSERT_EQ(pair.second.size(), 4);
    }
    {
      EXPECT_EQ((*result)["034j05kl"][0],
                "http://banners.com/render_url_389834");
      EXPECT_EQ((*result)["034j05kl"][1],
                "http://banners.com/render_url_197543");
      EXPECT_EQ((*result)["034j05kl"][2],
                "http://banners.com/render_url_903708");
      EXPECT_EQ((*result)["034j05kl"][3],
                "http://banners.com/render_url_763951");
    }
  }
  {
    absl::flat_hash_set<std::string_view> key_set = {};
    key_set.insert(std::string_view("aaaaaaaa"));
    auto result = annIndex.GetKeyValueSet(key_set);
    ASSERT_EQ(result->size(), key_set.size());
    for (const auto& pair : *result) {
      ASSERT_EQ(pair.second.size(), 4);
    }
    {
      EXPECT_EQ((*result)["aaaaaaaa"][0],
                "http://banners.com/render_url_903708");
      EXPECT_EQ((*result)["aaaaaaaa"][1],
                "http://banners.com/render_url_197543");
      EXPECT_EQ((*result)["aaaaaaaa"][2],
                "http://banners.com/render_url_801000");
      EXPECT_EQ((*result)["aaaaaaaa"][3],
                "http://banners.com/render_url_550435");
    }
  }
  {
    absl::flat_hash_set<std::string_view> key_set = {};
    key_set.insert(std::string_view("1"));
    auto result = annIndex.GetKeyValueSet(key_set);
    ASSERT_EQ(result->size(), key_set.size());
    for (const auto& pair : *result) {
      ASSERT_EQ(pair.second.size(), 0);
    }
  }
  {
    absl::flat_hash_set<std::string_view> key_set = {};
    key_set.insert(std::string_view("`,.,.`+-"));
    auto result = annIndex.GetKeyValueSet(key_set);
    ASSERT_EQ(result->size(), key_set.size());
    for (const auto& pair : *result) {
      ASSERT_EQ(pair.second.size(), 4);
    }
    {
      EXPECT_EQ((*result)["`,.,.`+-"][0],
                "http://banners.com/render_url_197543");
      EXPECT_EQ((*result)["`,.,.`+-"][1],
                "http://banners.com/render_url_903708");
      EXPECT_EQ((*result)["`,.,.`+-"][2],
                "http://banners.com/render_url_801000");
      EXPECT_EQ((*result)["`,.,.`+-"][3],
                "http://banners.com/render_url_831366");
    }
  }
}

}  // namespace
}  // namespace microsoft
}  // namespace kv_server
