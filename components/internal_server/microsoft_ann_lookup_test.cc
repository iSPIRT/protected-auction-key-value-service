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

#include "components/internal_server/microsoft_ann_lookup.h"

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "components/telemetry/server_definition.h"
#include "gtest/gtest.h"

namespace kv_server {
namespace microsoft {
namespace {

class AnnLookupTest : public testing::Test {
 protected:
  void SetUp() override {
    InitMetricsContextMap();
    fake_request_context_ = std::make_unique<RequestContext>();
    fake_request_context_->UpdateLogContext(
        privacy_sandbox::server_common::LogContext(),
        privacy_sandbox::server_common::ConsentedDebugConfiguration());

    unique_test_folder_ = "Test_Folder_" + rand_string();
    std::filesystem::create_directory(unique_test_folder_);
    std::vector<unsigned char> snapshotBytes = {
        237, 254, 13,  240, 4,   0,   0,   0,   11,  0,   0,   0,   99,  111,
        110, 102, 105, 103, 46,  106, 115, 111, 110, 83,  0,   0,   0,   0,
        0,   0,   0,   123, 34,  68,  105, 109, 101, 110, 115, 105, 111, 110,
        34,  58,  32,  56,  44,  32,  34,  81,  117, 101, 114, 121, 78,  101,
        105, 103, 104, 98,  111, 114, 115, 67,  111, 117, 110, 116, 34,  58,
        32,  56,  44,  32,  34,  84,  111, 112, 67,  111, 117, 110, 116, 34,
        58,  32,  52,  44,  32,  34,  86,  101, 99,  116, 111, 114, 84,  121,
        112, 101, 83,  116, 114, 34,  58,  32,  34,  117, 105, 110, 116, 56,
        34,  125, 5,   0,   0,   0,   105, 110, 100, 101, 120, 80,  2,   0,
        0,   0,   0,   0,   0,   80,  2,   0,   0,   0,   0,   0,   0,   9,
        0,   0,   0,   13,  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
        0,   6,   0,   0,   0,   13,  0,   0,   0,   5,   0,   0,   0,   9,
        0,   0,   0,   12,  0,   0,   0,   17,  0,   0,   0,   19,  0,   0,
        0,   6,   0,   0,   0,   13,  0,   0,   0,   2,   0,   0,   0,   4,
        0,   0,   0,   6,   0,   0,   0,   11,  0,   0,   0,   18,  0,   0,
        0,   8,   0,   0,   0,   1,   0,   0,   0,   3,   0,   0,   0,   4,
        0,   0,   0,   6,   0,   0,   0,   7,   0,   0,   0,   11,  0,   0,
        0,   14,  0,   0,   0,   15,  0,   0,   0,   6,   0,   0,   0,   13,
        0,   0,   0,   2,   0,   0,   0,   7,   0,   0,   0,   11,  0,   0,
        0,   14,  0,   0,   0,   17,  0,   0,   0,   7,   0,   0,   0,   13,
        0,   0,   0,   2,   0,   0,   0,   1,   0,   0,   0,   5,   0,   0,
        0,   12,  0,   0,   0,   16,  0,   0,   0,   19,  0,   0,   0,   6,
        0,   0,   0,   4,   0,   0,   0,   0,   0,   0,   0,   6,   0,   0,
        0,   10,  0,   0,   0,   18,  0,   0,   0,   19,  0,   0,   0,   6,
        0,   0,   0,   5,   0,   0,   0,   1,   0,   0,   0,   2,   0,   0,
        0,   9,   0,   0,   0,   11,  0,   0,   0,   16,  0,   0,   0,   4,
        0,   0,   0,   2,   0,   0,   0,   3,   0,   0,   0,   9,   0,   0,
        0,   14,  0,   0,   0,   4,   0,   0,   0,   13,  0,   0,   0,   9,
        0,   0,   0,   12,  0,   0,   0,   19,  0,   0,   0,   9,   0,   0,
        0,   8,   0,   0,   0,   7,   0,   0,   0,   0,   0,   0,   0,   6,
        0,   0,   0,   10,  0,   0,   0,   11,  0,   0,   0,   14,  0,   0,
        0,   17,  0,   0,   0,   18,  0,   0,   0,   7,   0,   0,   0,   5,
        0,   0,   0,   13,  0,   0,   0,   9,   0,   0,   0,   11,  0,   0,
        0,   14,  0,   0,   0,   15,  0,   0,   0,   16,  0,   0,   0,   9,
        0,   0,   0,   1,   0,   0,   0,   2,   0,   0,   0,   6,   0,   0,
        0,   9,   0,   0,   0,   3,   0,   0,   0,   10,  0,   0,   0,   12,
        0,   0,   0,   15,  0,   0,   0,   16,  0,   0,   0,   6,   0,   0,
        0,   11,  0,   0,   0,   0,   0,   0,   0,   4,   0,   0,   0,   8,
        0,   0,   0,   15,  0,   0,   0,   17,  0,   0,   0,   7,   0,   0,
        0,   4,   0,   0,   0,   1,   0,   0,   0,   3,   0,   0,   0,   0,
        0,   0,   0,   10,  0,   0,   0,   8,   0,   0,   0,   17,  0,   0,
        0,   7,   0,   0,   0,   3,   0,   0,   0,   9,   0,   0,   0,   2,
        0,   0,   0,   7,   0,   0,   0,   10,  0,   0,   0,   15,  0,   0,
        0,   17,  0,   0,   0,   5,   0,   0,   0,   11,  0,   0,   0,   14,
        0,   0,   0,   12,  0,   0,   0,   2,   0,   0,   0,   10,  0,   0,
        0,   4,   0,   0,   0,   11,  0,   0,   0,   4,   0,   0,   0,   10,
        0,   0,   0,   6,   0,   0,   0,   7,   0,   0,   0,   0,   0,   0,
        0,   12,  0,   0,   0,   9,   0,   0,   0,   3,   0,   0,   0,   13,
        0,   0,   0,   14,  0,   0,   0,   18,  0,   0,   0,   4,   0,   0,
        0,   17,  0,   0,   0,   9,   0,   0,   0,   1,   0,   0,   0,   5,
        0,   0,   0,   4,   0,   0,   0,   4,   0,   0,   0,   0,   0,   0,
        0,   5,   0,   0,   0,   8,   0,   0,   0,   10,  0,   0,   0,   105,
        110, 100, 101, 120, 46,  100, 97,  116, 97,  168, 0,   0,   0,   0,
        0,   0,   0,   20,  0,   0,   0,   8,   0,   0,   0,   242, 6,   59,
        90,  153, 146, 158, 226, 163, 161, 179, 208, 220, 222, 207, 56,  152,
        201, 198, 174, 38,  80,  232, 0,   182, 162, 190, 251, 189, 8,   42,
        112, 127, 172, 87,  150, 243, 74,  216, 52,  55,  200, 65,  80,  90,
        0,   236, 175, 17,  224, 54,  90,  89,  130, 62,  32,  247, 239, 253,
        161, 31,  110, 91,  19,  228, 168, 240, 20,  255, 188, 6,   212, 226,
        211, 115, 76,  37,  210, 33,  197, 41,  81,  157, 141, 102, 40,  181,
        245, 136, 119, 205, 169, 116, 223, 152, 48,  208, 23,  79,  108, 179,
        186, 126, 36,  203, 143, 148, 182, 237, 83,  193, 153, 245, 167, 71,
        244, 3,   11,  87,  149, 160, 23,  179, 138, 15,  109, 107, 1,   14,
        73,  133, 194, 179, 174, 233, 51,  219, 5,   78,  148, 134, 188, 92,
        184, 224, 181, 46,  152, 107, 253, 223, 182, 214, 119, 81,  9,   224,
        86,  249, 130, 7,   0,   0,   0,   109, 97,  112, 112, 105, 110, 103,
        0,   4,   0,   0,   0,   0,   0,   0,   20,  0,   0,   0,   47,  0,
        0,   0,   104, 116, 116, 112, 58,  47,  47,  102, 97,  107, 101, 46,
        109, 105, 99,  114, 111, 115, 111, 102, 116, 46,  101, 120, 97,  109,
        112, 108, 101, 47,  114, 101, 110, 100, 101, 114, 95,  117, 114, 108,
        95,  55,  55,  51,  51,  49,  54,  47,  0,   0,   0,   104, 116, 116,
        112, 58,  47,  47,  102, 97,  107, 101, 46,  109, 105, 99,  114, 111,
        115, 111, 102, 116, 46,  101, 120, 97,  109, 112, 108, 101, 47,  114,
        101, 110, 100, 101, 114, 95,  117, 114, 108, 95,  55,  48,  56,  49,
        48,  52,  47,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  102,
        97,  107, 101, 46,  109, 105, 99,  114, 111, 115, 111, 102, 116, 46,
        101, 120, 97,  109, 112, 108, 101, 47,  114, 101, 110, 100, 101, 114,
        95,  117, 114, 108, 95,  57,  50,  52,  53,  54,  57,  47,  0,   0,
        0,   104, 116, 116, 112, 58,  47,  47,  102, 97,  107, 101, 46,  109,
        105, 99,  114, 111, 115, 111, 102, 116, 46,  101, 120, 97,  109, 112,
        108, 101, 47,  114, 101, 110, 100, 101, 114, 95,  117, 114, 108, 95,
        54,  52,  50,  50,  52,  54,  47,  0,   0,   0,   104, 116, 116, 112,
        58,  47,  47,  102, 97,  107, 101, 46,  109, 105, 99,  114, 111, 115,
        111, 102, 116, 46,  101, 120, 97,  109, 112, 108, 101, 47,  114, 101,
        110, 100, 101, 114, 95,  117, 114, 108, 95,  56,  48,  51,  57,  54,
        53,  47,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  102, 97,
        107, 101, 46,  109, 105, 99,  114, 111, 115, 111, 102, 116, 46,  101,
        120, 97,  109, 112, 108, 101, 47,  114, 101, 110, 100, 101, 114, 95,
        117, 114, 108, 95,  55,  54,  51,  57,  53,  49,  47,  0,   0,   0,
        104, 116, 116, 112, 58,  47,  47,  102, 97,  107, 101, 46,  109, 105,
        99,  114, 111, 115, 111, 102, 116, 46,  101, 120, 97,  109, 112, 108,
        101, 47,  114, 101, 110, 100, 101, 114, 95,  117, 114, 108, 95,  57,
        48,  51,  55,  48,  56,  47,  0,   0,   0,   104, 116, 116, 112, 58,
        47,  47,  102, 97,  107, 101, 46,  109, 105, 99,  114, 111, 115, 111,
        102, 116, 46,  101, 120, 97,  109, 112, 108, 101, 47,  114, 101, 110,
        100, 101, 114, 95,  117, 114, 108, 95,  52,  48,  53,  53,  49,  49,
        47,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  102, 97,  107,
        101, 46,  109, 105, 99,  114, 111, 115, 111, 102, 116, 46,  101, 120,
        97,  109, 112, 108, 101, 47,  114, 101, 110, 100, 101, 114, 95,  117,
        114, 108, 95,  52,  51,  51,  53,  57,  48,  47,  0,   0,   0,   104,
        116, 116, 112, 58,  47,  47,  102, 97,  107, 101, 46,  109, 105, 99,
        114, 111, 115, 111, 102, 116, 46,  101, 120, 97,  109, 112, 108, 101,
        47,  114, 101, 110, 100, 101, 114, 95,  117, 114, 108, 95,  53,  53,
        56,  48,  54,  57,  47,  0,   0,   0,   104, 116, 116, 112, 58,  47,
        47,  102, 97,  107, 101, 46,  109, 105, 99,  114, 111, 115, 111, 102,
        116, 46,  101, 120, 97,  109, 112, 108, 101, 47,  114, 101, 110, 100,
        101, 114, 95,  117, 114, 108, 95,  51,  56,  57,  56,  51,  52,  47,
        0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  102, 97,  107, 101,
        46,  109, 105, 99,  114, 111, 115, 111, 102, 116, 46,  101, 120, 97,
        109, 112, 108, 101, 47,  114, 101, 110, 100, 101, 114, 95,  117, 114,
        108, 95,  53,  53,  48,  52,  51,  53,  47,  0,   0,   0,   104, 116,
        116, 112, 58,  47,  47,  102, 97,  107, 101, 46,  109, 105, 99,  114,
        111, 115, 111, 102, 116, 46,  101, 120, 97,  109, 112, 108, 101, 47,
        114, 101, 110, 100, 101, 114, 95,  117, 114, 108, 95,  56,  48,  49,
        48,  48,  48,  47,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,
        102, 97,  107, 101, 46,  109, 105, 99,  114, 111, 115, 111, 102, 116,
        46,  101, 120, 97,  109, 112, 108, 101, 47,  114, 101, 110, 100, 101,
        114, 95,  117, 114, 108, 95,  57,  50,  55,  56,  52,  54,  47,  0,
        0,   0,   104, 116, 116, 112, 58,  47,  47,  102, 97,  107, 101, 46,
        109, 105, 99,  114, 111, 115, 111, 102, 116, 46,  101, 120, 97,  109,
        112, 108, 101, 47,  114, 101, 110, 100, 101, 114, 95,  117, 114, 108,
        95,  52,  51,  52,  51,  57,  56,  47,  0,   0,   0,   104, 116, 116,
        112, 58,  47,  47,  102, 97,  107, 101, 46,  109, 105, 99,  114, 111,
        115, 111, 102, 116, 46,  101, 120, 97,  109, 112, 108, 101, 47,  114,
        101, 110, 100, 101, 114, 95,  117, 114, 108, 95,  49,  57,  55,  53,
        52,  51,  47,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  102,
        97,  107, 101, 46,  109, 105, 99,  114, 111, 115, 111, 102, 116, 46,
        101, 120, 97,  109, 112, 108, 101, 47,  114, 101, 110, 100, 101, 114,
        95,  117, 114, 108, 95,  52,  50,  49,  52,  55,  50,  47,  0,   0,
        0,   104, 116, 116, 112, 58,  47,  47,  102, 97,  107, 101, 46,  109,
        105, 99,  114, 111, 115, 111, 102, 116, 46,  101, 120, 97,  109, 112,
        108, 101, 47,  114, 101, 110, 100, 101, 114, 95,  117, 114, 108, 95,
        56,  51,  49,  51,  54,  54,  47,  0,   0,   0,   104, 116, 116, 112,
        58,  47,  47,  102, 97,  107, 101, 46,  109, 105, 99,  114, 111, 115,
        111, 102, 116, 46,  101, 120, 97,  109, 112, 108, 101, 47,  114, 101,
        110, 100, 101, 114, 95,  117, 114, 108, 95,  53,  50,  52,  51,  48,
        53,  47,  0,   0,   0,   104, 116, 116, 112, 58,  47,  47,  102, 97,
        107, 101, 46,  109, 105, 99,  114, 111, 115, 111, 102, 116, 46,  101,
        120, 97,  109, 112, 108, 101, 47,  114, 101, 110, 100, 101, 114, 95,
        117, 114, 108, 95,  56,  54,  57,  56,  51,  52,
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
  std::string unique_snapshot_name_ = "ANNSNAPSHOT_0000000000000001";
  std::string unique_snapshot_path_;
  std::shared_ptr<RequestContext> fake_request_context_;
  privacy_sandbox::server_common::log::NoOpContext log_context_;
};

TEST_F(AnnLookupTest, NotInitializedTest) {
  auto annIndex = ANNIndex();
  auto lookup = AnnLookup::CreateAnnLookup(annIndex);
  {
    absl::flat_hash_set<std::string_view> key_set = {};
    key_set.insert(std::string_view("01234567"));
    key_set.insert(std::string_view("00000000"));
    auto result = lookup->GetKeyValueSet(*fake_request_context_, key_set);
    ASSERT_EQ(result.status(),
              absl::InternalError("Do not have initialized snapshots"));
  }
}

TEST_F(AnnLookupTest, NotImplementedTest) {
  auto annIndex = ANNIndex();
  annIndex.TryAddANNSnapshot(unique_snapshot_name_, unique_snapshot_path_,
                             log_context_);
  auto lookup = AnnLookup::CreateAnnLookup(annIndex);
  {
    absl::flat_hash_set<std::string_view> key_set = {};
    auto result = lookup->GetKeyValues(*fake_request_context_, key_set);
    ASSERT_EQ(result.status(), absl::InternalError("Not implemented"));
  }
  {
    absl::flat_hash_set<std::string_view> key_set = {};
    auto result = lookup->GetUInt32ValueSet(*fake_request_context_, key_set);
    ASSERT_EQ(result.status(), absl::InternalError("Not implemented"));
  }
  {
    absl::flat_hash_set<std::string_view> key_set = {};
    auto result = lookup->GetUInt64ValueSet(*fake_request_context_, key_set);
    ASSERT_EQ(result.status(), absl::InternalError("Not implemented"));
  }
  {
    std::string query = "";
    auto result = lookup->RunQuery(*fake_request_context_, query);
    ASSERT_EQ(result.status(), absl::InternalError("Not implemented"));
  }
  {
    std::string query = "";
    auto result = lookup->RunSetQueryUInt32(*fake_request_context_, query);
    ASSERT_EQ(result.status(), absl::InternalError("Not implemented"));
  }
  {
    std::string query = "";
    auto result = lookup->RunSetQueryUInt64(*fake_request_context_, query);
    ASSERT_EQ(result.status(), absl::InternalError("Not implemented"));
  }
}

TEST_F(AnnLookupTest, EmptyTest) {
  auto annIndex = ANNIndex();
  annIndex.TryAddANNSnapshot(unique_snapshot_name_, unique_snapshot_path_,
                             log_context_);
  auto lookup = AnnLookup::CreateAnnLookup(annIndex);
  {
    absl::flat_hash_set<std::string_view> key_set = {};
    auto result = lookup->GetKeyValueSet(*fake_request_context_, key_set);
    ASSERT_EQ(result->kv_pairs().size(), 0);
  }
}

TEST_F(AnnLookupTest, CommonTest) {
  auto annIndex = ANNIndex();
  annIndex.TryAddANNSnapshot(unique_snapshot_name_, unique_snapshot_path_,
                             log_context_);
  auto lookup = AnnLookup::CreateAnnLookup(annIndex);
  {
    // first request
    absl::flat_hash_set<std::string_view> key_set = {};
    key_set.insert(std::string_view("01234567"));
    key_set.insert(std::string_view("00000000"));
    key_set.insert(std::string_view("ABCAIJSD"));
    key_set.insert(std::string_view("12"));
    key_set.insert(std::string_view("ABCAIJSD12312312312"));
    key_set.insert(std::string_view("00004321"));
    auto result = lookup->GetKeyValueSet(*fake_request_context_, key_set);
    ASSERT_EQ(result->kv_pairs().size(), key_set.size());
    for (const auto& key : key_set) {
      auto& singleResult = result->kv_pairs().at(std::string(key));
      if (key == "01234567") {
        ASSERT_EQ(singleResult.keyset_values().values().size(), 4);
        EXPECT_EQ(singleResult.keyset_values().values()[0],
                  "http://fake.microsoft.example/render_url_903708");
        EXPECT_EQ(singleResult.keyset_values().values()[1],
                  "http://fake.microsoft.example/render_url_197543");
        EXPECT_EQ(singleResult.keyset_values().values()[2],
                  "http://fake.microsoft.example/render_url_801000");
        EXPECT_EQ(singleResult.keyset_values().values()[3],
                  "http://fake.microsoft.example/render_url_389834");
      } else if (key == "00000000") {
        ASSERT_EQ(singleResult.keyset_values().values().size(), 4);
        EXPECT_EQ(singleResult.keyset_values().values()[0],
                  "http://fake.microsoft.example/render_url_903708");
        EXPECT_EQ(singleResult.keyset_values().values()[1],
                  "http://fake.microsoft.example/render_url_197543");
        EXPECT_EQ(singleResult.keyset_values().values()[2],
                  "http://fake.microsoft.example/render_url_801000");
        EXPECT_EQ(singleResult.keyset_values().values()[3],
                  "http://fake.microsoft.example/render_url_763951");
      } else if (key == "ABCAIJSD") {
        ASSERT_EQ(singleResult.keyset_values().values().size(), 4);
        EXPECT_EQ(singleResult.keyset_values().values()[0],
                  "http://fake.microsoft.example/render_url_903708");
        EXPECT_EQ(singleResult.keyset_values().values()[1],
                  "http://fake.microsoft.example/render_url_197543");
        EXPECT_EQ(singleResult.keyset_values().values()[2],
                  "http://fake.microsoft.example/render_url_801000");
        EXPECT_EQ(singleResult.keyset_values().values()[3],
                  "http://fake.microsoft.example/render_url_389834");
      } else if (key == "12") {
        EXPECT_EQ(singleResult.status().code(),
                  static_cast<const int>(absl::StatusCode::kNotFound));
        EXPECT_EQ(singleResult.status().message(),
                  "No result, most likely incorrect key: 12");
      } else if (key == "ABCAIJSD12312312312") {
        EXPECT_EQ(singleResult.status().code(),
                  static_cast<const int>(absl::StatusCode::kNotFound));
        EXPECT_EQ(singleResult.status().message(),
                  "No result, most likely incorrect key: ABCAIJSD12312312312");
      } else if (key == "00004321") {
        ASSERT_EQ(singleResult.keyset_values().values().size(), 4);
        EXPECT_EQ(singleResult.keyset_values().values()[0],
                  "http://fake.microsoft.example/render_url_903708");
        EXPECT_EQ(singleResult.keyset_values().values()[1],
                  "http://fake.microsoft.example/render_url_197543");
        EXPECT_EQ(singleResult.keyset_values().values()[2],
                  "http://fake.microsoft.example/render_url_801000");
        EXPECT_EQ(singleResult.keyset_values().values()[3],
                  "http://fake.microsoft.example/render_url_763951");
      } else {
        FAIL() << "Unexpected key: " << key;
      }
    }
  }
  {
    // second request
    absl::flat_hash_set<std::string_view> key_set = {};
    key_set.insert(std::string_view("AOISDASO8"));
    key_set.insert(std::string_view("~!@~*!@~"));
    key_set.insert(std::string_view(",l,l..~H"));
    key_set.insert(std::string_view("TESTTEST"));
    key_set.insert(std::string_view("'[']']'."));
    key_set.insert(std::string_view(",l,l..~H12312312312"));
    auto result = lookup->GetKeyValueSet(*fake_request_context_, key_set);
    ASSERT_EQ(result->kv_pairs().size(), key_set.size());
    for (const auto& key : key_set) {
      auto& singleResult = result->kv_pairs().at(std::string(key));
      if (key == "AOISDASO8") {
        EXPECT_EQ(singleResult.status().code(),
                  static_cast<const int>(absl::StatusCode::kNotFound));
        EXPECT_EQ(singleResult.status().message(),
                  "No result, most likely incorrect key: AOISDASO8");
      } else if (key == "~!@~*!@~") {
        ASSERT_EQ(singleResult.keyset_values().values().size(), 4);
        EXPECT_EQ(singleResult.keyset_values().values()[0],
                  "http://fake.microsoft.example/render_url_197543");
        EXPECT_EQ(singleResult.keyset_values().values()[1],
                  "http://fake.microsoft.example/render_url_831366");
        EXPECT_EQ(singleResult.keyset_values().values()[2],
                  "http://fake.microsoft.example/render_url_434398");
        EXPECT_EQ(singleResult.keyset_values().values()[3],
                  "http://fake.microsoft.example/render_url_389834");
      } else if (key == ",l,l..~H") {
        ASSERT_EQ(singleResult.keyset_values().values().size(), 4);
        EXPECT_EQ(singleResult.keyset_values().values()[0],
                  "http://fake.microsoft.example/render_url_903708");
        EXPECT_EQ(singleResult.keyset_values().values()[1],
                  "http://fake.microsoft.example/render_url_763951");
        EXPECT_EQ(singleResult.keyset_values().values()[2],
                  "http://fake.microsoft.example/render_url_197543");
        EXPECT_EQ(singleResult.keyset_values().values()[3],
                  "http://fake.microsoft.example/render_url_389834");
      } else if (key == "TESTTEST") {
        ASSERT_EQ(singleResult.keyset_values().values().size(), 4);
        EXPECT_EQ(singleResult.keyset_values().values()[0],
                  "http://fake.microsoft.example/render_url_197543");
        EXPECT_EQ(singleResult.keyset_values().values()[1],
                  "http://fake.microsoft.example/render_url_903708");
        EXPECT_EQ(singleResult.keyset_values().values()[2],
                  "http://fake.microsoft.example/render_url_801000");
        EXPECT_EQ(singleResult.keyset_values().values()[3],
                  "http://fake.microsoft.example/render_url_389834");
      } else if (key == "'[']']'.") {
        ASSERT_EQ(singleResult.keyset_values().values().size(), 4);
        EXPECT_EQ(singleResult.keyset_values().values()[0],
                  "http://fake.microsoft.example/render_url_903708");
        EXPECT_EQ(singleResult.keyset_values().values()[1],
                  "http://fake.microsoft.example/render_url_197543");
        EXPECT_EQ(singleResult.keyset_values().values()[2],
                  "http://fake.microsoft.example/render_url_801000");
        EXPECT_EQ(singleResult.keyset_values().values()[3],
                  "http://fake.microsoft.example/render_url_550435");
      } else if (key == ",l,l..~H12312312312") {
        EXPECT_EQ(singleResult.status().code(),
                  static_cast<const int>(absl::StatusCode::kNotFound));
        EXPECT_EQ(singleResult.status().message(),
                  "No result, most likely incorrect key: ,l,l..~H12312312312");
      } else {
        FAIL() << "Unexpected key: " << key;
      }
    }
  }
}

}  // namespace
}  // namespace microsoft
}  // namespace kv_server
