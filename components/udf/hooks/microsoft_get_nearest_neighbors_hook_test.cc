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

#include "components/udf/hooks/microsoft_get_nearest_neighbors_hook.h"

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "components/internal_server/mocks.h"
#include "gmock/gmock.h"
#include "google/protobuf/message_lite.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "nlohmann/json.hpp"
#include "public/test_util/proto_matcher.h"

namespace kv_server {
namespace {

using google::protobuf::TextFormat;
using google::scp::roma::FunctionBindingPayload;
using google::scp::roma::proto::FunctionBindingIoProto;
using testing::_;
using testing::Return;

class GetNearestNeighborsHookTest : public ::testing::Test {
 protected:
  GetNearestNeighborsHookTest() {
    InitMetricsContextMap();
    request_context_ = std::make_shared<RequestContext>();
  }
  std::shared_ptr<RequestContext> GetRequestContext() {
    return request_context_;
  }
  std::shared_ptr<RequestContext> request_context_;
};

TEST_F(GetNearestNeighborsHookTest, SuccessfullyProcessesValue) {
  absl::flat_hash_set<std::string_view> keys = {"embedding1", "embedding2"};
  InternalLookupResponse lookup_response;
  ASSERT_TRUE(
      TextFormat::ParseFromString(R"pb(kv_pairs {
                                         key: "embedding1"
                                         value {
                                           keyset_values {
                                             values: "embedding1_renderurl1"
                                             values: "embedding1_renderurl2"
                                             values: "embedding1_renderurl3"
                                           }
                                         }
                                       }
                                       kv_pairs {
                                         key: "embedding2"
                                         value {
                                           keyset_values {
                                             values: "embedding2_renderurl1"
                                             values: "embedding2_renderurl2"
                                             values: "embedding2_renderurl3"
                                           }
                                         }
                                       })pb",
                                  &lookup_response));
  auto mock_lookup = std::make_unique<MockLookup>();
  EXPECT_CALL(*mock_lookup, GetKeyValueSet(_, keys))
      .WillOnce(Return(lookup_response));

  FunctionBindingIoProto io;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(input_list_of_string { data: "embedding1" data: "embedding2" })pb",
      &io));
  auto get_values_hook = GetNearestNeighborsHook::Create();
  get_values_hook->FinishInit(std::move(mock_lookup));
  FunctionBindingPayload<std::weak_ptr<RequestContext>> payload{
      io, GetRequestContext()};
  (*get_values_hook)(payload);

  nlohmann::json result_json =
      nlohmann::json::parse(io.output_string(), nullptr,
                            /*allow_exceptions=*/false,
                            /*ignore_comments=*/true);
  ASSERT_FALSE(result_json.is_discarded());
  ASSERT_EQ(result_json["status"]["code"], 0);
  ASSERT_EQ(result_json["status"]["message"], "ok");
  ASSERT_TRUE(result_json.contains("kvPairs"));
  nlohmann::json expected_value1 =
      R"({"keysetValues":{"values":["embedding1_renderurl1","embedding1_renderurl2","embedding1_renderurl3"]}})"_json;
  nlohmann::json expected_value2 =
      R"({"keysetValues":{"values":["embedding2_renderurl1","embedding2_renderurl2","embedding2_renderurl3"]}})"_json;
  EXPECT_EQ(result_json["kvPairs"]["embedding1"], expected_value1);
  EXPECT_EQ(result_json["kvPairs"]["embedding2"], expected_value2);
}

TEST_F(GetNearestNeighborsHookTest, SuccessfullyProcessesResultsWithStatus) {
  absl::flat_hash_set<std::string_view> keys = {"embedding1"};
  InternalLookupResponse lookup_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(kv_pairs {
             key: "embedding1"
             value { status { code: 2, message: "Some error" } }
           })pb",
      &lookup_response));

  auto mock_lookup = std::make_unique<MockLookup>();
  EXPECT_CALL(*mock_lookup, GetKeyValueSet(_, keys))
      .WillOnce(Return(lookup_response));

  FunctionBindingIoProto io;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(input_list_of_string { data: "embedding1" })pb", &io));
  auto get_values_hook = GetNearestNeighborsHook::Create();
  get_values_hook->FinishInit(std::move(mock_lookup));
  FunctionBindingPayload<std::weak_ptr<RequestContext>> payload{
      io, GetRequestContext()};
  (*get_values_hook)(payload);

  nlohmann::json result_json =
      nlohmann::json::parse(io.output_string(), nullptr,
                            /*allow_exceptions=*/false,
                            /*ignore_comments=*/true);
  ASSERT_FALSE(result_json.is_discarded());
  ASSERT_EQ(result_json["status"]["code"], 0);
  ASSERT_EQ(result_json["status"]["message"], "ok");
  ASSERT_TRUE(result_json.contains("kvPairs"));
  nlohmann::json expected =
      R"({"embedding1":{"status":{"code":2,"message":"Some error"}}})"_json;
  EXPECT_EQ(result_json["kvPairs"], expected);
}

TEST_F(GetNearestNeighborsHookTest, LookupReturnsError) {
  absl::flat_hash_set<std::string_view> keys = {"embedding1"};
  auto mock_lookup = std::make_unique<MockLookup>();
  EXPECT_CALL(*mock_lookup, GetKeyValueSet(_, keys))
      .WillOnce(Return(absl::UnknownError("Some error")));

  FunctionBindingIoProto io;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(input_list_of_string { data: "embedding1" })pb", &io));
  auto get_values_hook = GetNearestNeighborsHook::Create();
  get_values_hook->FinishInit(std::move(mock_lookup));
  FunctionBindingPayload<std::weak_ptr<RequestContext>> payload{
      io, GetRequestContext()};
  (*get_values_hook)(payload);

  nlohmann::json expected = R"({"code":2,"message":"Some error"})"_json;
  EXPECT_EQ(io.output_string(), expected.dump());
}

TEST_F(GetNearestNeighborsHookTest, InputIsNotListOfStrings) {
  auto mock_lookup = std::make_unique<MockLookup>();

  FunctionBindingIoProto io;
  ASSERT_TRUE(
      TextFormat::ParseFromString(R"pb(input_string: "embedding1")pb", &io));
  auto get_values_hook = GetNearestNeighborsHook::Create();
  get_values_hook->FinishInit(std::move(mock_lookup));
  FunctionBindingPayload<std::weak_ptr<RequestContext>> payload{
      io, GetRequestContext()};
  (*get_values_hook)(payload);

  nlohmann::json expected =
      R"({"code":3,"message":"getNearestNeighbors input must be list of strings"})"_json;
  EXPECT_EQ(io.output_string(), expected.dump());
}

TEST_F(GetNearestNeighborsHookTest, EmptyInputListOfStrings) {
  auto mock_lookup = std::make_unique<MockLookup>();

  FunctionBindingIoProto io;
  ASSERT_TRUE(
      TextFormat::ParseFromString(R"pb(input_list_of_string {})pb", &io));
  auto get_values_hook = GetNearestNeighborsHook::Create();
  get_values_hook->FinishInit(std::move(mock_lookup));
  FunctionBindingPayload<std::weak_ptr<RequestContext>> payload{
      io, GetRequestContext()};
  (*get_values_hook)(payload);

  nlohmann::json expected =
      R"({"code":3,"message":"getNearestNeighbors input must have keys"})"_json;
  EXPECT_EQ(io.output_string(), expected.dump());
}

TEST_F(GetNearestNeighborsHookTest, NotFinishInitCalled) {
  FunctionBindingIoProto io;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(input_list_of_string { data: "embedding1" })pb", &io));
  auto get_values_hook = GetNearestNeighborsHook::Create();
  FunctionBindingPayload<std::weak_ptr<RequestContext>> payload{
      io, GetRequestContext()};
  (*get_values_hook)(payload);

  nlohmann::json expected =
      R"({"code":13,"message":"getNearestNeighbors has not been initialized yet"})"_json;
  EXPECT_EQ(io.output_string(), expected.dump());
}

TEST_F(GetNearestNeighborsHookTest, AllKeysPassedToLookup) {
  absl::flat_hash_set<std::string_view> keys = {
      "embedding1", "embedding2",  "embedding3",  "embedding4",
      "embedding5", "embedding6",  "embedding7",  "embedding8",
      "embedding9", "embedding10", "embedding11", "embedding12"};
  InternalLookupResponse lookup_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(kv_pairs {
             key: "embedding1"
             value { keyset_values { values: "embedding1_renderurl" } }
           }
           kv_pairs {
             key: "embedding2"
             value { keyset_values { values: "embedding2_renderurl" } }
           }
           kv_pairs {
             key: "embedding3"
             value { keyset_values { values: "embedding3_renderurl" } }
           }
           kv_pairs {
             key: "embedding4"
             value { keyset_values { values: "embedding4_renderurl" } }
           }
           kv_pairs {
             key: "embedding5"
             value { keyset_values { values: "embedding5_renderurl" } }
           }
           kv_pairs {
             key: "embedding6"
             value { keyset_values { values: "embedding6_renderurl" } }
           }
           kv_pairs {
             key: "embedding7"
             value { keyset_values { values: "embedding7_renderurl" } }
           }
           kv_pairs {
             key: "embedding8"
             value { keyset_values { values: "embedding8_renderurl" } }
           }
           kv_pairs {
             key: "embedding9"
             value { keyset_values { values: "embedding9_renderurl" } }
           }
           kv_pairs {
             key: "embedding10"
             value { keyset_values { values: "embedding10_renderurl" } }
           }
           kv_pairs {
             key: "embedding11"
             value { keyset_values { values: "embedding11_renderurl" } }
           }
           kv_pairs {
             key: "embedding12"
             value { keyset_values { values: "embedding12_renderurl" } }
           })pb",
      &lookup_response));
  auto mock_lookup = std::make_unique<MockLookup>();
  EXPECT_CALL(*mock_lookup, GetKeyValueSet(_, _))
      .WillRepeatedly(Return(absl::UnknownError("Wrong call")));
  EXPECT_CALL(*mock_lookup, GetKeyValueSet(_, keys))
      .WillOnce(Return(lookup_response));

  FunctionBindingIoProto ioMore;
  ASSERT_TRUE(TextFormat::ParseFromString(R"pb(input_list_of_string {
                                                 data: "embedding1"
                                                 data: "embedding2"
                                                 data: "embedding3"
                                                 data: "embedding4"
                                                 data: "embedding5"
                                                 data: "embedding6"
                                                 data: "embedding7"
                                                 data: "embedding8"
                                                 data: "embedding9"
                                                 data: "embedding10"
                                                 data: "embedding11"
                                                 data: "embedding12"
                                                 data: "embedding13"
                                               })pb",
                                          &ioMore));

  FunctionBindingIoProto ioMoreCenter;
  ASSERT_TRUE(TextFormat::ParseFromString(R"pb(input_list_of_string {
                                                 data: "embedding1"
                                                 data: "embedding2"
                                                 data: "embedding3"
                                                 data: "embedding4"
                                                 data: "embedding5"
                                                 data: "embedding6"
                                                 data: "embedding6_1"
                                                 data: "embedding7"
                                                 data: "embedding8"
                                                 data: "embedding9"
                                                 data: "embedding10"
                                                 data: "embedding11"
                                                 data: "embedding12"
                                               })pb",
                                          &ioMoreCenter));

  FunctionBindingIoProto ioLess;
  ASSERT_TRUE(TextFormat::ParseFromString(R"pb(input_list_of_string {
                                                 data: "embedding1"
                                                 data: "embedding2"
                                                 data: "embedding3"
                                                 data: "embedding4"
                                                 data: "embedding5"
                                                 data: "embedding6"
                                                 data: "embedding7"
                                                 data: "embedding8"
                                                 data: "embedding9"
                                                 data: "embedding10"
                                                 data: "embedding11"
                                               })pb",
                                          &ioLess));

  FunctionBindingIoProto ioLessCenter;
  ASSERT_TRUE(TextFormat::ParseFromString(R"pb(input_list_of_string {
                                                 data: "embedding1"
                                                 data: "embedding2"
                                                 data: "embedding3"
                                                 data: "embedding4"
                                                 data: "embedding5"
                                                 data: "embedding7"
                                                 data: "embedding8"
                                                 data: "embedding9"
                                                 data: "embedding10"
                                                 data: "embedding11"
                                                 data: "embedding12"
                                               })pb",
                                          &ioLessCenter));

  FunctionBindingIoProto ioCorrect;
  ASSERT_TRUE(TextFormat::ParseFromString(R"pb(input_list_of_string {
                                                 data: "embedding1"
                                                 data: "embedding2"
                                                 data: "embedding3"
                                                 data: "embedding4"
                                                 data: "embedding5"
                                                 data: "embedding6"
                                                 data: "embedding7"
                                                 data: "embedding8"
                                                 data: "embedding9"
                                                 data: "embedding10"
                                                 data: "embedding11"
                                                 data: "embedding12"
                                               })pb",
                                          &ioCorrect));
  auto get_values_hook = GetNearestNeighborsHook::Create();
  get_values_hook->FinishInit(std::move(mock_lookup));

  FunctionBindingPayload<std::weak_ptr<RequestContext>> payload1{
      ioMore, GetRequestContext()};
  (*get_values_hook)(payload1);
  nlohmann::json result1 =
      nlohmann::json::parse(ioMore.output_string(), nullptr, false, true);
  ASSERT_FALSE(result1.is_discarded());
  EXPECT_EQ(result1["code"], 2);
  EXPECT_EQ(result1["message"], "Wrong call");
  EXPECT_FALSE(result1.contains("kvPairs"));

  FunctionBindingPayload<std::weak_ptr<RequestContext>> payload2{
      ioMoreCenter, GetRequestContext()};
  (*get_values_hook)(payload2);
  nlohmann::json result2 =
      nlohmann::json::parse(ioMoreCenter.output_string(), nullptr, false, true);
  ASSERT_FALSE(result2.is_discarded());
  EXPECT_EQ(result2["code"], 2);
  EXPECT_EQ(result2["message"], "Wrong call");
  EXPECT_FALSE(result2.contains("kvPairs"));

  FunctionBindingPayload<std::weak_ptr<RequestContext>> payload3{
      ioLess, GetRequestContext()};
  (*get_values_hook)(payload3);
  nlohmann::json result3 =
      nlohmann::json::parse(ioLess.output_string(), nullptr, false, true);
  ASSERT_FALSE(result3.is_discarded());
  EXPECT_EQ(result3["code"], 2);
  EXPECT_EQ(result3["message"], "Wrong call");
  EXPECT_FALSE(result3.contains("kvPairs"));

  FunctionBindingPayload<std::weak_ptr<RequestContext>> payload4{
      ioLessCenter, GetRequestContext()};
  (*get_values_hook)(payload4);
  nlohmann::json result4 =
      nlohmann::json::parse(ioLessCenter.output_string(), nullptr, false, true);
  ASSERT_FALSE(result4.is_discarded());
  EXPECT_EQ(result4["code"], 2);
  EXPECT_EQ(result4["message"], "Wrong call");
  EXPECT_FALSE(result4.contains("kvPairs"));

  FunctionBindingPayload<std::weak_ptr<RequestContext>> payload5{
      ioCorrect, GetRequestContext()};
  (*get_values_hook)(payload5);
  nlohmann::json result5 =
      nlohmann::json::parse(ioCorrect.output_string(), nullptr, false, true);
  ASSERT_FALSE(result5.is_discarded());
  EXPECT_EQ(result5["status"]["code"], 0);
  EXPECT_EQ(result5["status"]["message"], "ok");
  ASSERT_TRUE(result5.contains("kvPairs"));
  EXPECT_EQ(result5["kvPairs"].size(), 12);
}

}  // namespace
}  // namespace kv_server
