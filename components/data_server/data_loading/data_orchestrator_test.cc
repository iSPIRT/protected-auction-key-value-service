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

#include "components/data_server/data_loading/data_orchestrator.h"

#include <string>
#include <utility>
#include <vector>

#include "absl/log/log.h"
#include "absl/synchronization/notification.h"
#include "components/data/common/mocks.h"
#include "components/data/realtime/realtime_notifier.h"
#include "components/data_server/cache/cache.h"
#include "components/data_server/cache/mocks.h"
#include "components/udf/code_config.h"
#include "components/udf/mocks.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "public/constants.h"
#include "public/data_loading/filename_utils.h"
#include "public/data_loading/record_utils.h"
#include "public/sharding/key_sharder.h"
#include "public/sharding/sharding_function.h"
#include "public/test_util/data_record.h"
#include "public/test_util/mocks.h"
#include "public/test_util/proto_matcher.h"

namespace kv_server {
using testing::_;
using testing::AllOf;
using testing::ByMove;
using testing::Field;
using testing::Pair;
using testing::Return;
using testing::ReturnRef;
using testing::UnorderedElementsAre;

namespace {
// using google::protobuf::TextFormat;

BlobStorageClient::DataLocation GetTestLocation(
    const std::string& basename = "") {
  static constexpr absl::string_view kBucket = "testbucket";
  return BlobStorageClient::DataLocation{.bucket = std::string(kBucket),
                                         .key = basename};
}

class DataOrchestratorTest : public ::testing::Test {
 protected:
  void SetUp() override { kv_server::InitMetricsContextMap(); }
  DataOrchestratorTest()
      : options_(DataOrchestrator::Options{
            .data_bucket = GetTestLocation().bucket,
            .cache = cache_,
            .blob_client = blob_client_,
            .delta_notifier = notifier_,
            .change_notifier = change_notifier_,
            .udf_client = udf_client_,
            .delta_stream_reader_factory = delta_stream_reader_factory_,
            .realtime_thread_pool_manager = realtime_thread_pool_manager_,
            .key_sharder =
                kv_server::KeySharder(kv_server::ShardingFunction{/*seed=*/""}),
            .blob_prefix_allowlist = kv_server::BlobPrefixAllowlist(""),
            .log_context = log_context_
#if defined(MICROSOFT_AD_SELECTION_BUILD)
            ,
            .microsoft_ann_index = microsoft_ann_index_,
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
        }) {
  }

  MockBlobStorageClient blob_client_;
  MockDeltaFileNotifier notifier_;
  MockBlobStorageChangeNotifier change_notifier_;
  MockUdfClient udf_client_;
  MockStreamRecordReaderFactory delta_stream_reader_factory_;
  MockCache cache_;
  MockRealtimeThreadPoolManager realtime_thread_pool_manager_;
  DataOrchestrator::Options options_;
  privacy_sandbox::server_common::log::NoOpContext log_context_;
#if defined(MICROSOFT_AD_SELECTION_BUILD)
  kv_server::microsoft::ANNIndex microsoft_ann_index_;
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
};

TEST_F(DataOrchestratorTest, InitCacheListRetriesOnFailure) {
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
#if defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .Times(1)
      .WillOnce(Return(absl::UnknownError("list failed")));

  EXPECT_EQ(DataOrchestrator::TryCreate(options_).status(),
            absl::UnknownError("list failed"));
}

TEST_F(DataOrchestratorTest, InitCacheListSnapshotsFailure) {
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(absl::UnknownError("list snapshots failed")));
  EXPECT_EQ(DataOrchestrator::TryCreate(options_).status(),
            absl::UnknownError("list snapshots failed"));
}

TEST_F(DataOrchestratorTest, InitCacheNoFiles) {
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
#if defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .WillOnce(Return(std::vector<std::string>()));

  EXPECT_CALL(blob_client_, GetBlobReader).Times(0);

  EXPECT_TRUE(DataOrchestrator::TryCreate(options_).ok());
}

TEST_F(DataOrchestratorTest, InitCacheFilteroutInvalidFiles) {
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
#if defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .WillOnce(Return(std::vector<std::string>({"DELTA_01"})));

  EXPECT_CALL(blob_client_, GetBlobReader).Times(0);

  EXPECT_TRUE(DataOrchestrator::TryCreate(options_).ok());
}

TEST_F(DataOrchestratorTest, InitCacheFiltersDeltasUsingSnapshotEndingFile) {
  auto snapshot_name = ToSnapshotFileName(1);
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>({*snapshot_name})));
#if defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
  KVFileMetadata metadata;
  *metadata.mutable_snapshot()->mutable_starting_file() =
      ToDeltaFileName(1).value();
  *metadata.mutable_snapshot()->mutable_ending_delta_file() =
      ToDeltaFileName(5).value();
  auto record_reader1 = std::make_unique<MockStreamRecordReader>();
  EXPECT_CALL(*record_reader1, GetKVFileMetadata)
      .Times(1)
      .WillOnce(Return(metadata));
  auto record_reader2 = std::make_unique<MockStreamRecordReader>();
  EXPECT_CALL(*record_reader2, GetKVFileMetadata)
      .Times(1)
      .WillOnce(Return(metadata));
  EXPECT_CALL(delta_stream_reader_factory_, CreateConcurrentReader)
      .Times(2)
      .WillOnce(Return(ByMove(std::move(record_reader1))))
      .WillOnce(Return(ByMove(std::move(record_reader2))));

  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after,
                            ToDeltaFileName(5).value()),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_TRUE(DataOrchestrator::TryCreate(options_).ok());
}

TEST_F(DataOrchestratorTest, InitCache_SkipsInvalidKVMutation) {
  const std::vector<std::string> fnames({ToDeltaFileName(1).value()});
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
#if defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .WillOnce(Return(fnames));

  KVFileMetadata metadata;
  auto update_reader = std::make_unique<MockStreamRecordReader>();
  EXPECT_CALL(*update_reader, GetKVFileMetadata)
      .Times(1)
      .WillOnce(Return(metadata));

  flatbuffers::FlatBufferBuilder builder;
  const auto kv_mutation_fbs = CreateKeyValueMutationRecordDirect(
      builder,
      /*mutation_type=*/KeyValueMutationType::Update,
      /*logical_commit_time=*/0,
      /*key=*/nullptr,
      /*value_type=*/Value::StringValue,
      /*value=*/0);
  const auto data_record_fbs =
      CreateDataRecord(builder, /*record_type=*/Record::KeyValueMutationRecord,
                       kv_mutation_fbs.Union());
  builder.Finish(data_record_fbs);
  EXPECT_CALL(*update_reader, ReadStreamRecords)
      .Times(1)
      .WillOnce(
          [&builder](
              const std::function<absl::Status(std::string_view)>& callback) {
            callback(ToStringView(builder)).IgnoreError();
            return absl::OkStatus();
          });
  auto delete_reader = std::make_unique<MockStreamRecordReader>();
  EXPECT_CALL(*delete_reader, ReadStreamRecords).Times(0);
  EXPECT_CALL(delta_stream_reader_factory_, CreateConcurrentReader)
      .Times(1)
      .WillOnce(Return(ByMove(std::move(update_reader))));

  EXPECT_CALL(cache_, UpdateKeyValue).Times(0);

  auto maybe_orchestrator = DataOrchestrator::TryCreate(options_);
  ASSERT_TRUE(maybe_orchestrator.ok());

  const std::string last_basename = ToDeltaFileName(1).value();
  EXPECT_CALL(notifier_,
              Start(_, GetTestLocation(),
                    UnorderedElementsAre(Pair("", last_basename)), _))
      .WillOnce(Return(absl::UnknownError("")));
  EXPECT_FALSE((*maybe_orchestrator)->Start().ok());
}

TEST_F(DataOrchestratorTest, InitCacheSuccess) {
  const std::vector<std::string> fnames(
      {ToDeltaFileName(1).value(), ToDeltaFileName(2).value()});
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
#if defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .WillOnce(Return(fnames));

  KVFileMetadata metadata;
  auto update_reader = std::make_unique<MockStreamRecordReader>();
  EXPECT_CALL(*update_reader, GetKVFileMetadata)
      .Times(1)
      .WillOnce(Return(metadata));
  EXPECT_CALL(*update_reader, ReadStreamRecords)
      .Times(1)
      .WillOnce(
          [](const std::function<absl::Status(std::string_view)>& callback) {
            KeyValueMutationRecordT kv_mutation_record = {
                .mutation_type = KeyValueMutationType::Update,
                .logical_commit_time = 3,
                .key = "bar",
            };
            kv_mutation_record.value.Set(GetSimpleStringValue("bar value"));
            DataRecordT data_record =
                GetNativeDataRecord(std::move(kv_mutation_record));
            auto [fbs_buffer, serialized_string_view] = Serialize(data_record);
            callback(serialized_string_view).IgnoreError();
            return absl::OkStatus();
          });
  auto delete_reader = std::make_unique<MockStreamRecordReader>();
  EXPECT_CALL(*delete_reader, GetKVFileMetadata)
      .Times(1)
      .WillOnce(Return(metadata));
  EXPECT_CALL(*delete_reader, ReadStreamRecords)
      .Times(1)
      .WillOnce(
          [](const std::function<absl::Status(std::string_view)>& callback) {
            KeyValueMutationRecordT kv_mutation_record = {
                .mutation_type = KeyValueMutationType::Delete,
                .logical_commit_time = 3,
                .key = "bar",
            };
            kv_mutation_record.value.Set(StringValueT{.value = ""});
            DataRecordT data_record =
                GetNativeDataRecord(std::move(kv_mutation_record));
            auto [fbs_buffer, serialized_string_view] = Serialize(data_record);
            callback(serialized_string_view).IgnoreError();
            return absl::OkStatus();
          });
  EXPECT_CALL(delta_stream_reader_factory_, CreateConcurrentReader)
      .Times(2)
      .WillOnce(Return(ByMove(std::move(update_reader))))
      .WillOnce(Return(ByMove(std::move(delete_reader))));

  EXPECT_CALL(cache_, UpdateKeyValue(_, "bar", "bar value", 3, _)).Times(1);
  EXPECT_CALL(cache_, DeleteKey(_, "bar", 3, _)).Times(1);
  EXPECT_CALL(cache_, RemoveDeletedKeys(_, 3, _)).Times(2);

  auto maybe_orchestrator = DataOrchestrator::TryCreate(options_);
  ASSERT_TRUE(maybe_orchestrator.ok());

  const std::string last_basename = ToDeltaFileName(2).value();
  EXPECT_CALL(notifier_,
              Start(_, GetTestLocation(),
                    UnorderedElementsAre(Pair("", last_basename)), _))
      .WillOnce(Return(absl::UnknownError("")));
  EXPECT_FALSE((*maybe_orchestrator)->Start().ok());
}

TEST_F(DataOrchestratorTest, UpdateUdfCodeSuccess) {
  const std::vector<std::string> fnames({ToDeltaFileName(1).value()});
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .WillOnce(Return(std::vector<std::string>()));
#if defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .WillOnce(Return(fnames));

  KVFileMetadata metadata;
  auto reader = std::make_unique<MockStreamRecordReader>();
  EXPECT_CALL(*reader, GetKVFileMetadata).Times(1).WillOnce(Return(metadata));
  EXPECT_CALL(*reader, ReadStreamRecords)
      .WillOnce(
          [](const std::function<absl::Status(std::string_view)>& callback) {
            UserDefinedFunctionsConfigT udf_config_record = {
                .language = UserDefinedFunctionsLanguage::Javascript,
                .code_snippet = "function hello(){}",
                .handler_name = "hello",
                .logical_commit_time = 1,
                .version = 1,
            };
            DataRecordT data_record =
                GetNativeDataRecord(std::move(udf_config_record));
            auto [fbs_buffer, serialized_string_view] = Serialize(data_record);
            callback(serialized_string_view).IgnoreError();
            return absl::OkStatus();
          });
  EXPECT_CALL(delta_stream_reader_factory_, CreateConcurrentReader)
      .WillOnce(Return(ByMove(std::move(reader))));

  EXPECT_CALL(udf_client_, SetCodeObject(CodeConfig{.js = "function hello(){}",
                                                    .udf_handler_name = "hello",
                                                    .logical_commit_time = 1,
                                                    .version = 1},
                                         _))
      .WillOnce(Return(absl::OkStatus()));
  auto maybe_orchestrator = DataOrchestrator::TryCreate(options_);
  ASSERT_TRUE(maybe_orchestrator.ok());

  const std::string last_basename = ToDeltaFileName(1).value();
  EXPECT_CALL(notifier_,
              Start(_, GetTestLocation(),
                    UnorderedElementsAre(Pair("", last_basename)), _))
      .WillOnce(Return(absl::UnknownError("")));
  EXPECT_FALSE((*maybe_orchestrator)->Start().ok());
}

TEST_F(DataOrchestratorTest, UpdateUdfCodeWithWasmBinSuccess) {
  const std::vector<std::string> fnames({ToDeltaFileName(1).value()});
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .WillOnce(Return(std::vector<std::string>()));
#if defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .WillOnce(Return(fnames));

  KVFileMetadata metadata;
  auto reader = std::make_unique<MockStreamRecordReader>();
  EXPECT_CALL(*reader, GetKVFileMetadata).Times(1).WillOnce(Return(metadata));
  EXPECT_CALL(*reader, ReadStreamRecords)
      .WillOnce(
          [](const std::function<absl::Status(std::string_view)>& callback) {
            UserDefinedFunctionsConfigT udf_config_record = {
                .language = UserDefinedFunctionsLanguage::Javascript,
                .code_snippet = "function hello(){}",
                .handler_name = "hello",
                .logical_commit_time = 1,
                .version = 1,
                // This is not an accurate example of a
                // byte string that should be in wasm_bin
                .wasm_bin = "abc",
            };
            DataRecordT data_record =
                GetNativeDataRecord(std::move(udf_config_record));
            auto [fbs_buffer, serialized_string_view] = Serialize(data_record);
            callback(serialized_string_view).IgnoreError();
            return absl::OkStatus();
          });
  EXPECT_CALL(delta_stream_reader_factory_, CreateConcurrentReader)
      .WillOnce(Return(ByMove(std::move(reader))));

  EXPECT_CALL(udf_client_, SetCodeObject(CodeConfig{.js = "function hello(){}",
                                                    .udf_handler_name = "hello",
                                                    .logical_commit_time = 1,
                                                    .version = 1,
                                                    .wasm_bin = "abc"},
                                         _))
      .WillOnce(Return(absl::OkStatus()));
  auto maybe_orchestrator = DataOrchestrator::TryCreate(options_);
  ASSERT_TRUE(maybe_orchestrator.ok());

  const std::string last_basename = ToDeltaFileName(1).value();
  EXPECT_CALL(notifier_,
              Start(_, GetTestLocation(),
                    UnorderedElementsAre(Pair("", last_basename)), _))
      .WillOnce(Return(absl::UnknownError("")));
  EXPECT_FALSE((*maybe_orchestrator)->Start().ok());
}

TEST_F(DataOrchestratorTest, UpdateUdfCodeFails_OrchestratorContinues) {
  const std::vector<std::string> fnames({ToDeltaFileName(1).value()});
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .WillOnce(Return(std::vector<std::string>()));
#if defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .WillOnce(Return(fnames));

  KVFileMetadata metadata;
  auto reader = std::make_unique<MockStreamRecordReader>();
  EXPECT_CALL(*reader, GetKVFileMetadata).Times(1).WillOnce(Return(metadata));
  EXPECT_CALL(*reader, ReadStreamRecords)
      .WillOnce(
          [](const std::function<absl::Status(std::string_view)>& callback) {
            UserDefinedFunctionsConfigT udf_config_record = {
                .language = UserDefinedFunctionsLanguage::Javascript,
                .code_snippet = "function hello(){}",
                .handler_name = "hello",
                .logical_commit_time = 1,
                .version = 1,
            };
            DataRecordT data_record =
                GetNativeDataRecord(std::move(udf_config_record));
            auto [fbs_buffer, serialized_string_view] = Serialize(data_record);
            callback(serialized_string_view).IgnoreError();
            return absl::OkStatus();
          });
  EXPECT_CALL(delta_stream_reader_factory_, CreateConcurrentReader)
      .WillOnce(Return(ByMove(std::move(reader))));

  EXPECT_CALL(udf_client_, SetCodeObject(CodeConfig{.js = "function hello(){}",
                                                    .udf_handler_name = "hello",
                                                    .logical_commit_time = 1,
                                                    .version = 1},
                                         _))
      .WillOnce(Return(absl::UnknownError("Some error.")));
  auto maybe_orchestrator = DataOrchestrator::TryCreate(options_);
  ASSERT_TRUE(maybe_orchestrator.ok());

  const std::string last_basename = ToDeltaFileName(1).value();
  EXPECT_CALL(notifier_,
              Start(_, GetTestLocation(),
                    UnorderedElementsAre(Pair("", last_basename)), _))
      .WillOnce(Return(absl::UnknownError("")));
  EXPECT_FALSE((*maybe_orchestrator)->Start().ok());
}

TEST_F(DataOrchestratorTest, StartLoading) {
  ON_CALL(blob_client_, ListBlobs)
      .WillByDefault(Return(std::vector<std::string>({})));
  auto maybe_orchestrator = DataOrchestrator::TryCreate(options_);
  ASSERT_TRUE(maybe_orchestrator.ok());
  auto orchestrator = std::move(maybe_orchestrator.value());

  const std::string last_basename = "";
  EXPECT_CALL(notifier_,
              Start(_, GetTestLocation(),
                    absl::flat_hash_map<std::string, std::string>(), _))
      .WillOnce([](BlobStorageChangeNotifier&, BlobStorageClient::DataLocation,
                   absl::flat_hash_map<std::string, std::string>,
                   std::function<void(const std::string& key)> callback) {
        callback(ToDeltaFileName(6).value());
        callback(ToDeltaFileName(7).value());
        LOG(INFO) << "Notified 2 files";
        return absl::OkStatus();
      });

  EXPECT_CALL(notifier_, IsRunning).Times(1).WillOnce(Return(true));
  EXPECT_CALL(notifier_, Stop()).Times(1).WillOnce(Return(absl::OkStatus()));

  absl::Notification all_records_loaded;
  KVFileMetadata metadata;
  auto update_reader = std::make_unique<MockStreamRecordReader>();
  EXPECT_CALL(*update_reader, GetKVFileMetadata)
      .Times(1)
      .WillOnce(Return(metadata));
  EXPECT_CALL(*update_reader, ReadStreamRecords)
      .Times(1)
      .WillOnce(
          [](const std::function<absl::Status(std::string_view)>& callback) {
            KeyValueMutationRecordT kv_mutation_record = {
                .mutation_type = KeyValueMutationType::Update,
                .logical_commit_time = 3,
                .key = "bar",
            };
            kv_mutation_record.value.Set(GetSimpleStringValue("bar value"));
            DataRecordT data_record =
                GetNativeDataRecord(std::move(kv_mutation_record));
            auto [fbs_buffer, serialized_string_view] = Serialize(data_record);
            callback(serialized_string_view).IgnoreError();
            return absl::OkStatus();
          });
  auto delete_reader = std::make_unique<MockStreamRecordReader>();
  EXPECT_CALL(*delete_reader, GetKVFileMetadata)
      .Times(1)
      .WillOnce(Return(metadata));
  EXPECT_CALL(*delete_reader, ReadStreamRecords)
      .Times(1)
      .WillOnce(
          [&all_records_loaded](
              const std::function<absl::Status(std::string_view)>& callback) {
            KeyValueMutationRecordT kv_mutation_record = {
                .mutation_type = KeyValueMutationType::Delete,
                .logical_commit_time = 3,
                .key = "bar",
            };
            kv_mutation_record.value.Set(StringValueT{.value = ""});
            DataRecordT data_record =
                GetNativeDataRecord(std::move(kv_mutation_record));
            auto [fbs_buffer, serialized_string_view] = Serialize(data_record);
            callback(serialized_string_view).IgnoreError();
            all_records_loaded.Notify();
            return absl::OkStatus();
          });
  EXPECT_CALL(delta_stream_reader_factory_, CreateConcurrentReader)
      .Times(2)
      .WillOnce(Return(ByMove(std::move(update_reader))))
      .WillOnce(Return(ByMove(std::move(delete_reader))));

  EXPECT_CALL(cache_, UpdateKeyValue(_, "bar", "bar value", 3, _)).Times(1);
  EXPECT_CALL(cache_, DeleteKey(_, "bar", 3, _)).Times(1);
  EXPECT_CALL(cache_, RemoveDeletedKeys(_, 3, _)).Times(2);

  EXPECT_TRUE(orchestrator->Start().ok());
  LOG(INFO) << "Created ContinuouslyLoadNewData";
  all_records_loaded.WaitForNotificationWithTimeout(absl::Seconds(10));
}

TEST_F(DataOrchestratorTest, CreateOrchestratorWithRealtimeDisabled) {
  ON_CALL(blob_client_, ListBlobs)
      .WillByDefault(Return(std::vector<std::string>({})));
  auto maybe_orchestrator = DataOrchestrator::TryCreate(options_);
  ASSERT_TRUE(maybe_orchestrator.ok());
}

TEST_F(DataOrchestratorTest, InitCacheShardedSuccessSkipRecord) {
  testing::StrictMock<MockCache> strict_cache;

  const std::vector<std::string> fnames(
      {ToDeltaFileName(1).value(), ToDeltaFileName(2).value()});
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
#if defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .WillOnce(Return(fnames));

  KVFileMetadata metadata;
  auto update_reader = std::make_unique<MockStreamRecordReader>();
  EXPECT_CALL(*update_reader, GetKVFileMetadata)
      .Times(1)
      .WillOnce(Return(metadata));
  EXPECT_CALL(*update_reader, ReadStreamRecords)
      .Times(1)
      .WillOnce(
          [](const std::function<absl::Status(std::string_view)>& callback) {
            // key: "shard1" -> shard num: 0
            KeyValueMutationRecordT kv_mutation_record = {
                .mutation_type = KeyValueMutationType::Update,
                .logical_commit_time = 3,
                .key = "shard1",
            };
            kv_mutation_record.value.Set(GetSimpleStringValue("bar value"));
            DataRecordT data_record =
                GetNativeDataRecord(std::move(kv_mutation_record));
            auto [fbs_buffer, serialized_string_view] = Serialize(data_record);
            callback(serialized_string_view).IgnoreError();
            return absl::OkStatus();
          });
  auto delete_reader = std::make_unique<MockStreamRecordReader>();
  EXPECT_CALL(*delete_reader, GetKVFileMetadata)
      .Times(1)
      .WillOnce(Return(metadata));
  EXPECT_CALL(*delete_reader, ReadStreamRecords)
      .Times(1)
      .WillOnce(
          [](const std::function<absl::Status(std::string_view)>& callback) {
            // key: "shard2" -> shard num: 1
            KeyValueMutationRecordT kv_mutation_record = {
                .mutation_type = KeyValueMutationType::Delete,
                .logical_commit_time = 3,
                .key = "shard2",
            };
            kv_mutation_record.value.Set(StringValueT{.value = ""});
            DataRecordT data_record =
                GetNativeDataRecord(std::move(kv_mutation_record));
            auto [fbs_buffer, serialized_string_view] = Serialize(data_record);
            callback(serialized_string_view).IgnoreError();
            return absl::OkStatus();
          });
  EXPECT_CALL(delta_stream_reader_factory_, CreateConcurrentReader)
      .Times(2)
      .WillOnce(Return(ByMove(std::move(update_reader))))
      .WillOnce(Return(ByMove(std::move(delete_reader))));

  EXPECT_CALL(strict_cache, RemoveDeletedKeys(_, 0, _)).Times(1);
  EXPECT_CALL(strict_cache, DeleteKey(_, "shard2", 3, _)).Times(1);
  EXPECT_CALL(strict_cache, RemoveDeletedKeys(_, 3, _)).Times(1);

  auto sharded_options = DataOrchestrator::Options{
      .data_bucket = GetTestLocation().bucket,
      .cache = strict_cache,
      .blob_client = blob_client_,
      .delta_notifier = notifier_,
      .change_notifier = change_notifier_,
      .udf_client = udf_client_,
      .delta_stream_reader_factory = delta_stream_reader_factory_,
      .realtime_thread_pool_manager = realtime_thread_pool_manager_,
      .shard_num = 1,
      .num_shards = 2,
      .key_sharder =
          kv_server::KeySharder(kv_server::ShardingFunction{/*seed=*/""}),
      .blob_prefix_allowlist = BlobPrefixAllowlist(""),
      .log_context = log_context_
#if defined(MICROSOFT_AD_SELECTION_BUILD)
      ,
      .microsoft_ann_index = microsoft_ann_index_,
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
  };

  auto maybe_orchestrator = DataOrchestrator::TryCreate(sharded_options);
  ASSERT_TRUE(maybe_orchestrator.ok());
}

TEST_F(DataOrchestratorTest, InitCacheSkipsSnapshotFilesForOtherShards) {
  auto snapshot_name = ToSnapshotFileName(1);
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .WillOnce(Return(std::vector<std::string>({*snapshot_name})));
#if defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
  KVFileMetadata metadata;
  *metadata.mutable_snapshot()->mutable_starting_file() =
      ToDeltaFileName(1).value();
  *metadata.mutable_snapshot()->mutable_ending_delta_file() =
      ToDeltaFileName(5).value();
  metadata.mutable_sharding_metadata()->set_shard_num(17);
  auto record_reader1 = std::make_unique<MockStreamRecordReader>();
  EXPECT_CALL(*record_reader1, GetKVFileMetadata)
      .Times(1)
      .WillOnce(Return(metadata));
  EXPECT_CALL(delta_stream_reader_factory_, CreateConcurrentReader)
      .Times(1)
      .WillOnce(Return(ByMove(std::move(record_reader1))));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_TRUE(DataOrchestrator::TryCreate(options_).ok());
}

TEST_F(DataOrchestratorTest, VerifyLoadingDataFromPrefixes) {
  for (auto file_type : {FilePrefix<FileType::DELTA>(),
#if defined(MICROSOFT_AD_SELECTION_BUILD)
                         FilePrefix<FileType::ANNSNAPSHOT>(),
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
                         FilePrefix<FileType::SNAPSHOT>()}) {
    for (auto prefix : {"", "prefix1", "prefix2"}) {
      EXPECT_CALL(
          blob_client_,
          ListBlobs(
              BlobStorageClient::DataLocation{.bucket = "testbucket",
                                              .prefix = prefix},
              AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                    Field(&BlobStorageClient::ListOptions::prefix, file_type))))
          .WillOnce(Return(std::vector<std::string>({})));
    }
  }
  auto options = options_;
  options.blob_prefix_allowlist = BlobPrefixAllowlist("prefix1,prefix2");
  auto maybe_orchestrator = DataOrchestrator::TryCreate(options);
  ASSERT_TRUE(maybe_orchestrator.ok());
}

#if defined(MICROSOFT_AD_SELECTION_BUILD)
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

const std::vector<unsigned char> valid_snapshot_bytes_ = {
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

TEST_F(DataOrchestratorTest, MICROSOFT_InitCacheListAnnSnapshotsFailure) {
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(absl::UnknownError("list ann snapshots failed")));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_TRUE(DataOrchestrator::TryCreate(options_).ok());
}

TEST_F(DataOrchestratorTest, MICROSOFT_InitAnnSnapshotFilteroutInvalidFiles) {
  auto folder_path = rand_string();
  std::string filename = "ANNSNAPSHOT_01";
  auto file_path = options_.data_bucket + "/" + folder_path + "/" + filename;
  auto location = BlobStorageClient::DataLocation{
      .bucket = options_.data_bucket, .prefix = folder_path};
  std::filesystem::create_directories(options_.data_bucket + "/" + folder_path);
  DumpFile(file_path, valid_snapshot_bytes_);
  options_.blob_prefix_allowlist = kv_server::BlobPrefixAllowlist(folder_path);
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(location,
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(location,
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>({filename})));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(location,
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .WillOnce(Return(std::vector<std::string>()));

  EXPECT_TRUE(DataOrchestrator::TryCreate(options_).ok());

  // empty result means no active snapshots
  EXPECT_EQ(microsoft_ann_index_.GetKeyValueSet({"1", "2", "3"}), nullptr);
}

TEST_F(DataOrchestratorTest, MICROSOFT_AnnSnapshotLoadsCorrect) {
  auto folder_path = rand_string();
  std::string filename = "ANNSNAPSHOT_0000000000000001";
  auto file_path = options_.data_bucket + "/" + folder_path + "/" + filename;
  auto location = BlobStorageClient::DataLocation{
      .bucket = options_.data_bucket, .prefix = folder_path};
  std::filesystem::create_directories(options_.data_bucket + "/" + folder_path);
  DumpFile(file_path, valid_snapshot_bytes_);
  options_.blob_prefix_allowlist = kv_server::BlobPrefixAllowlist(folder_path);
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(location,
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(location,
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>({filename})));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(location,
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .WillOnce(Return(std::vector<std::string>()));

  EXPECT_TRUE(DataOrchestrator::TryCreate(options_).ok());

  ASSERT_NE(microsoft_ann_index_.GetKeyValueSet({"1", "2", "3"}), nullptr);
  EXPECT_EQ(microsoft_ann_index_.GetKeyValueSet({"1", "2", "3"})->size(), 3);
}

TEST_F(DataOrchestratorTest, MICROSOFT_AnnSnapshotSkipInvalidState) {
  auto folder_path = rand_string();
  std::string filename = "ANNSNAPSHOT_0000000000000001";
  auto file_path = options_.data_bucket + "/" + folder_path + "/" + filename;
  auto location = BlobStorageClient::DataLocation{
      .bucket = options_.data_bucket, .prefix = folder_path};
  std::filesystem::create_directories(options_.data_bucket + "/" + folder_path);
  DumpFile(file_path, {0, 0, 0, 0});
  options_.blob_prefix_allowlist = kv_server::BlobPrefixAllowlist(folder_path);
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(location,
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(location,
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>({filename})));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(location,
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .WillOnce(Return(std::vector<std::string>()));

  EXPECT_TRUE(DataOrchestrator::TryCreate(options_).ok());

  // empty result means no active snapshots
  EXPECT_EQ(microsoft_ann_index_.GetKeyValueSet({"1", "2", "3"}), nullptr);
}

TEST_F(DataOrchestratorTest, MICROSOFT_AnnSnapshotArrivesDuringRuntimeSuccess) {
  auto folder_path = rand_string();
  std::string filename = "ANNSNAPSHOT_0000000000000001";
  auto file_path = options_.data_bucket + "/" + folder_path + "/" + filename;
  auto location = BlobStorageClient::DataLocation{
      .bucket = options_.data_bucket, .prefix = folder_path};
  std::filesystem::create_directories(options_.data_bucket + "/" + folder_path);
  DumpFile(file_path, valid_snapshot_bytes_);
  options_.blob_prefix_allowlist = kv_server::BlobPrefixAllowlist(folder_path);
  ON_CALL(blob_client_, ListBlobs)
      .WillByDefault(Return(std::vector<std::string>({})));

  auto maybe_orchestrator = DataOrchestrator::TryCreate(options_);
  ASSERT_TRUE(maybe_orchestrator.ok());
  auto orchestrator = std::move(maybe_orchestrator.value());

  // empty result means no active snapshots
  EXPECT_EQ(microsoft_ann_index_.GetKeyValueSet({"1", "2", "3"}), nullptr);

  absl::Notification all_records_loaded;
  EXPECT_CALL(
      notifier_,
      Start(_, BlobStorageClient::DataLocation{.bucket = options_.data_bucket},
            absl::flat_hash_map<std::string, std::string>(), _))
      .WillOnce([fn = folder_path + "/" + filename, &all_records_loaded](
                    BlobStorageChangeNotifier&, BlobStorageClient::DataLocation,
                    absl::flat_hash_map<std::string, std::string>,
                    std::function<void(const std::string& key)> callback) {
        callback(fn);
        all_records_loaded.Notify();
        LOG(INFO) << "Notified 1 files";
        return absl::OkStatus();
      });

  EXPECT_CALL(notifier_, IsRunning).Times(1).WillOnce(Return(true));
  EXPECT_CALL(notifier_, Stop()).Times(1).WillOnce(Return(absl::OkStatus()));

  EXPECT_TRUE(orchestrator->Start().ok());
  LOG(INFO) << "Created ContinuouslyLoadNewData";
  all_records_loaded.WaitForNotificationWithTimeout(absl::Seconds(10));
  absl::SleepFor(absl::Seconds(1));

  // data loaded
  ASSERT_NE(microsoft_ann_index_.GetKeyValueSet({"1", "2", "3"}), nullptr);
  EXPECT_EQ(microsoft_ann_index_.GetKeyValueSet({"1", "2", "3"})->size(), 3);
}

TEST_F(DataOrchestratorTest,
       MICROSOFT_AnnSnapshotArrivesDuringRuntimeSkipInvalidName) {
  auto folder_path = rand_string();
  std::string filename = "ANNSNAPSHOT_01";
  auto file_path = options_.data_bucket + "/" + folder_path + "/" + filename;
  auto location = BlobStorageClient::DataLocation{
      .bucket = options_.data_bucket, .prefix = folder_path};
  std::filesystem::create_directories(options_.data_bucket + "/" + folder_path);
  DumpFile(file_path, valid_snapshot_bytes_);
  options_.blob_prefix_allowlist = kv_server::BlobPrefixAllowlist(folder_path);
  ON_CALL(blob_client_, ListBlobs)
      .WillByDefault(Return(std::vector<std::string>({})));

  auto maybe_orchestrator = DataOrchestrator::TryCreate(options_);
  ASSERT_TRUE(maybe_orchestrator.ok());
  auto orchestrator = std::move(maybe_orchestrator.value());

  // empty result means no active snapshots
  EXPECT_EQ(microsoft_ann_index_.GetKeyValueSet({"1", "2", "3"}), nullptr);

  absl::Notification all_records_loaded;
  EXPECT_CALL(
      notifier_,
      Start(_, BlobStorageClient::DataLocation{.bucket = options_.data_bucket},
            absl::flat_hash_map<std::string, std::string>(), _))
      .WillOnce([fn = folder_path + "/" + filename, &all_records_loaded](
                    BlobStorageChangeNotifier&, BlobStorageClient::DataLocation,
                    absl::flat_hash_map<std::string, std::string>,
                    std::function<void(const std::string& key)> callback) {
        callback(fn);
        all_records_loaded.Notify();
        LOG(INFO) << "Notified 1 files";
        return absl::OkStatus();
      });

  EXPECT_CALL(notifier_, IsRunning).Times(1).WillOnce(Return(true));
  EXPECT_CALL(notifier_, Stop()).Times(1).WillOnce(Return(absl::OkStatus()));

  EXPECT_TRUE(orchestrator->Start().ok());
  LOG(INFO) << "Created ContinuouslyLoadNewData";
  all_records_loaded.WaitForNotificationWithTimeout(absl::Seconds(10));
  absl::SleepFor(absl::Seconds(1));

  // data NOT loaded
  ASSERT_EQ(microsoft_ann_index_.GetKeyValueSet({"1", "2", "3"}), nullptr);
}

TEST_F(DataOrchestratorTest,
       MICROSOFT_AnnSnapshotArrivesDuringRuntimeSkipInvalidIndex) {
  auto folder_path = rand_string();
  std::string filename = "ANNSNAPSHOT_0000000000000001";
  auto file_path = options_.data_bucket + "/" + folder_path + "/" + filename;
  auto location = BlobStorageClient::DataLocation{
      .bucket = options_.data_bucket, .prefix = folder_path};
  std::filesystem::create_directories(options_.data_bucket + "/" + folder_path);
  DumpFile(file_path, {0, 0, 0, 0});
  options_.blob_prefix_allowlist = kv_server::BlobPrefixAllowlist(folder_path);
  ON_CALL(blob_client_, ListBlobs)
      .WillByDefault(Return(std::vector<std::string>({})));

  auto maybe_orchestrator = DataOrchestrator::TryCreate(options_);
  ASSERT_TRUE(maybe_orchestrator.ok());
  auto orchestrator = std::move(maybe_orchestrator.value());

  // empty result means no active snapshots
  EXPECT_EQ(microsoft_ann_index_.GetKeyValueSet({"1", "2", "3"}), nullptr);

  absl::Notification all_records_loaded;
  EXPECT_CALL(
      notifier_,
      Start(_, BlobStorageClient::DataLocation{.bucket = options_.data_bucket},
            absl::flat_hash_map<std::string, std::string>(), _))
      .WillOnce([fn = folder_path + "/" + filename, &all_records_loaded](
                    BlobStorageChangeNotifier&, BlobStorageClient::DataLocation,
                    absl::flat_hash_map<std::string, std::string>,
                    std::function<void(const std::string& key)> callback) {
        callback(fn);
        all_records_loaded.Notify();
        LOG(INFO) << "Notified 1 files";
        return absl::OkStatus();
      });

  EXPECT_CALL(notifier_, IsRunning).Times(1).WillOnce(Return(true));
  EXPECT_CALL(notifier_, Stop()).Times(1).WillOnce(Return(absl::OkStatus()));

  EXPECT_TRUE(orchestrator->Start().ok());
  LOG(INFO) << "Created ContinuouslyLoadNewData";
  all_records_loaded.WaitForNotificationWithTimeout(absl::Seconds(10));
  absl::SleepFor(absl::Seconds(1));

  // data NOT loaded
  ASSERT_EQ(microsoft_ann_index_.GetKeyValueSet({"1", "2", "3"}), nullptr);
}

TEST_F(DataOrchestratorTest, MICROSOFT_AnnSnapshotLoadsLastValidState) {
  auto folder_path = rand_string();
  std::string filename1 = "ANNSNAPSHOT_0000000000000001";
  std::string filename2 = "ANNSNAPSHOT_0000000000000002";
  std::string filename3 = "ANNSNAPSHOT_0000000000000003";
  auto file_path1 = options_.data_bucket + "/" + folder_path + "/" + filename1;
  auto file_path2 = options_.data_bucket + "/" + folder_path + "/" + filename2;
  auto file_path3 = options_.data_bucket + "/" + folder_path + "/" + filename3;
  auto location = BlobStorageClient::DataLocation{
      .bucket = options_.data_bucket, .prefix = folder_path};
  std::filesystem::create_directories(options_.data_bucket + "/" + folder_path);
  DumpFile(file_path1, valid_snapshot_bytes_);
  DumpFile(
      file_path2,
      {
          237, 254, 13,  240, 4,   0,   0,   0,   5,   0,   0,   0,   105, 110,
          100, 101, 120, 204, 0,   0,   0,   0,   0,   0,   0,   204, 0,   0,
          0,   0,   0,   0,   0,   5,   0,   0,   0,   7,   0,   0,   0,   0,
          0,   0,   0,   0,   0,   0,   0,   5,   0,   0,   0,   7,   0,   0,
          0,   2,   0,   0,   0,   3,   0,   0,   0,   5,   0,   0,   0,   8,
          0,   0,   0,   3,   0,   0,   0,   7,   0,   0,   0,   2,   0,   0,
          0,   6,   0,   0,   0,   3,   0,   0,   0,   1,   0,   0,   0,   0,
          0,   0,   0,   4,   0,   0,   0,   5,   0,   0,   0,   7,   0,   0,
          0,   0,   0,   0,   0,   5,   0,   0,   0,   6,   0,   0,   0,   8,
          0,   0,   0,   3,   0,   0,   0,   2,   0,   0,   0,   5,   0,   0,
          0,   9,   0,   0,   0,   5,   0,   0,   0,   7,   0,   0,   0,   3,
          0,   0,   0,   0,   0,   0,   0,   4,   0,   0,   0,   8,   0,   0,
          0,   2,   0,   0,   0,   1,   0,   0,   0,   3,   0,   0,   0,   4,
          0,   0,   0,   5,   0,   0,   0,   1,   0,   0,   0,   3,   0,   0,
          0,   9,   0,   0,   0,   3,   0,   0,   0,   3,   0,   0,   0,   0,
          0,   0,   0,   5,   0,   0,   0,   2,   0,   0,   0,   4,   0,   0,
          0,   7,   0,   0,   0,   10,  0,   0,   0,   105, 110, 100, 101, 120,
          46,  100, 97,  116, 97,  28,  0,   0,   0,   0,   0,   0,   0,   10,
          0,   0,   0,   2,   0,   0,   0,   175, 213, 107, 53,  86,  55,  236,
          132, 21,  58,  146, 115, 143, 15,  141, 106, 215, 171, 25,  111, 7,
          0,   0,   0,   109, 97,  112, 112, 105, 110, 103, 54,  0,   0,   0,
          0,   0,   0,   0,   10,  0,   0,   0,   1,   0,   0,   0,   48,  1,
          0,   0,   0,   49,  1,   0,   0,   0,   50,  1,   0,   0,   0,   51,
          1,   0,   0,   0,   52,  1,   0,   0,   0,   53,  1,   0,   0,   0,
          54,  1,   0,   0,   0,   55,  1,   0,   0,   0,   56,  1,   0,   0,
          0,   57,  11,  0,   0,   0,   99,  111, 110, 102, 105, 103, 46,  106,
          115, 111, 110, 83,  0,   0,   0,   0,   0,   0,   0,   123, 34,  68,
          105, 109, 101, 110, 115, 105, 111, 110, 34,  58,  32,  50,  44,  32,
          34,  81,  117, 101, 114, 121, 78,  101, 105, 103, 104, 98,  111, 114,
          115, 67,  111, 117, 110, 116, 34,  58,  32,  50,  44,  32,  34,  84,
          111, 112, 67,  111, 117, 110, 116, 34,  58,  32,  50,  44,  32,  34,
          86,  101, 99,  116, 111, 114, 84,  121, 112, 101, 83,  116, 114, 34,
          58,  32,  34,  117, 105, 110, 116, 56,  34,  125,
      });
  DumpFile(file_path3, {0, 0, 0, 0, 0});
  options_.blob_prefix_allowlist = kv_server::BlobPrefixAllowlist(folder_path);
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(location,
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::SNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(location,
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::ANNSNAPSHOT>()))))
      .Times(1)
      .WillOnce(
          Return(std::vector<std::string>({filename1, filename2, filename3})));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(GetTestLocation(),
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .WillOnce(Return(std::vector<std::string>()));
  EXPECT_CALL(
      blob_client_,
      ListBlobs(location,
                AllOf(Field(&BlobStorageClient::ListOptions::start_after, ""),
                      Field(&BlobStorageClient::ListOptions::prefix,
                            FilePrefix<FileType::DELTA>()))))
      .WillOnce(Return(std::vector<std::string>()));

  EXPECT_TRUE(DataOrchestrator::TryCreate(options_).ok());

  // in case of first index it should be other results
  auto result = microsoft_ann_index_.GetKeyValueSet({"11", "AA", "~\n"});
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(result->size(), 3);
  ASSERT_EQ(result->at("11").size(), 2);
  EXPECT_EQ(result->at("11")[0], "4");
  EXPECT_EQ(result->at("11")[1], "2");
  ASSERT_EQ(result->at("AA").size(), 2);
  EXPECT_EQ(result->at("AA")[0], "2");
  EXPECT_EQ(result->at("AA")[1], "1");
  ASSERT_EQ(result->at("~\n").size(), 2);
  EXPECT_EQ(result->at("~\n")[0], "6");
  EXPECT_EQ(result->at("~\n")[1], "1");
}
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)

}  // namespace
}  // namespace kv_server
