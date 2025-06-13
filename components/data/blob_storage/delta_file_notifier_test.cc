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

#include "components/data/blob_storage/delta_file_notifier.h"

#include <string>
#include <utility>
#include <vector>

#include "absl/synchronization/notification.h"
#include "components/data/common/mocks.h"
#include "components/telemetry/server_definition.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "public/data_loading/filename_utils.h"
#include "src/util/sleep/sleepfor_mock.h"

using testing::_;
using testing::AllOf;
using testing::Field;
using testing::Return;

namespace kv_server {
namespace {

using ::privacy_sandbox::server_common::MockSleepFor;
using privacy_sandbox::server_common::SimulatedSteadyClock;

constexpr std::string_view kBlobPrefix1 = "prefix1";

class DeltaFileNotifierTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::unique_ptr<MockSleepFor> mock_sleep_for =
        std::make_unique<MockSleepFor>();
    sleep_for_ = mock_sleep_for.get();
    notifier_ = DeltaFileNotifier::Create(client_, poll_frequency_,
                                          std::move(mock_sleep_for), sim_clock_,
                                          BlobPrefixAllowlist(kBlobPrefix1));
  }

  MockBlobStorageClient client_;
  std::unique_ptr<DeltaFileNotifier> notifier_;
  MockBlobStorageChangeNotifier change_notifier_;
  std::string initial_key_ = ToDeltaFileName(1).value();
  MockSleepFor* sleep_for_;
  SimulatedSteadyClock sim_clock_;
  absl::Duration poll_frequency_ = absl::Minutes(5);
};

TEST_F(DeltaFileNotifierTest, NotRunning) {
  ASSERT_FALSE(notifier_->IsRunning());
}

TEST_F(DeltaFileNotifierTest, StartFailure) {
  BlobStorageClient::DataLocation location = {.bucket = "testbucket"};
  absl::Status status = notifier_->Start(
      change_notifier_, {.bucket = "testbucket"},
      {std::make_pair("", initial_key_)}, [](const std::string&) {});
  ASSERT_TRUE(status.ok());
  status = notifier_->Start(change_notifier_, {.bucket = "testbucket"},
                            {std::make_pair("", initial_key_)},
                            [](const std::string&) {});
  ASSERT_FALSE(status.ok());
}

TEST_F(DeltaFileNotifierTest, StartsAndStops) {
  BlobStorageClient::DataLocation location = {.bucket = "testbucket"};
  absl::Status status = notifier_->Start(
      change_notifier_, {.bucket = "testbucket"},
      {std::make_pair("", initial_key_)}, [](const std::string&) {});
  ASSERT_TRUE(status.ok());
  EXPECT_TRUE(notifier_->IsRunning());
  status = notifier_->Stop();
  ASSERT_TRUE(status.ok());
  EXPECT_FALSE(notifier_->IsRunning());
}

TEST_F(DeltaFileNotifierTest, NotifiesWithNewFiles) {
  BlobStorageClient::DataLocation location = {.bucket = "testbucket"};
  EXPECT_CALL(change_notifier_, GetNotifications(_, _))
      .WillOnce(Return(std::vector<std::string>({ToDeltaFileName(3).value()})))
      .WillOnce(Return(std::vector<std::string>({ToDeltaFileName(4).value()})))
      .WillRepeatedly(Return(std::vector<std::string>()));
#if defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      client_,
      ListBlobs(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::ListOptions::start_after, "")))
      .WillRepeatedly(Return(std::vector<std::string>()));
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      client_,
      ListBlobs(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::ListOptions::start_after,
                      ToDeltaFileName(1).value())))
      .WillOnce(Return(std::vector<std::string>({})))
      .WillOnce(Return(std::vector<std::string>({ToDeltaFileName(3).value()})));
  EXPECT_CALL(
      client_,
      ListBlobs(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::ListOptions::start_after,
                      ToDeltaFileName(3).value())))
      .WillOnce(Return(std::vector<std::string>({ToDeltaFileName(4).value()})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          AllOf(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::DataLocation::prefix, kBlobPrefix1)),
          Field(&BlobStorageClient::ListOptions::start_after, "")))
      .WillRepeatedly(Return(std::vector<std::string>()));

  absl::Notification finished;
  testing::MockFunction<void(const std::string& record)> callback;
  EXPECT_CALL(callback, Call)
      .Times(2)
      .WillOnce([&](const std::string& key) {
        EXPECT_EQ(key, ToDeltaFileName(3).value());
      })
      .WillOnce([&](const std::string& key) {
        EXPECT_EQ(key, ToDeltaFileName(4).value());
        finished.Notify();
      });

  absl::Status status = notifier_->Start(
      change_notifier_, {.bucket = "testbucket"},
      {std::make_pair("", initial_key_)}, callback.AsStdFunction());
  ASSERT_TRUE(status.ok());
  EXPECT_TRUE(notifier_->IsRunning());
  finished.WaitForNotification();
  status = notifier_->Stop();
  ASSERT_TRUE(status.ok());
  EXPECT_FALSE(notifier_->IsRunning());
}

TEST_F(DeltaFileNotifierTest, NotifiesWithInvalidFilesIngored) {
  BlobStorageClient::DataLocation location = {.bucket = "testbucket"};
  std::string invalid_delta_name = "DELTA_5";
  EXPECT_CALL(change_notifier_, GetNotifications(_, _))
      .WillOnce(Return(std::vector<std::string>({ToDeltaFileName(3).value()})))
      .WillOnce(Return(std::vector<std::string>({invalid_delta_name})))
      .WillOnce(Return(std::vector<std::string>({ToDeltaFileName(4).value()})))
      .WillOnce(Return(std::vector<std::string>(
          {invalid_delta_name, ToDeltaFileName(5).value()})))
      .WillRepeatedly(Return(std::vector<std::string>()));
#if defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      client_,
      ListBlobs(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::ListOptions::start_after, "")))
      .WillRepeatedly(Return(std::vector<std::string>()));
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      client_,
      ListBlobs(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::ListOptions::start_after,
                      ToDeltaFileName(1).value())))
      .WillOnce(Return(std::vector<std::string>({})))
      .WillOnce(Return(std::vector<std::string>(
          {ToDeltaFileName(3).value(), invalid_delta_name})));
  EXPECT_CALL(
      client_,
      ListBlobs(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::ListOptions::start_after,
                      ToDeltaFileName(3).value())))
      .WillOnce(Return(std::vector<std::string>({ToDeltaFileName(4).value()})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          AllOf(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::DataLocation::prefix, kBlobPrefix1)),
          Field(&BlobStorageClient::ListOptions::start_after, "")))
      .WillRepeatedly(Return(std::vector<std::string>()));
  EXPECT_CALL(
      client_,
      ListBlobs(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::ListOptions::start_after,
                      ToDeltaFileName(4).value())))
      .WillOnce(Return(std::vector<std::string>({ToDeltaFileName(5).value()})));

  absl::Notification finished;
  testing::MockFunction<void(const std::string& record)> callback;
  EXPECT_CALL(callback, Call)
      .Times(3)
      .WillOnce([&](const std::string& key) {
        EXPECT_EQ(key, ToDeltaFileName(3).value());
      })
      .WillOnce([&](const std::string& key) {
        EXPECT_EQ(key, ToDeltaFileName(4).value());
      })
      .WillOnce([&](const std::string& key) {
        EXPECT_EQ(key, ToDeltaFileName(5).value());
        finished.Notify();
      });

  absl::Status status = notifier_->Start(
      change_notifier_, {.bucket = "testbucket"},
      {std::make_pair("", initial_key_)}, callback.AsStdFunction());
  ASSERT_TRUE(status.ok());
  EXPECT_TRUE(notifier_->IsRunning());
  finished.WaitForNotification();
  status = notifier_->Stop();
  ASSERT_TRUE(status.ok());
  EXPECT_FALSE(notifier_->IsRunning());
}

TEST_F(DeltaFileNotifierTest, GetChangesFailure) {
  BlobStorageClient::DataLocation location = {.bucket = "testbucket"};
  EXPECT_CALL(change_notifier_, GetNotifications(_, _))
      .WillOnce(Return(absl::InvalidArgumentError("stuff")))
      .WillOnce(Return(absl::InvalidArgumentError("stuff")))
      .WillOnce(Return(std::vector<std::string>({ToDeltaFileName(1).value()})))
      .WillRepeatedly(Return(std::vector<std::string>()));
#if defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      client_,
      ListBlobs(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::ListOptions::start_after, "")))
      .WillRepeatedly(Return(std::vector<std::string>()));
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      client_,
      ListBlobs(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::ListOptions::start_after,
                      ToDeltaFileName(1).value())))
      .WillOnce(Return(std::vector<std::string>({})))
      .WillOnce(Return(std::vector<std::string>({ToDeltaFileName(1).value()})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          AllOf(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::DataLocation::prefix, kBlobPrefix1)),
          Field(&BlobStorageClient::ListOptions::start_after, "")))
      .WillRepeatedly(Return(std::vector<std::string>()));
  absl::Notification finished;
  testing::MockFunction<void(const std::string& record)> callback;
  EXPECT_CALL(callback, Call).Times(1).WillOnce([&](const std::string& key) {
    EXPECT_EQ(key, ToDeltaFileName(1).value());
    finished.Notify();
  });
  EXPECT_CALL(*sleep_for_, Duration(absl::Seconds(2)))
      .Times(1)
      .WillOnce(Return(true));
  EXPECT_CALL(*sleep_for_, Duration(absl::Seconds(4)))
      .Times(1)
      .WillOnce(Return(true));

  absl::Status status = notifier_->Start(
      change_notifier_, {.bucket = "testbucket"},
      {std::make_pair("", initial_key_)}, callback.AsStdFunction());
  ASSERT_TRUE(status.ok());
  EXPECT_TRUE(notifier_->IsRunning());
  finished.WaitForNotification();
  status = notifier_->Stop();
  ASSERT_TRUE(status.ok());
  EXPECT_FALSE(notifier_->IsRunning());
}

TEST_F(DeltaFileNotifierTest, BackupPoll) {
  EXPECT_CALL(change_notifier_, GetNotifications(_, _))
      .WillOnce(Return(absl::DeadlineExceededError("too long")))
      .WillRepeatedly(Return(std::vector<std::string>()));
#if defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      client_,
      ListBlobs(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::ListOptions::start_after, "")))
      .WillRepeatedly(Return(std::vector<std::string>()));
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      client_,
      ListBlobs(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::ListOptions::start_after,
                      ToDeltaFileName(1).value())))
      .WillOnce(Return(std::vector<std::string>({ToDeltaFileName(2).value()})));
  EXPECT_CALL(
      client_,
      ListBlobs(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::ListOptions::start_after,
                      ToDeltaFileName(2).value())))
      .WillOnce(Return(std::vector<std::string>({ToDeltaFileName(3).value()})));
  EXPECT_CALL(
      client_,
      ListBlobs(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::ListOptions::start_after,
                      ToDeltaFileName(3).value())))
      .WillOnce(Return(std::vector<std::string>({ToDeltaFileName(4).value()})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          AllOf(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::DataLocation::prefix, kBlobPrefix1)),
          Field(&BlobStorageClient::ListOptions::start_after, "")))
      .WillRepeatedly(Return(std::vector<std::string>()));
  absl::Notification finished;
  testing::MockFunction<void(const std::string& record)> callback;
  EXPECT_CALL(callback, Call)
      .Times(3)
      .WillOnce([&](const std::string& key) {
        // Initial poll
        EXPECT_EQ(key, ToDeltaFileName(2).value());
        sim_clock_.AdvanceTime(poll_frequency_ + absl::Seconds(1));
      })
      .WillOnce([&](const std::string& key) {
        // Backup poll due to expired flag
        EXPECT_EQ(key, ToDeltaFileName(3).value());
      })
      .WillOnce([&](const std::string& key) {
        // Backup poll due to WaitForNotification returning DeadlineExceeded
        EXPECT_EQ(key, ToDeltaFileName(4).value());
        finished.Notify();
      });

  absl::Status status = notifier_->Start(
      change_notifier_, {.bucket = "testbucket"},
      {std::make_pair("", initial_key_)}, callback.AsStdFunction());
  ASSERT_TRUE(status.ok());
  EXPECT_TRUE(notifier_->IsRunning());
  finished.WaitForNotification();
  status = notifier_->Stop();
  ASSERT_TRUE(status.ok());
  EXPECT_FALSE(notifier_->IsRunning());
}

TEST_F(DeltaFileNotifierTest, NotifiesWithNewPrefixedFiles) {
  BlobStorageClient::DataLocation location = {.bucket = "testbucket"};
  EXPECT_CALL(change_notifier_, GetNotifications(_, _))
      .WillOnce(Return(std::vector<std::string>({ToDeltaFileName(3).value()})))
      .WillOnce(Return(std::vector<std::string>({ToDeltaFileName(4).value()})))
      .WillRepeatedly(Return(std::vector<std::string>()));
#if defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      client_,
      ListBlobs(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::ListOptions::start_after, "")))
      .WillRepeatedly(Return(std::vector<std::string>()));
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)
  EXPECT_CALL(
      client_,
      ListBlobs(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::ListOptions::start_after,
                      ToDeltaFileName(1).value())))
      .WillOnce(Return(std::vector<std::string>({})))
      .WillOnce(Return(std::vector<std::string>({ToDeltaFileName(3).value()})));
  EXPECT_CALL(
      client_,
      ListBlobs(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::ListOptions::start_after,
                      ToDeltaFileName(3).value())))
      .WillOnce(Return(std::vector<std::string>({ToDeltaFileName(4).value()})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          AllOf(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::DataLocation::prefix, kBlobPrefix1)),
          Field(&BlobStorageClient::ListOptions::start_after,
                ToDeltaFileName(10).value())))
      .WillOnce(
          Return(std::vector<std::string>({ToDeltaFileName(11).value()})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          AllOf(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::DataLocation::prefix, kBlobPrefix1)),
          Field(&BlobStorageClient::ListOptions::start_after,
                ToDeltaFileName(11).value())))
      .WillRepeatedly(Return(std::vector<std::string>()));

  absl::Notification finished;
  testing::MockFunction<void(const std::string& record)> callback;
  EXPECT_CALL(callback, Call(ToDeltaFileName(3).value()));
  EXPECT_CALL(callback, Call(absl::StrCat(kBlobPrefix1, "/",
                                          ToDeltaFileName(11).value())));
  EXPECT_CALL(callback, Call(ToDeltaFileName(4).value())).WillOnce([&]() {
    finished.Notify();
  });

  absl::Status status =
      notifier_->Start(change_notifier_, {.bucket = "testbucket"},
                       {
                           std::make_pair("", initial_key_),
                           std::make_pair(std::string(kBlobPrefix1),
                                          ToDeltaFileName(10).value()),
                       },
                       callback.AsStdFunction());
  ASSERT_TRUE(status.ok());
  EXPECT_TRUE(notifier_->IsRunning());
  finished.WaitForNotification();
  status = notifier_->Stop();
  ASSERT_TRUE(status.ok());
  EXPECT_FALSE(notifier_->IsRunning());
}

#if defined(MICROSOFT_AD_SELECTION_BUILD)
TEST_F(DeltaFileNotifierTest, Microsoft_NotifiesWithNewAnnFiles) {
  std::string ann_snapshot1 = "ANNSNAPSHOT_0000000000000001";
  std::string ann_snapshot2 = "ANNSNAPSHOT_0000000000000002";
  std::string ann_snapshot4 = "ANNSNAPSHOT_0000000000000004";
  BlobStorageClient::DataLocation location = {.bucket = "testbucket"};
  EXPECT_CALL(change_notifier_, GetNotifications(_, _))
      .WillOnce(Return(std::vector<std::string>({ann_snapshot1})))
      .WillOnce(Return(std::vector<std::string>({ann_snapshot2})))
      .WillOnce(Return(std::vector<std::string>({ann_snapshot4})))
      .WillRepeatedly(Return(std::vector<std::string>()));
  EXPECT_CALL(
      client_,
      ListBlobs(
          Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
          Field(&BlobStorageClient::ListOptions::start_after, initial_key_)))
      .WillRepeatedly(Return(std::vector<std::string>()));
  EXPECT_CALL(
      client_,
      ListBlobs(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::ListOptions::start_after, "")))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>({ann_snapshot1})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
          Field(&BlobStorageClient::ListOptions::start_after, ann_snapshot1)))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>({ann_snapshot2})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
          Field(&BlobStorageClient::ListOptions::start_after, ann_snapshot2)))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>({ann_snapshot4})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
          Field(&BlobStorageClient::ListOptions::start_after, ann_snapshot4)))
      .WillRepeatedly(Return(std::vector<std::string>({})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          AllOf(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::DataLocation::prefix, kBlobPrefix1)),
          Field(&BlobStorageClient::ListOptions::start_after, "")))
      .WillRepeatedly(Return(std::vector<std::string>()));

  absl::Notification finished;
  testing::MockFunction<void(const std::string& record)> callback;
  EXPECT_CALL(callback, Call)
      .Times(3)
      .WillOnce([&](const std::string& key) { EXPECT_EQ(key, ann_snapshot1); })
      .WillOnce([&](const std::string& key) { EXPECT_EQ(key, ann_snapshot2); })
      .WillOnce([&](const std::string& key) {
        EXPECT_EQ(key, ann_snapshot4);
        finished.Notify();
      });

  absl::Status status = notifier_->Start(
      change_notifier_, {.bucket = "testbucket"},
      {std::make_pair("", initial_key_)}, callback.AsStdFunction());
  ASSERT_TRUE(status.ok());
  EXPECT_TRUE(notifier_->IsRunning());
  finished.WaitForNotification();
  status = notifier_->Stop();
  ASSERT_TRUE(status.ok());
  EXPECT_FALSE(notifier_->IsRunning());
}

TEST_F(DeltaFileNotifierTest, Microsoft_NotLoadOldOrSameOrInvalidAnnFiles) {
  std::string ann_snapshot2 = "ANNSNAPSHOT_0000000000000002";
  std::string ann_snapshot4 = "ANNSNAPSHOT_0000000000000004";
  std::string ann_snapshot_inv = "ANNSNAPSHOT_6";
  // we are loading 5th to be sure that we are not loading old one (2nd)
  std::string ann_snapshot5 = "ANNSNAPSHOT_0000000000000005";
  BlobStorageClient::DataLocation location = {.bucket = "testbucket"};
  EXPECT_CALL(change_notifier_, GetNotifications(_, _))
      .WillOnce(Return(std::vector<std::string>({ann_snapshot4})))
      .WillOnce(Return(std::vector<std::string>(
          {ann_snapshot2})))  // this one should be ignored
      .WillOnce(Return(std::vector<std::string>(
          {ann_snapshot4})))  // this one should be ignored
      .WillOnce(Return(std::vector<std::string>(
          {ann_snapshot_inv})))  // this one should be ignored
      .WillOnce(Return(std::vector<std::string>(
          {ann_snapshot5})))  // this one we need to notify test finish
      .WillRepeatedly(Return(std::vector<std::string>()));
  EXPECT_CALL(
      client_,
      ListBlobs(
          Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
          Field(&BlobStorageClient::ListOptions::start_after, initial_key_)))
      .WillRepeatedly(Return(std::vector<std::string>()));
  EXPECT_CALL(
      client_,
      ListBlobs(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::ListOptions::start_after, "")))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>({ann_snapshot4})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
          Field(&BlobStorageClient::ListOptions::start_after, ann_snapshot4)))
      .Times(1)
      .WillOnce(
          Return(std::vector<std::string>({ann_snapshot5, ann_snapshot_inv})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
          Field(&BlobStorageClient::ListOptions::start_after, ann_snapshot5)))
      .WillRepeatedly(Return(std::vector<std::string>({ann_snapshot_inv})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          AllOf(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::DataLocation::prefix, kBlobPrefix1)),
          Field(&BlobStorageClient::ListOptions::start_after, "")))
      .WillRepeatedly(Return(std::vector<std::string>()));

  absl::Notification finished;
  testing::MockFunction<void(const std::string& record)> callback;
  EXPECT_CALL(callback, Call)
      .Times(2)
      .WillOnce([&](const std::string& key) { EXPECT_EQ(key, ann_snapshot4); })
      .WillOnce([&](const std::string& key) {
        EXPECT_EQ(key, ann_snapshot5);
        finished.Notify();
      });

  absl::Status status = notifier_->Start(
      change_notifier_, {.bucket = "testbucket"},
      {std::make_pair("", initial_key_)}, callback.AsStdFunction());
  ASSERT_TRUE(status.ok());
  EXPECT_TRUE(notifier_->IsRunning());
  finished.WaitForNotification();
  status = notifier_->Stop();
  ASSERT_TRUE(status.ok());
  EXPECT_FALSE(notifier_->IsRunning());
}

TEST_F(DeltaFileNotifierTest, Microsoft_CombinedAnnKvTest) {
  std::string ann_snapshot1 = "ANNSNAPSHOT_0000000000000001";
  std::string ann_snapshot2 = "ANNSNAPSHOT_0000000000000002";
  std::string ann_snapshot3 = "ANNSNAPSHOT_0000000000000003";
  std::string ann_snapshot4 = "ANNSNAPSHOT_0000000000000004";
  std::string ann_snapshot5 = "ANNSNAPSHOT_0000000000000005";
  std::string delta_file2 = ToDeltaFileName(2).value();
  std::string delta_file4 = ToDeltaFileName(4).value();
  std::string delta_file5 = ToDeltaFileName(5).value();
  std::string delta_file6 = ToDeltaFileName(6).value();
  BlobStorageClient::DataLocation location = {.bucket = "testbucket"};
  EXPECT_CALL(change_notifier_, GetNotifications(_, _))
      .WillOnce(Return(std::vector<std::string>({ann_snapshot1})))
      .WillOnce(Return(std::vector<std::string>({ann_snapshot2})))
      .WillOnce(Return(std::vector<std::string>({delta_file2})))
      .WillOnce(Return(std::vector<std::string>({delta_file4})))
      .WillOnce(Return(std::vector<std::string>({ann_snapshot3})))
      .WillOnce(Return(std::vector<std::string>({ann_snapshot4})))
      .WillOnce(Return(std::vector<std::string>({ann_snapshot5})))
      .WillOnce(Return(std::vector<std::string>({delta_file5})))
      .WillOnce(Return(std::vector<std::string>({delta_file6})))
      .WillRepeatedly(Return(std::vector<std::string>()));
  EXPECT_CALL(
      client_,
      ListBlobs(
          Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
          Field(&BlobStorageClient::ListOptions::start_after, initial_key_)))
      .Times(3)
      .WillOnce(Return(std::vector<std::string>()))
      .WillOnce(Return(std::vector<std::string>()))
      .WillOnce(Return(std::vector<std::string>({delta_file2})));
  EXPECT_CALL(
      client_,
      ListBlobs(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::ListOptions::start_after, "")))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>({ann_snapshot1})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
          Field(&BlobStorageClient::ListOptions::start_after, ann_snapshot1)))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>({ann_snapshot2})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
          Field(&BlobStorageClient::ListOptions::start_after, ann_snapshot2)))
      .Times(3)
      .WillOnce(Return(std::vector<std::string>({delta_file2})))
      .WillOnce(Return(std::vector<std::string>({delta_file2, delta_file4})))
      .WillOnce(Return(
          std::vector<std::string>({ann_snapshot3, delta_file2, delta_file4})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
          Field(&BlobStorageClient::ListOptions::start_after, delta_file2)))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>({delta_file4})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
          Field(&BlobStorageClient::ListOptions::start_after, delta_file4)))
      .Times(4)
      .WillOnce(Return(std::vector<std::string>()))
      .WillOnce(Return(std::vector<std::string>()))
      .WillOnce(Return(std::vector<std::string>()))
      .WillOnce(Return(std::vector<std::string>({delta_file5})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
          Field(&BlobStorageClient::ListOptions::start_after, ann_snapshot3)))
      .Times(1)
      .WillOnce(Return(
          std::vector<std::string>({ann_snapshot4, delta_file2, delta_file4})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
          Field(&BlobStorageClient::ListOptions::start_after, ann_snapshot4)))
      .Times(1)
      .WillOnce(Return(
          std::vector<std::string>({ann_snapshot5, delta_file2, delta_file4})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
          Field(&BlobStorageClient::ListOptions::start_after, ann_snapshot5)))
      .WillOnce(Return(
          std::vector<std::string>({delta_file2, delta_file4, delta_file5})))
      .WillRepeatedly(Return(std::vector<std::string>(
          {delta_file2, delta_file4, delta_file5, delta_file6})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
          Field(&BlobStorageClient::ListOptions::start_after, delta_file5)))
      .Times(1)
      .WillOnce(Return(std::vector<std::string>({delta_file6})));
  EXPECT_CALL(
      client_,
      ListBlobs(
          Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
          Field(&BlobStorageClient::ListOptions::start_after, delta_file6)))
      .WillRepeatedly(Return(std::vector<std::string>()));
  EXPECT_CALL(
      client_,
      ListBlobs(
          AllOf(Field(&BlobStorageClient::DataLocation::bucket, "testbucket"),
                Field(&BlobStorageClient::DataLocation::prefix, kBlobPrefix1)),
          Field(&BlobStorageClient::ListOptions::start_after, "")))
      .WillRepeatedly(Return(std::vector<std::string>()));

  absl::Notification finished;
  testing::MockFunction<void(const std::string& record)> callback;
  EXPECT_CALL(callback, Call)
      .Times(9)
      .WillOnce([&](const std::string& key) { EXPECT_EQ(key, ann_snapshot1); })
      .WillOnce([&](const std::string& key) { EXPECT_EQ(key, ann_snapshot2); })
      .WillOnce([&](const std::string& key) { EXPECT_EQ(key, delta_file2); })
      .WillOnce([&](const std::string& key) { EXPECT_EQ(key, delta_file4); })
      .WillOnce([&](const std::string& key) { EXPECT_EQ(key, ann_snapshot3); })
      .WillOnce([&](const std::string& key) { EXPECT_EQ(key, ann_snapshot4); })
      .WillOnce([&](const std::string& key) { EXPECT_EQ(key, ann_snapshot5); })
      .WillOnce([&](const std::string& key) { EXPECT_EQ(key, delta_file5); })
      .WillOnce([&](const std::string& key) {
        EXPECT_EQ(key, delta_file6);
        finished.Notify();
      });

  absl::Status status = notifier_->Start(
      change_notifier_, {.bucket = "testbucket"},
      {std::make_pair("", initial_key_)}, callback.AsStdFunction());
  ASSERT_TRUE(status.ok());
  EXPECT_TRUE(notifier_->IsRunning());
  finished.WaitForNotification();
  status = notifier_->Stop();
  ASSERT_TRUE(status.ok());
  EXPECT_FALSE(notifier_->IsRunning());
}
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)

}  // namespace
}  // namespace kv_server
