// Copyright 2022 Google LLC
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

#include <cstdint>
#include <string>
#include <utility>

#include "absl/status/statusor.h"
#include "components/data_server/server/mocks.h"
#include "components/data_server/server/parameter_fetcher.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace kv_server {

TEST(ParameterFetcherTest, CreateChangeNotifierSmokeTest) {
  MockParameterClient client;
  EXPECT_CALL(client, GetParameter("kv-server-local-directory",
                                   testing::Eq(std::nullopt)))
      .Times(1)
      .WillOnce(::testing::Return(::testing::TempDir()));
  ParameterFetcher fetcher(
      /*environment=*/"local", client);

  const auto metadata = fetcher.GetBlobStorageNotifierMetadata();
  auto local_notifier_metadata = std::get<LocalNotifierMetadata>(metadata);

  EXPECT_EQ(::testing::TempDir(), local_notifier_metadata.local_directory);
}

TEST(ParameterFetcherTest, CreateDeltaFileRecordChangeNotifierSmokeTest) {
  MockParameterClient client;
  EXPECT_CALL(client, GetParameter("kv-server-local-realtime-directory",
                                   testing::Eq(std::nullopt)))
      .Times(1)
      .WillOnce(::testing::Return(::testing::TempDir()));

  ParameterFetcher fetcher(
      /*environment=*/"local", client);

  const int32_t num_shards = 1;
  const int32_t shard_num = 0;
  const auto notifier_metadata =
      fetcher.GetRealtimeNotifierMetadata(num_shards, shard_num);
  auto local_notifier_metadata =
      std::get<LocalNotifierMetadata>(notifier_metadata);

  EXPECT_EQ(::testing::TempDir(), local_notifier_metadata.local_directory);
}

}  // namespace kv_server
