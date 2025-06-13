//  Copyright (C) Microsoft Corporation. All rights reserved.
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

#include <string>
#include <thread>

#include "absl/status/statusor.h"
#include "components/cloud_config/parameter_client.h"
#include "gtest/gtest.h"

namespace kv_server {
namespace {

TEST(ParameterClientAzure, ExpectedFlagDefaultsArePresent) {
  std::unique_ptr<ParameterClient> client = ParameterClient::Create();
  ASSERT_TRUE(client != nullptr);

  {
    const auto statusor = client->GetParameter("kv-server-azure-directory");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ("", *statusor);
  }
  {
    const auto statusor =
        client->GetParameter("kv-server-azure-data-bucket-id");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ("", *statusor);
  }
  {
    const auto statusor =
        client->GetParameter("kv-server-azure-data-loading-file-format");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ("riegeli", *statusor);
  }
  {
    const auto statusor = client->GetParameter(
        "kv-server-azure-data-loading-blob-prefix-allowlist");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ("", *statusor);
  }
  {
    const auto statusor =
        client->GetParameter("kv-server-azure-consented-debug-token");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ("", *statusor);
  }
  {
    const auto statusor =
        client->GetParameter("kv-server-azure-realtime-directory");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ("", *statusor);
  }
  {
    const auto statusor = client->GetParameter("kv-server-azure-launch-hook");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ("", *statusor);
  }
  {
    const auto statusor = client->GetInt32Parameter(
        "kv-server-azure-metrics-export-interval-millis");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ(30000, *statusor);
  }
  {
    const auto statusor = client->GetInt32Parameter(
        "kv-server-azure-metrics-export-timeout-millis");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ(5000, *statusor);
  }
  {
    const auto statusor =
        client->GetInt32Parameter("kv-server-azure-backup-poll-frequency-secs");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ(5, *statusor);
  }
  {
    const auto statusor = client->GetInt32Parameter(
        "kv-server-azure-realtime-updater-num-threads");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ(1, *statusor);
  }
  {
    const auto statusor =
        client->GetInt32Parameter("kv-server-azure-data-loading-num-threads");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ(std::min(std::thread::hardware_concurrency(), 8u), *statusor);
  }
  {
    const auto statusor =
        client->GetInt32Parameter("kv-server-azure-num-shards");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ(1, *statusor);
  }
  {
    const auto statusor =
        client->GetInt32Parameter("kv-server-azure-udf-num-workers");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ(2, *statusor);
  }
  {
    const auto statusor =
        client->GetInt32Parameter("kv-server-azure-logging-verbosity-level");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ(0, *statusor);
  }
  {
    const auto statusor = client->GetInt32Parameter(
        "kv-server-azure-logging-verbosity-backup-poll-frequency-secs");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ(300, *statusor);
  }
  {
    const auto statusor =
        client->GetInt32Parameter("kv-server-azure-udf-timeout-millis");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ(5000, *statusor);
  }
  {
    const auto statusor =
        client->GetInt32Parameter("kv-server-azure-udf-update-timeout-millis");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ(30000, *statusor);
  }
  {
    const auto statusor =
        client->GetInt32Parameter("kv-server-azure-udf-min-log-level");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ(0, *statusor);
  }
  {
    const auto statusor =
        client->GetBoolParameter("kv-server-azure-route-v1-to-v2");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ(true, *statusor);
  }
  {
    const auto statusor =
        client->GetBoolParameter("kv-server-azure-add-missing-keys-v1");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ(false, *statusor);
  }
  {
    const auto statusor =
        client->GetBoolParameter("kv-server-azure-use-real-coordinators");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ(true, *statusor);
  }
  {
    const auto statusor = client->GetBoolParameter(
        "kv-server-azure-use-external-metrics-collector-endpoint");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ(false, *statusor);
  }
  {
    const auto statusor =
        client->GetBoolParameter("kv-server-azure-enable-otel-logger");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ(false, *statusor);
  }
  {
    const auto statusor =
        client->GetBoolParameter("kv-server-azure-use-sharding-key-regex");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ(false, *statusor);
  }
  {
    const auto statusor =
        client->GetBoolParameter("kv-server-azure-enable-consented-log");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ(false, *statusor);
  }
  {
    const auto statusor =
        client->GetParameter("kv-server-azure-telemetry-config");
    ASSERT_TRUE(statusor.ok());
    EXPECT_EQ("mode: EXPERIMENT", *statusor);
  }
}

}  // namespace
}  // namespace kv_server
