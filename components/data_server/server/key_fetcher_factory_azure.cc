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

#include "components/data_server/server/key_fetcher_factory.h"

namespace kv_server {
namespace {
using ::google::scp::cpio::PrivateKeyVendingEndpoint;
using ::privacy_sandbox::server_common::CloudPlatform;

class KeyFetcherFactoryAzure : public CloudKeyFetcherFactory {
 public:
  explicit KeyFetcherFactoryAzure(
      privacy_sandbox::server_common::log::PSLogContext& log_context)
      : CloudKeyFetcherFactory(log_context) {}

 protected:
  PrivateKeyVendingEndpoint GetPrimaryKeyFetchingEndpoint(
      const ParameterFetcher& parameter_fetcher) const override {
    PrivateKeyVendingEndpoint endpoint;
    endpoint.account_identity = "accountIdentity";

    const char* value = std::getenv("PRIMARY_COORDINATOR_PRIVATE_KEY_ENDPOINT");
    if (value) {
      endpoint.private_key_vending_service_endpoint = value;
      PS_LOG(INFO, log_context_)
          << "Retrieved PRIMARY_COORDINATOR_PRIVATE_KEY_ENDPOINT environment "
             "variable: "
          << endpoint.private_key_vending_service_endpoint;
    } else {
      PS_LOG(ERROR, log_context_)
          << "Environment variable PRIMARY_COORDINATOR_PRIVATE_KEY_ENDPOINT "
             "not set";
    }

    return endpoint;
  }

  PrivateKeyVendingEndpoint GetSecondaryKeyFetchingEndpoint(
      const ParameterFetcher& parameter_fetcher) const override {
    PrivateKeyVendingEndpoint endpoint;

    return endpoint;
  }

  CloudPlatform GetCloudPlatform() const override {
    return CloudPlatform::kAzure;
  }
};
}  // namespace

std::unique_ptr<KeyFetcherFactory> KeyFetcherFactory::Create(
    privacy_sandbox::server_common::log::PSLogContext& log_context) {
  return std::make_unique<KeyFetcherFactoryAzure>(log_context);
}

}  // namespace kv_server
