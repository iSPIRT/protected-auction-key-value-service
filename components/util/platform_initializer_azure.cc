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

#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "components/util/platform_initializer.h"
#include "src/public/core/interface/execution_result.h"
#include "src/public/cpio/interface/cpio.h"

namespace kv_server {
using google::scp::core::errors::GetErrorMessage;
using google::scp::cpio::Cpio;
using google::scp::cpio::CpioOptions;

using google::scp::cpio::LogOption;

namespace {
google::scp::cpio::CpioOptions cpio_options_;

}  // namespace

PlatformInitializer::PlatformInitializer() {
  cpio_options_.log_option = LogOption::kConsoleLog;
  if (auto error = Cpio::InitCpio(cpio_options_); !error.Successful()) {
    LOG(ERROR) << "Failed to initialize CPIO: "
               << GetErrorMessage(error.status_code) << std::endl;
  } else {
    LOG(INFO) << "CPIO initialized successfully." << std::endl;
  }
}

PlatformInitializer::~PlatformInitializer() {
  if (auto error = Cpio::ShutdownCpio(cpio_options_); !error.Successful()) {
    LOG(ERROR) << "Failed to shutdown CPIO: "
               << GetErrorMessage(error.status_code) << std::endl;
  }
}

}  // namespace kv_server
