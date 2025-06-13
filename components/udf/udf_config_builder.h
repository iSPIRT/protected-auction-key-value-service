/*
 * Copyright 2023 Google LLC
 * Copyright (C) Microsoft Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <memory>

#include "components/udf/hooks/get_values_hook.h"
#include "components/udf/hooks/run_query_hook.h"
#include "src/roma/config/config.h"
#if defined(MICROSOFT_AD_SELECTION_BUILD)
#include "components/udf/hooks/microsoft_get_nearest_neighbors_hook.h"
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)

namespace kv_server {

class UdfConfigBuilder {
 public:
#if defined(MICROSOFT_AD_SELECTION_BUILD)
  UdfConfigBuilder& MicrosoftRegisterGetNearestNeighborsHook(
      GetNearestNeighborsHook& get_nearest_neighbors_hook);
#endif  // defined(MICROSOFT_AD_SELECTION_BUILD)

  UdfConfigBuilder& RegisterStringGetValuesHook(GetValuesHook& get_values_hook);

  UdfConfigBuilder& RegisterBinaryGetValuesHook(GetValuesHook& get_values_hook);

  UdfConfigBuilder& RegisterRunSetQueryStringHook(
      RunSetQueryStringHook& run_query_hook);

  UdfConfigBuilder& RegisterRunSetQueryUInt32Hook(
      RunSetQueryUInt32Hook& run_set_query_uint32_hook);

  UdfConfigBuilder& RegisterRunSetQueryUInt64Hook(
      RunSetQueryUInt64Hook& run_set_query_uint64_hook);

  // Registers V8 logMessage function
  UdfConfigBuilder& RegisterLogMessageHook();

  // Registers V8 console logging function
  UdfConfigBuilder& RegisterConsoleLogHook();

  UdfConfigBuilder& RegisterCustomMetricHook();

  UdfConfigBuilder& SetNumberOfWorkers(int number_of_workers);

  UdfConfigBuilder& DisableUdfStackTraces(bool disable_stacktrace);

  google::scp::roma::Config<std::weak_ptr<RequestContext>>& Config();

 private:
  google::scp::roma::Config<std::weak_ptr<RequestContext>> config_;
};
}  // namespace kv_server
