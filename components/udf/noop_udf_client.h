/*
 * Copyright 2023 Google LLC
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

#ifndef COMPONENTS_UDF_NOOP_UDF_CLIENT_H_
#define COMPONENTS_UDF_NOOP_UDF_CLIENT_H_

#include <memory>

#include "components/udf/udf_client.h"

namespace kv_server {

// Create a no-op UDF client that doesn't do anything. Useful for certain
// tests/tools that need a udf client, but don't actually want to use it.
std::unique_ptr<UdfClient> NewNoopUdfClient();

}  // namespace kv_server

#endif  // COMPONENTS_UDF_NOOP_UDF_CLIENT_H_
