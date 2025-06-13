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

#include <string>
#include <utility>

#include "components/telemetry/server_definition.h"

namespace kv_server {
namespace microsoft {

std::unique_ptr<Lookup> AnnLookup::CreateAnnLookup(const ANNIndex& index) {
  return std::make_unique<AnnLookup>(index);
}

AnnLookup::AnnLookup(const ANNIndex& index) : index_(index) {}

absl::StatusOr<InternalLookupResponse> AnnLookup::GetKeyValueSet(
    const RequestContext& request_context,
    const absl::flat_hash_set<std::string_view>& key_set) const {
  ScopeLatencyMetricsRecorder<InternalLookupMetricsContext,
                              kMicrosoftAnnGetKeyValueSetLatencyInMicros>
      latency_recorder(request_context.GetInternalLookupMetricsContext());
  LogIfError(request_context.GetInternalLookupMetricsContext()
                 .AccumulateMetric<kMicrosoftAnnHookTotalKeysCallCount>(
                     static_cast<int>(key_set.size())));
  InternalLookupResponse response;
  if (key_set.empty()) {
    return response;
  }
  auto index_retval = index_.GetKeyValueSet(key_set);
  if (!index_retval || index_retval->empty()) {
    LogIfError(request_context.GetInternalLookupMetricsContext()
                   .AccumulateMetric<kMicrosoftAnnHookErrorsCallCount>(
                       static_cast<int>(key_set.size())));
    return absl::InternalError("Do not have initialized snapshots");
  }
  int keys_without_results = 0;
  for (const auto& key : key_set) {
    SingleLookupResult result;
    const auto value_set = (*index_retval)[std::string(key)];
    if (!value_set.empty()) {
      auto* keyset_values = result.mutable_keyset_values();
      keyset_values->mutable_values()->Reserve(value_set.size());
      keyset_values->mutable_values()->Add(value_set.begin(), value_set.end());
    } else {
      auto status = result.mutable_status();
      ++keys_without_results;
      status->set_code(static_cast<int>(absl::StatusCode::kNotFound));
      status->set_message(
          absl::StrCat("No result, most likely incorrect key: ", key));
    }
    (*response.mutable_kv_pairs())[key] = std::move(result);
  }
  LogIfError(request_context.GetInternalLookupMetricsContext()
                 .AccumulateMetric<kMicrosoftAnnHookErrorsCallCount>(
                     keys_without_results));
  return response;
}

absl::StatusOr<InternalLookupResponse> AnnLookup::GetKeyValues(
    const RequestContext& request_context,
    const absl::flat_hash_set<std::string_view>& keys)
    const {  // fake method, will never be used
  return absl::InternalError("Not implemented");
}

absl::StatusOr<InternalLookupResponse> AnnLookup::GetUInt32ValueSet(
    const RequestContext& request_context,
    const absl::flat_hash_set<std::string_view>& key_set)
    const {  // fake method, will never be used
  return absl::InternalError("Not implemented");
}

absl::StatusOr<InternalLookupResponse> AnnLookup::GetUInt64ValueSet(
    const RequestContext& request_context,
    const absl::flat_hash_set<std::string_view>& key_set)
    const {  // fake method, will never be used
  return absl::InternalError("Not implemented");
}

absl::StatusOr<InternalRunQueryResponse> AnnLookup::RunQuery(
    const RequestContext& request_context,
    std::string query) const {  // fake method, will never be used
  return absl::InternalError("Not implemented");
}

absl::StatusOr<InternalRunSetQueryUInt32Response> AnnLookup::RunSetQueryUInt32(
    const RequestContext& request_context,
    std::string query) const {  // fake method, will never be used
  return absl::InternalError("Not implemented");
}

absl::StatusOr<InternalRunSetQueryUInt64Response> AnnLookup::RunSetQueryUInt64(
    const RequestContext& request_context,
    std::string query) const {  // fake method, will never be used
  return absl::InternalError("Not implemented");
}

}  // namespace microsoft
}  // namespace kv_server
