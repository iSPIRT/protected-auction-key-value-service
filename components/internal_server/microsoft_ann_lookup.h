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

#ifndef COMPONENTS_INTERNAL_SERVER_MICROSOFT_ANN_LOOKUP_H_
#define COMPONENTS_INTERNAL_SERVER_MICROSOFT_ANN_LOOKUP_H_

#include <memory>
#include <string>
#include <utility>

#include "components/data_server/microsoft_ann_index/index.h"
#include "components/internal_server/lookup.h"

namespace kv_server {
namespace microsoft {

class AnnLookup : public Lookup {
 public:
  // this method is creating instance of this class.
  // This class is a special abstraction of Lookup class - is is needed to link
  // the ANNIndex and UDF Hook classes - all hooks are initialized with Lookup
  // class and they are using lookup class to communicate with another parts of
  // the system.
  // Other Lookup classes using the the same pattern of creation
  static std::unique_ptr<Lookup> CreateAnnLookup(const ANNIndex& annIndex);

  absl::StatusOr<InternalLookupResponse> GetKeyValueSet(
      const RequestContext& request_context,
      const absl::flat_hash_set<std::string_view>& key_set) const override;

  // all other Get* methods below (except GetKeyValueSet) are fake methods, will
  // never be used. This is done intentionally to have the same interface as
  // other lookups.
  // Other lookups are not using ANNIndex as a data source, but some KV storage.
  // This storage (that is hidden after other Lookup classes) allows to have
  // different types of data and different types of queries.
  // ANNIndex is a special case of data storage, where we have only one type of
  // requests: GetKeyValueSet, where for one key we can have multiple values.
  // This is why we have only one method GetKeyValueSet in this class.
  // This is supported in ann hook class and this is also checked in tests.
  absl::StatusOr<InternalLookupResponse> GetKeyValues(
      const RequestContext& request_context,
      const absl::flat_hash_set<std::string_view>& keys) const override;

  absl::StatusOr<InternalLookupResponse> GetUInt32ValueSet(
      const RequestContext& request_context,
      const absl::flat_hash_set<std::string_view>& key_set) const override;

  absl::StatusOr<InternalLookupResponse> GetUInt64ValueSet(
      const RequestContext& request_context,
      const absl::flat_hash_set<std::string_view>& key_set) const override;

  absl::StatusOr<InternalRunQueryResponse> RunQuery(
      const RequestContext& request_context, std::string query) const override;

  absl::StatusOr<InternalRunSetQueryUInt32Response> RunSetQueryUInt32(
      const RequestContext& request_context, std::string query) const override;

  absl::StatusOr<InternalRunSetQueryUInt64Response> RunSetQueryUInt64(
      const RequestContext& request_context, std::string query) const override;

 private:
  friend std::unique_ptr<AnnLookup> std::make_unique<AnnLookup>(
      const ANNIndex& index);

  explicit AnnLookup(const ANNIndex& index);

  const ANNIndex& index_;
};

}  // namespace microsoft
}  // namespace kv_server

#endif  // COMPONENTS_INTERNAL_SERVER_MICROSOFT_ANN_LOOKUP_H_
