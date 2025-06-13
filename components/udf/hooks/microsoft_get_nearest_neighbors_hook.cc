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

#include "components/udf/hooks/microsoft_get_nearest_neighbors_hook.h"

#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/functional/any_invocable.h"
#include "absl/log/log.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_join.h"
#include "components/internal_server/lookup.pb.h"
#include "components/telemetry/server_definition.h"
#include "google/protobuf/util/json_util.h"
#include "nlohmann/json.hpp"

namespace kv_server {
namespace {

using google::protobuf::json::MessageToJsonString;
using google::scp::roma::FunctionBindingPayload;
using google::scp::roma::proto::FunctionBindingIoProto;

constexpr char kOkStatusMessage[] = "ok";

class GetNearestNeighborsHookImpl : public GetNearestNeighborsHook {
 public:
  GetNearestNeighborsHookImpl() {}

  void FinishInit(std::unique_ptr<Lookup> lookup) {
    if (lookup_) {
      PS_VLOG(1)
          << "Attempt to reinitialize lookup_ which is already initialized.";
      lookup_.reset();  // Reset existing lookup before reassigning
    }
    lookup_ = std::move(lookup);
  }

  void operator()(
      FunctionBindingPayload<std::weak_ptr<RequestContext>>& payload) {
    std::shared_ptr<RequestContext> request_context = payload.metadata.lock();
    if (!request_context) {
      PS_VLOG(1) << "Request context is not available, the request might "
                    "have been marked as complete";
      return;
    }
    PS_VLOG(9, request_context->GetPSLogContext())
        << "Called getNearestNeighbors hook";
    if (!lookup_) {
      SetStatus(absl::StatusCode::kInternal,
                "getNearestNeighbors has not been initialized yet",
                payload.io_proto);
      PS_LOG(ERROR, request_context->GetPSLogContext())
          << "getNearestNeighbors hook is not initialized properly: lookup is "
             "nullptr";
      return;
    }

    PS_VLOG(9, request_context->GetPSLogContext())
        << "getNearestNeighbors request: " << payload.io_proto.DebugString();
    if (!payload.io_proto.has_input_list_of_string()) {
      SetStatus(absl::StatusCode::kInvalidArgument,
                "getNearestNeighbors input must be list of strings",
                payload.io_proto);
      PS_VLOG(1, request_context->GetPSLogContext())
          << "getNearestNeighbors result: " << payload.io_proto.DebugString();
      return;
    }
    if (payload.io_proto.input_list_of_string().data().empty()) {
      SetStatus(absl::StatusCode::kInvalidArgument,
                "getNearestNeighbors input must have keys", payload.io_proto);
      PS_VLOG(1, request_context->GetPSLogContext())
          << "getNearestNeighbors result: " << payload.io_proto.DebugString();
      return;
    }
    // TODO: support passing different arguments: task.ms/54427185
    // TODO: multi-index call support: task.ms/54427159

    absl::flat_hash_set<std::string_view> keys;
    for (const auto& key : payload.io_proto.input_list_of_string().data()) {
      keys.insert(key);
    }

    PS_VLOG(9, request_context->GetPSLogContext())
        << "Calling internal Aproximate Nearest Neighbour lookup client";
    absl::StatusOr<InternalLookupResponse> response_or_status =
        lookup_->GetKeyValueSet(*request_context, keys);
    if (!response_or_status.ok()) {
      SetStatus(response_or_status.status().code(),
                response_or_status.status().message(), payload.io_proto);
      PS_VLOG(1, request_context->GetPSLogContext())
          << "getNearestNeighbors result: " << payload.io_proto.DebugString();
      return;
    }

    SetOutput(response_or_status.value(), payload.io_proto, *request_context);
    PS_VLOG(9, request_context->GetPSLogContext())
        << "getNearestNeighbors result: " << payload.io_proto.DebugString();
  }

 private:
  void SetStatus(absl::StatusCode code, std::string_view message,
                 FunctionBindingIoProto& io) {
    nlohmann::json status;
    status["code"] = code;
    status["message"] = std::string(message);
    io.set_output_string(status.dump());
  }

  void SetOutput(const InternalLookupResponse& response,
                 FunctionBindingIoProto& io,
                 const RequestContext& request_context) {
    PS_VLOG(9, request_context.GetPSLogContext())
        << "Processing internal Aproximate Nearest Neighbour lookup response";
    std::string kv_pairs_json;
    const auto json_status = MessageToJsonString(response, &kv_pairs_json);
    if (!json_status.ok()) {
      SetStatus(json_status.code(), json_status.message(), io);
      PS_LOG(ERROR, request_context.GetPSLogContext())
          << "MessageToJsonString failed with " << json_status;
      PS_VLOG(1, request_context.GetPSLogContext())
          << "getNearestNeighbors result: " << io.DebugString();
      return;
    }

    auto kv_pairs_json_object =
        nlohmann::json::parse(kv_pairs_json, nullptr,
                              /*allow_exceptions=*/false,
                              /*ignore_comments=*/true);
    if (kv_pairs_json_object.is_discarded()) {
      SetStatus(absl::StatusCode::kInvalidArgument,
                "Error while parsing JSON string.", io);
      PS_LOG(ERROR, request_context.GetPSLogContext())
          << "json parse failed for " << kv_pairs_json;

      return;
    }
    kv_pairs_json_object["status"]["code"] = 0;
    kv_pairs_json_object["status"]["message"] = kOkStatusMessage;
    io.set_output_string(kv_pairs_json_object.dump());
  }

  // `lookup_` is initialized separately, since its dependencies create threads.
  // Lazy load is used to ensure that it only happens after Roma forks.
  std::unique_ptr<Lookup> lookup_;
};
}  // namespace

std::unique_ptr<GetNearestNeighborsHook> GetNearestNeighborsHook::Create() {
  return std::make_unique<GetNearestNeighborsHookImpl>();
}

}  // namespace kv_server
