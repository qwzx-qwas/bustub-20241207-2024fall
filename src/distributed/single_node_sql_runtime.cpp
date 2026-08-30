//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// single_node_sql_runtime.cpp
//
//===----------------------------------------------------------------------===//

#include "recovery/single_node_runtime.h"

#include <stdexcept>

#include "distributed/request_fingerprint.h"
#include "distributed/sql_command_preparer.h"

namespace bustub {

auto SingleNodeCommandRuntime::CommitSql(const std::string &sql, uint64_t client_id, uint64_t request_id)
    -> std::vector<std::byte> {
  const auto request_fingerprint = ComputeWriteIntentFingerprintV1(sql);
  std::lock_guard write_lock(write_mutex_);
  {
    auto visible = visibility_.LockShared();
    const auto disposition = recovered_->sessions_->Classify(client_id, request_id, request_fingerprint);
    if (disposition == RequestDisposition::RETRY_LAST) {
      const auto response = recovered_->sessions_->GetLastResponse(client_id);
      if (!response.has_value()) {
        throw std::runtime_error("retry session has no committed response");
      }
      return *response;
    }
    if (disposition == RequestDisposition::PAYLOAD_MISMATCH) {
      throw std::runtime_error("request payload does not match request identity");
    }
    if (disposition != RequestDisposition::NEW_REQUEST) {
      throw std::runtime_error("SQL request id is old or contains a session sequence gap");
    }
  }
  const auto batch =
      SqlCommandPreparer(recovered_->catalog_.get()).Prepare(sql, client_id, request_id, request_fingerprint);
  return CommitLocked(batch);
}

}  // namespace bustub
