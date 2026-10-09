//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sql_command_preparer.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <string>

#include "catalog/catalog.h"
#include "distributed/command.h"
#include "execution/executor_context.h"

namespace bustub {

/** Parse and expand one autocommit SQL write into a private deterministic CommandBatch without changing Catalog/pages.
 */
class SqlCommandPreparer {
 public:
  explicit SqlCommandPreparer(ExecutorContext *context) : catalog_(context->GetCatalog()), context_(context) {}

  auto Prepare(const std::string &sql, uint64_t client_id, uint64_t request_id,
               const RequestFingerprintV1 &request_fingerprint) const -> TransactionCommandBatch;

 private:
  Catalog *catalog_;
  ExecutorContext *context_;
};

}  // namespace bustub
