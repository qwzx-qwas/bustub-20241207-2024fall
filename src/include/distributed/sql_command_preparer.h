//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sql_command_preparer.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include "execution/plans/abstract_plan.h"

#include "catalog/catalog.h"
#include "distributed/command.h"
#include "execution/executor_context.h"

namespace bustub {

// Dependency identity is logical (table/key), never a tuple version or physical RID.
struct SqlWritePlan {
  enum class Scope { KEYS, TABLE, CATALOG };
  Scope scope_{Scope::CATALOG};
  table_oid_t table_{0};
  std::vector<std::string> keys_;
  uint64_t schema_epoch_{0};
  AbstractPlanNodeRef plan_;
  std::vector<Tuple> insert_values_;
  std::optional<TransactionCommandBatch> ddl_;
  std::weak_ptr<const void> workspace_;
};

/** Parse and expand one autocommit SQL write into a private deterministic CommandBatch without changing Catalog/pages.
 */
class SqlCommandPreparer {
 public:
  explicit SqlCommandPreparer(ExecutorContext *context,
                              size_t command_bytes = CommandBatchCodec::MAX_ENCODED_BATCH_BYTES)
      : command_bytes_(command_bytes), catalog_(context->GetCatalog()), context_(context) {}

  auto Analyze(const std::string &sql, uint64_t client_id, uint64_t request_id,
               const RequestFingerprintV1 &request_fingerprint) const -> SqlWritePlan;
  auto Prepare(const SqlWritePlan &plan, uint64_t client_id, uint64_t request_id,
               const RequestFingerprintV1 &request_fingerprint) const -> TransactionCommandBatch;
  auto Prepare(const std::string &sql, uint64_t client_id, uint64_t request_id,
               const RequestFingerprintV1 &request_fingerprint) const -> TransactionCommandBatch;

 private:
  const size_t command_bytes_;
  Catalog *catalog_;
  ExecutorContext *context_;
};

}  // namespace bustub
