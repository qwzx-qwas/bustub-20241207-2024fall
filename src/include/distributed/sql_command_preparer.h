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

namespace bustub {

/** Parse and expand one autocommit SQL write into a private deterministic CommandBatch without changing Catalog/pages.
 */
class SqlCommandPreparer {
 public:
  explicit SqlCommandPreparer(Catalog *catalog) : catalog_(catalog) {}

  auto Prepare(const std::string &sql, uint64_t client_id, uint64_t request_id) const -> TransactionCommandBatch;

 private:
  Catalog *catalog_;
};

}  // namespace bustub
