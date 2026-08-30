//===----------------------------------------------------------------------===//
//                         BusTub
//
// binder/index_statement.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "binder/bound_statement.h"
#include "binder/expressions/bound_column_ref.h"
#include "binder/table_ref/bound_base_table_ref.h"
#include "catalog/column.h"

namespace bustub {

using IndexOptionValue = std::variant<int64_t, std::string>;
using IndexOption = std::pair<std::string, IndexOptionValue>;

class IndexStatement : public BoundStatement {
 public:
  explicit IndexStatement(std::string index_name, std::unique_ptr<BoundBaseTableRef> table,
                          std::vector<std::unique_ptr<BoundColumnRef>> cols, std::string index_type,
                          std::vector<std::string> col_options, std::vector<IndexOption> options, bool is_unique);

  /** Name of the index */
  std::string index_name_;

  /** Create on which table */
  std::unique_ptr<BoundBaseTableRef> table_;

  /** Name of the columns */
  std::vector<std::unique_ptr<BoundColumnRef>> cols_;

  /** Using */
  std::string index_type_;

  std::vector<std::string> col_options_;
  std::vector<IndexOption> options_;

  /** True for CREATE UNIQUE INDEX; distributed V1 rejects it before proposal. */
  bool is_unique_{false};

  auto ToString() const -> std::string override;
};

}  // namespace bustub
