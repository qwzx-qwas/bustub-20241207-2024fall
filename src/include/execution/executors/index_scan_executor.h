//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_set>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  /** Populate the generic point-lookup RID bucket for the current predicate key. */
  void LoadPointLookupRids();

  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;

  IndexInfo *index_info_;

  BPlusTreeIndexForTwoIntegerColumn *tree_;
  std::vector<std::pair<IntegerKeyType_BTree, RID>> full_scan_entries_;
  size_t full_scan_entry_idx_{0};

  /** Point lookups use Index::ScanKey so ordinary non-unique indexes return every matching RID. */
  std::vector<RID> point_lookup_rids_;
  size_t point_lookup_rid_idx_{0};
  std::unordered_set<RID> emitted_rids_;

  // For handling multiple point lookups (OR clause)
  size_t current_key_idx_{0};
};
}  // namespace bustub
