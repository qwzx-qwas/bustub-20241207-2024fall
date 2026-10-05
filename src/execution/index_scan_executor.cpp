//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

#include <algorithm>
#include <cstring>
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto *catalog = exec_ctx_->GetCatalog();
  index_info_ = catalog->GetIndex(plan_->GetIndexOid()).get();
  current_key_idx_ = 0;
  point_lookup_rid_idx_ = 0;
  full_scan_entry_idx_ = 0;
  emitted_rids_.clear();
  prefetched_end_ = 0;

  // 查看上面优化器传来的pred_keys_（之前收集的常量值），决定是点查找模式还是全表扫描模式
  if (!plan_->pred_keys_.empty()) {
    // Point lookup must use the generic Index interface: ordinary secondary indexes may map one key to many RIDs.
    LoadPointLookupRids();
  } else {
    // 全表扫描模式
    tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
    if (tree_ == nullptr) {
      throw Exception("full IndexScan requires the supported B+Tree key specialization");
    }
    full_scan_entries_ = tree_->GetAllEntriesSnapshot();
  }

  auto txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    txn->AppendScanPredicate(plan_->table_oid_, plan_->filter_predicate_);
  }
}

void IndexScanExecutor::LoadPointLookupRids() {
  std::vector<Value> values;
  values.push_back(plan_->pred_keys_[current_key_idx_]->Evaluate(nullptr, plan_->OutputSchema()));
  Tuple key_tuple(values, index_info_->index_->GetKeySchema());
  point_lookup_rids_.clear();
  index_info_->index_->ScanKey(key_tuple, &point_lookup_rids_, exec_ctx_->GetTransaction());
  point_lookup_rid_idx_ = 0;
  prefetched_end_ = 0;
}
void IndexScanExecutor::PrefetchWindow() {
  auto *bpm = exec_ctx_->GetBufferPoolManager();
  const auto window = bpm->PrefetchWindow();
  const bool point = !plan_->pred_keys_.empty();
  const auto current = point ? point_lookup_rid_idx_ : full_scan_entry_idx_;
  const auto size = point ? point_lookup_rids_.size() : full_scan_entries_.size();
  if (window == 0 || current < prefetched_end_) {
    return;
  }
  prefetched_end_ = current + std::min(window, size - current);
  std::vector<page_id_t> pages;
  pages.reserve(prefetched_end_ - current);
  for (auto i = current; i < prefetched_end_; ++i) {
    const auto page = (point ? point_lookup_rids_[i] : full_scan_entries_[i].second).GetPageId();
    if (std::find(pages.begin(), pages.end(), page) == pages.end()) {
      pages.push_back(page);
    }
  }
  bpm->PrefetchPages(pages);
}
// MVCC版本
auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!plan_->pred_keys_.empty()) {
    auto txn = exec_ctx_->GetTransaction();
    auto *txn_mgr = exec_ctx_->GetTransactionManager();
    auto table_info = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
    while (true) {
      while (point_lookup_rid_idx_ >= point_lookup_rids_.size()) {
        if (current_key_idx_ + 1 >= plan_->pred_keys_.size()) {
          return false;
        }
        current_key_idx_++;
        LoadPointLookupRids();
      }

      PrefetchWindow();
      *rid = point_lookup_rids_[point_lookup_rid_idx_++];
      auto [base_meta, base_tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_info->table_.get(), *rid);
      auto undo_logs = CollectUndoLogs(*rid, base_meta, base_tuple, undo_link, txn, txn_mgr);
      if (!undo_logs.has_value()) {
        continue;
      }
      std::optional<Tuple> visible_tuple;
      if (undo_logs->empty()) {
        if (base_meta.is_deleted_) {
          continue;
        }
        visible_tuple = base_tuple;
      } else {
        visible_tuple = ReconstructTuple(&table_info->schema_, base_tuple, base_meta, *undo_logs);
      }
      if (!visible_tuple.has_value()) {
        continue;
      }
      const auto expected = plan_->pred_keys_[current_key_idx_]->Evaluate(nullptr, plan_->OutputSchema());
      const auto actual = visible_tuple->GetValue(&table_info->schema_, index_info_->index_->GetKeyAttrs()[0]);
      if (actual.CompareEquals(expected) != CmpBool::CmpTrue) {
        continue;
      }
      if (!emitted_rids_.insert(*rid).second) {
        continue;
      }
      *tuple = *visible_tuple;
      return true;
    }
  }

  auto txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  auto table_info = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
  while (full_scan_entry_idx_ < full_scan_entries_.size()) {
    PrefetchWindow();
    const auto &[entry_key, entry_rid] = full_scan_entries_[full_scan_entry_idx_++];
    *rid = entry_rid;

    auto [base_meta, base_tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_info->table_.get(), *rid);

    // 通过 undo log 重建该 rid 在当前事务视角下的可见版本
    auto undo_logs_opt = CollectUndoLogs(*rid, base_meta, base_tuple, undo_link, txn, txn_mgr);
    if (!undo_logs_opt.has_value()) {
      continue;
    }

    std::optional<Tuple> visible_tuple_opt;
    if (undo_logs_opt->empty()) {
      if (base_meta.is_deleted_) {
        continue;
      }
      visible_tuple_opt = base_tuple;
    } else {
      visible_tuple_opt = ReconstructTuple(&table_info->schema_, base_tuple, base_meta, *undo_logs_opt);
    }

    if (!visible_tuple_opt.has_value()) {
      continue;
    }
    const auto &visible_tuple = *visible_tuple_opt;

    // 普通二级索引会保留旧键，以便旧快照仍能定位同一 RID。全索引扫描只能在 entry
    // 的物理键与当前事务可见 tuple 的键一致时输出它，否则更新后的行会出现在旧排序位置。
    auto visible_key_tuple = visible_tuple.KeyFromTuple(table_info->schema_, *index_info_->index_->GetKeySchema(),
                                                        index_info_->index_->GetKeyAttrs());
    IntegerKeyType_BTree visible_index_key;
    visible_index_key.SetFromKey(visible_key_tuple);
    if (std::memcmp(entry_key.data_, visible_index_key.data_, sizeof(visible_index_key.data_)) != 0 ||
        !emitted_rids_.insert(*rid).second) {
      continue;
    }

    *tuple = visible_tuple;
    return true;
  }
  return false;
}

}  // namespace bustub
