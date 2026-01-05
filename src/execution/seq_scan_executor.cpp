//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
    //Init的职责是初始化扫描器，设置迭代器到表的起始位置
    //先通过ExecutorContext获取Catalog
    auto catalog = exec_ctx_->GetCatalog();
    //拿到目标id(从plan中获取）的表信息
    table_info = catalog->GetTable(plan_->GetTableOid()).get();
    //初始化迭代器到表的起始位置
    iter_.emplace(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (iter_.has_value() && !iter_->IsEnd()) {
    auto [meta, current_tuple] = iter_->GetTuple();
    if (!meta.is_deleted_) {
      if (plan_->filter_predicate_ != nullptr) {
        //在seq scan中应用谓词过滤
        auto value = plan_->filter_predicate_->Evaluate(&current_tuple, table_info->schema_);
        if (value.IsNull() || !value.GetAs<bool>()) {
          ++(*iter_);
          continue;
        }
      }
      *tuple = current_tuple;
      *rid = current_tuple.GetRid();
      ++(*iter_);
      return true;
    }
    ++(*iter_);
  }
  return false;
}

}  // namespace bustub
