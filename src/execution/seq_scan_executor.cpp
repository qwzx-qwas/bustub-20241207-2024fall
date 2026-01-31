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
#include "execution/execution_common.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // Init的职责是初始化扫描器，设置迭代器到表的起始位置
  //先通过ExecutorContext获取Catalog
  auto catalog = exec_ctx_->GetCatalog();
  //拿到目标id(从plan中获取）的表信息
  table_info_ = catalog->GetTable(plan_->GetTableOid()).get();
  //初始化迭代器到表的起始位置
  iter_.emplace(table_info_->table_->MakeIterator());
}
/*
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (iter_.has_value() && !iter_->IsEnd()) {
    auto [meta, current_tuple] = iter_->GetTuple();
    if (!meta.is_deleted_) {
      if (plan_->filter_predicate_ != nullptr) {
        //在seq scan中应用谓词过滤
        auto value = plan_->filter_predicate_->Evaluate(&current_tuple, table_info_->schema_);
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
*/
//下面是MVCC版本的SeqScanExecutor::Next

//因为table_heap中的tuple永远是现在最新版本
//而你现在所处于的事务时间点，可能和最新版本不一样
//所以你需要根据当前事务的时间点，去重建你所需要的tuple版本
//通过CollectUndoLogs去获取当前事务的undo log（即pre_version)，
// 将获得的undo log输入进ReconstructTuple来完成重现特定时间戳时的tuple
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  //获取当前事务
  auto txn = exec_ctx_->GetTransaction();
  //获取事务管理器
  auto txn_mgr = exec_ctx_->GetTransactionManager();

  while (iter_.has_value() && !iter_->IsEnd()) {
    //获取当前的RID
    auto current_rid = iter_->GetRID();
    //获取当前的tuple和meta
    auto [meta, current_tuple] = iter_->GetTuple();
    //获取当前tuple的undo link
    auto undo_link = txn_mgr->GetUndoLink(current_rid);
    //收集重建当前tuple所需的undo logs
    auto undo_logs_opt = CollectUndoLogs(current_rid, meta, current_tuple, undo_link, txn, txn_mgr);
    //如果返回的undo_logs_opt是std::nullopt，说明当前tuple在该事务的时间点上不存在
    if (!undo_logs_opt.has_value()) {
      ++(*iter_);
      continue;
    }
    auto logs = std::move(*undo_logs_opt);
    //根据收集到的undo logs重建tuple
    Tuple projected_tuple;
    if (logs.empty()) {
      //没有undo log，说明当前tuple就是需要的版本
      //还是需要检查是否被删除
      //if(meta.is_deleted_) {
        //++(*iter_);
        //continue;
      //}
      projected_tuple = current_tuple;
    } else {
      //有undo log，调用ReconstructTuple进行重建
      auto reconstructed_tuple_opt = ReconstructTuple(&table_info_->schema_, current_tuple, meta, logs);
      
      if (!reconstructed_tuple_opt.has_value()) {
        ++(*iter_);
        continue;
      }
      projected_tuple = *reconstructed_tuple_opt;
    }


      if (plan_->filter_predicate_ != nullptr) {
        //在seq scan中应用谓词过滤
        auto value = plan_->filter_predicate_->Evaluate(&projected_tuple, table_info_->schema_);
        if (value.IsNull() || !value.GetAs<bool>()) {
          ++(*iter_);
          continue;
        }
      }
    *tuple = projected_tuple;
    *rid = current_rid;
    ++(*iter_);
    return true;
  }
  return false;
}


}  // namespace bustub
