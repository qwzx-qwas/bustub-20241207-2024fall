//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"
#include "type/value_factory.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}
//需要先删除旧的tuple，然后插入新的tuple
//需要注意更新索引

void UpdateExecutor::Init() {
  //初始化child executor
  child_executor_->Init();
  auto *catalog = exec_ctx_->GetCatalog();
  //获取要更新的表
  table_info_ = catalog->GetTable(plan_->GetTableOid()).get();
  //获得表的堆（实际存储数据的地方）
  table_heap_ = table_info_->table_.get();
  //获得表的索引信息
  auto indexes = catalog->GetTableIndexes(table_info_->name_);
  for (const auto &index : indexes) {
    indexes_.push_back(index.get());
  }
  //初始化状态量
  executed_ = false;
}
/*auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  //先检查是否已经执行过更新
  if (executed_) {
    return false;
  }
  executed_ = true;
  // Update logic here
  uint32_t update_count = 0;
  Tuple old_tuple;
  RID old_rid;

  // 1. 收集所有需要更新的元组，避免 Halloween Problem 和迭代器失效
  //因为一边读取一边更新会导致后续更新的tuple被再次读到，导致死循环或迭代器失效
  // 所以提前收集起来（对delete也是一样）
  std::vector<std::pair<Tuple, RID>> tuples_to_update;
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    tuples_to_update.emplace_back(old_tuple, old_rid);
  }

  // 2. 执行更新操作
  for (const auto &[curr_old_tuple, curr_old_rid] : tuples_to_update) {
    //根据更新计划生成新的tuple
    std::vector<Value> updated_values;
    //构造更新后的值列表
    for (const auto &expr : plan_->target_expressions_) {
      updated_values.push_back(expr->Evaluate(&curr_old_tuple, table_info_->schema_));
    }
    //构造新的tuple
    Tuple new_tuple(updated_values, &table_info_->schema_);

    //先维护索引
    for (auto index_info : indexes_) {
      //根据旧tuple和索引的schema生成索引键值
      auto old_key = curr_old_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                                 index_info->index_->GetKeyAttrs());
      //删除旧的索引项
      index_info->index_->DeleteEntry(old_key, curr_old_rid, exec_ctx_->GetTransaction());
      //注意：新键的插入需要在下面拿到new_rid之后进行
    }

    //标记删除旧tuple
    TupleMeta old_meta = table_heap_->GetTupleMeta(curr_old_rid);
    old_meta.is_deleted_ = true;
    table_heap_->UpdateTupleMeta(old_meta, curr_old_rid);

    //插入新的tuple
    auto new_rid_opt = table_heap_->InsertTuple(TupleMeta{0, false}, new_tuple, exec_ctx_->GetLockManager(),
                                                exec_ctx_->GetTransaction(), table_info_->oid_);
    if (new_rid_opt.has_value()) {
      RID new_rid = new_rid_opt.value();
      update_count++;
      //更新相关索引
      for (auto index_info : indexes_) {
        //根据新tuple和索引的schema生成索引键值
        auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                              index_info->index_->GetKeyAttrs());
        //插入新的索引项
        index_info->index_->InsertEntry(new_key, new_rid, exec_ctx_->GetTransaction());
      }
    }
  }

  //构造返回的tuple，表示更新的行数
  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(update_count));
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}*/

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {

  //先检查是否已经执行过更新
  if (executed_) {
    return false;
  }

  auto txn = exec_ctx_->GetTransaction();
  //当前事务id
  timestamp_t curr_txn_id = txn->GetTransactionId();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  auto read_ts = txn->GetReadTs();

  executed_ = true;
  // Update logic here
  uint32_t update_count = 0;
  Tuple old_tuple;
  RID old_rid;

  // 收集所有需要更新的元组，避免 Halloween Problem 和迭代器失效
  //因为一边读取一边更新会导致后续更新的tuple被再次读到，导致死循环或迭代器失效
  // 所以提前收集起来（对delete也是一样）
  std::vector<std::pair<Tuple, RID>> tuples_to_update;
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    tuples_to_update.emplace_back(old_tuple, old_rid);
  }

  // 执行更新操作
  for (const auto &[curr_old_tuple, curr_old_rid] : tuples_to_update) {

    auto tuple_meta = table_heap_->GetTupleMeta(curr_old_rid);
    auto tuple_ts = tuple_meta.ts_;
    
  //如果是write-write冲突，直接报错
  //如果是未提交事务或读取时间戳落后于当前事务ID，说明有冲突
  //检测到冲突时，将事务状态设为 TAINTED，并抛出 ExecutionException
  if (((tuple_ts & TXN_START_ID) && (tuple_ts != curr_txn_id)) ||
    ((tuple_ts > read_ts) && !(tuple_ts & TXN_START_ID))) {
    txn->SetTainted();
    throw ExecutionException("UpdateExecutor::Next failed due to write-write conflict.");
  }

    //根据更新计划生成新的tuple
    std::vector<Value> updated_values;
    //处理undolog
    //判断是否是第一次修改自己所修改的tuple
    //第一次则需要用GenerateNewUndoLog生成新的undolog
    //否则需要用GenerateUpdatedUndoLog生成更新后的undolog
    bool is_self_modified = (tuple_ts == curr_txn_id);
    UndoLog undo_log;

    //构造更新后的值列表
    for (const auto &expr : plan_->target_expressions_) {
      updated_values.push_back(expr->Evaluate(&curr_old_tuple, table_info_->schema_));
    }
    //构造新的tuple
    Tuple new_tuple(updated_values, &table_info_->schema_);
    std::optional<UndoLink> prev_link = txn_mgr->GetUndoLink(curr_old_rid);
    //对undolog的处理
    bool update_success = false;
    if (!is_self_modified) {
      //第一次修改
      //获取之前的版本链接
      UndoLink prev_version;
      if (prev_link.has_value()) {
        prev_version = *prev_link;
      } else {
        prev_version = UndoLink();
      }
      //调用 GenerateNewUndoLog，把当前主表里的旧值存进日志
      undo_log = GenerateNewUndoLog(&table_info_->schema_, &curr_old_tuple, &new_tuple, tuple_ts, prev_version);
      //在事务里新建一条日志记录
      UndoLink new_undo_link = txn->AppendUndoLog(undo_log);
      //原子性地修改主表数据，并将主表的 UndoLink 指向这条新日志
      /*原子性地同时更新主表中 curr_old_rid 位置的 tuple 内容（用 new_tuple 覆盖），
      并将该 tuple 的 UndoLink 指向新的 undo log（new_undo_link）。
      这样可以保证主表和 UndoLink 的一致性，防止并发下出现部分更新。
      通常用于第一次被本事务修改该行时，需要新建 UndoLog 并让主表指向它。
      返回值为 bool，表示更新是否成功。*/
      //手动维护事务的写集，确保 Commit 时能更新时间戳
       txn->AppendWriteSet(table_info_->oid_, curr_old_rid);
      update_success =UpdateTupleAndUndoLink(txn_mgr, curr_old_rid, new_undo_link, table_heap_, txn, {curr_txn_id, false}, new_tuple);
    } else {
      //不是第一次修改，获取之前的undolog
        if (prev_link.has_value() && prev_link->IsValid()) {
          UndoLog old_undo_log = txn_mgr->GetUndoLog(*prev_link);
          //将本次修改与事务中已有的旧日志合并。
          undo_log = GenerateUpdatedUndoLog(&table_info_->schema_, &curr_old_tuple, &new_tuple, old_undo_log);  
          //直接覆盖原来的日志，不增加日志数量。
          txn->ModifyUndoLog(prev_link->prev_log_idx_, undo_log);
        }

        //只改主表内容，不改 UndoLink（因为它已经指向正确的日志位置了）
        /*在表的主存储（TableHeap）中，直接用 new_tuple 的内容覆盖 curr_old_rid 位置的旧 tuple 数据，
        同时将该 tuple 的元数据（TupleMeta）中的事务 ID 设置为 curr_txn_id，is_deleted 标记为 false。
        这个操作不会修改 UndoLink，只更新主表内容，适用于同一事务多次更新同一行的情况
        （即已经有 UndoLog 记录，无需再新建 UndoLog，只需覆盖主表数据）。
        返回值为 bool，表示更新是否成功。*/
        update_success = table_heap_->UpdateTupleInPlace({curr_txn_id, false}, new_tuple, curr_old_rid);
    }
if (update_success) {
    //维护索引
    for (auto index_info : indexes_) {
      auto old_key = curr_old_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs());
      auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs());
      
      // 只有键值改变时才更新索引，且 RID 始终是 curr_old_rid
      if (!IsTupleContentEqual(old_key, new_key)) {
        //删除旧的索引项，插入新的索引项
        index_info->index_->DeleteEntry(old_key, curr_old_rid, txn);
        index_info->index_->InsertEntry(new_key, curr_old_rid, txn);
      }
    }
      
        update_count++;
      } else {
        throw ExecutionException("UpdateExecutor::Next failed to update tuple.");
      }
    }
  //构造返回的tuple，表示更新的行数
  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(update_count));
  *tuple = Tuple(values, &GetOutputSchema());
  return true;

}


}  // namespace bustub
