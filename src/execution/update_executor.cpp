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

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"
#include "type/value_factory.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}
// 需要先删除旧的tuple，然后插入新的tuple
// 需要注意更新索引

void UpdateExecutor::Init() {
  // 初始化child executor
  child_executor_->Init();
  auto *catalog = exec_ctx_->GetCatalog();
  // 获取要更新的表
  table_info_ = catalog->GetTable(plan_->GetTableOid()).get();
  // 获得表的堆（实际存储数据的地方）
  table_heap_ = table_info_->table_.get();
  // 获得表的索引信息
  auto indexes = catalog->GetTableIndexes(table_info_->name_);
  for (const auto &index : indexes) {
    indexes_.push_back(index.get());
  }
  // 初始化状态量
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
  //事务的临时时间戳
  timestamp_t txn_ts = txn->GetTransactionTempTs();

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

  struct DeferredInsert {
    Tuple new_tuple_;
  };
  std::vector<DeferredInsert> deferred_inserts;

  // ----------------------------------------------------------------
  // 阶段 1: 处理所有 Delete (针对主键更新) 和 In-Place Update
  // ----------------------------------------------------------------
  for (const auto &[curr_old_tuple, curr_old_rid] : tuples_to_update) {
    auto tuple_meta = table_heap_->GetTupleMeta(curr_old_rid);
    auto tuple_ts = tuple_meta.ts_;

    //如果是write-write冲突，直接报错
    //如果是未提交事务或读取时间戳落后于当前事务ID，说明有冲突
    //检测到冲突时，将事务状态设为 TAINTED，并抛出 ExecutionException
    if (((tuple_ts & TXN_START_ID) != 0 && (tuple_ts != curr_txn_id)) ||
        ((tuple_ts > read_ts) && ((tuple_ts & TXN_START_ID) == 0))) {
      txn->SetTainted();
      throw ExecutionException("UpdateExecutor::Next failed due to write-write conflict.");
    }

    //构造更新后的值列表
    std::vector<Value> updated_values;
    for (const auto &expr : plan_->target_expressions_) {
      updated_values.push_back(expr->Evaluate(&curr_old_tuple, table_info_->schema_));
    }
    //构造新的tuple
    Tuple new_tuple(updated_values, &table_info_->schema_);

    //处理undolog
    //判断是否是第一次修改自己所修改的tuple
    //第一次则需要用GenerateNewUndoLog生成新的undolog
    //否则需要用GenerateUpdatedUndoLog生成更新后的undolog
    bool is_self_modified = (tuple_ts == curr_txn_id);
    std::optional<UndoLink> prev_link = txn_mgr->GetUndoLink(curr_old_rid);
    UndoLog undo_log;

    // 判断是否修改了主键
    bool primary_key_changed = false;
    struct PrimaryKeyInfo {
      IndexInfo *index_info_;
      Tuple old_key_;
      Tuple new_key_;
    };
    std::vector<PrimaryKeyInfo> pk_indexes;
    for (auto index_info : indexes_) {
      if (index_info->is_primary_key_) {
        auto old_key = curr_old_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                                   index_info->index_->GetKeyAttrs());
        auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                              index_info->index_->GetKeyAttrs());
        if (!IsTupleContentEqual(old_key, new_key)) {
          primary_key_changed = true;
        }
        pk_indexes.push_back(PrimaryKeyInfo{index_info, old_key, new_key});
      }
    }

    //主键没变，直接更新主表内容，不删除索引条目
    if (!primary_key_changed) {
      bool update_success = false;
      if (!is_self_modified) {
        //第一次修改
        //获取之前的版本链接
        UndoLink prev_version = prev_link.has_value() ? *prev_link : UndoLink();
        //调用 GenerateNewUndoLog，把当前主表里的旧值存进日志
        undo_log = GenerateNewUndoLog(&table_info_->schema_, &curr_old_tuple, &new_tuple, tuple_ts, prev_version);
        //在事务里新建一条日志记录
        UndoLink new_undo_link = txn->AppendUndoLog(undo_log);
        //原子性地修改主表数据，并将主表的 UndoLink 指向这条新日志
        //手动维护事务的写集，确保 Commit 时能更新时间戳
        txn->AppendWriteSet(table_info_->oid_, curr_old_rid);
        update_success = UpdateTupleAndUndoLink(
            txn_mgr, curr_old_rid, new_undo_link, table_heap_, txn, {curr_txn_id, false}, new_tuple,
            // Check function
            [tuple_ts](const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink> undo_link) {
              return meta.ts_ == tuple_ts;
            });
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
        update_success = table_heap_->UpdateTupleInPlace(
            {curr_txn_id, false}, new_tuple, curr_old_rid,
            // Check function
            [tuple_ts](const TupleMeta &meta, const Tuple &tuple, RID rid) { return meta.ts_ == tuple_ts; });
      }

      if (!update_success) {
        throw ExecutionException("UpdateExecutor::Next failed to update tuple.");
      }

      update_count++;
      continue;
    }

    //主键变了，先做删除语义，再做插入语义
    bool delete_success = false;
    if (!is_self_modified) {
      //第一次修改
      //获取之前的版本链接
      UndoLink prev_version = prev_link.has_value() ? *prev_link : UndoLink();
      //调用 GenerateNewUndoLog，把当前主表里的旧值存进日志
      //这里的target_tuple为nullptr，表示删除操作
      undo_log = GenerateNewUndoLog(&table_info_->schema_, &curr_old_tuple, nullptr, tuple_ts, prev_version);
      //在事务里新建一条日志记录
      UndoLink new_undo_link = txn->AppendUndoLog(undo_log);
      //原子性地修改主表数据，并将主表的 UndoLink 指向这条新日志
      delete_success = UpdateTupleAndUndoLink(
          txn_mgr, curr_old_rid, new_undo_link, table_heap_, txn, {curr_txn_id, true}, curr_old_tuple,
          // Check function
          [tuple_ts](const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink> undo_link) {
            return meta.ts_ == tuple_ts;
          });
    } else {
      //不是第一次修改，获取之前的undolog
      if (prev_link.has_value() && prev_link->IsValid()) {
        UndoLog old_undo_log = txn_mgr->GetUndoLog(*prev_link);
        //将本次修改与事务中已有的旧日志合并。
        //这里的target_tuple为nullptr，表示删除操作
        undo_log = GenerateUpdatedUndoLog(&table_info_->schema_, &curr_old_tuple, nullptr, old_undo_log);
        //直接覆盖原来的日志，不增加日志数量。
        txn->ModifyUndoLog(prev_link->prev_log_idx_, undo_log);
      }
      //只改主表内容，不改 UndoLink（因为它已经指向正确的日志位置了）
      delete_success = table_heap_->UpdateTupleInPlace(
          {curr_txn_id, true}, curr_old_tuple, curr_old_rid,
          // Check function
          [tuple_ts](const TupleMeta &meta, const Tuple &tuple, RID rid) { return meta.ts_ == tuple_ts; });
    }

    if (!delete_success) {
      throw ExecutionException("UpdateExecutor::Next failed to update tuple.");
    }
    txn->AppendWriteSet(table_info_->oid_, curr_old_rid);
    deferred_inserts.push_back({new_tuple});
  }

  // ----------------------------------------------------------------
  // 阶段 2: 执行所有 Deferred Inserts (针对主键更新)
  // ----------------------------------------------------------------
  for (const auto &insert_info : deferred_inserts) {
    const auto &new_tuple = insert_info.new_tuple_;

    //再insert, 先做唯一性检查
    //如果索引已有条目且指向deleted RID，则尝试复用该RID
    bool reused_deleted_rid = false;
    for (auto index_info : indexes_) {
      if (!index_info->is_primary_key_) {
        continue;
      }
      auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                            index_info->index_->GetKeyAttrs());
      std::vector<RID> scan_result;
      index_info->index_->ScanKey(new_key, &scan_result, exec_ctx_->GetTransaction());
      if (!scan_result.empty()) {
        if (scan_result.size() == 1) {
          RID existing_rid = scan_result[0];
          auto [existing_meta, existing_tuple] = table_info_->table_->GetTuple(existing_rid);
          auto existing_ts = existing_meta.ts_;

          // write-write冲突检测
          if (((existing_ts & TXN_START_ID) != 0 && (existing_ts != curr_txn_id)) ||
              ((existing_ts > read_ts) && ((existing_ts & TXN_START_ID) == 0))) {
            txn->SetTainted();
            throw ExecutionException("UpdateExecutor::Next failed due to write-write conflict.");
          }

          if (existing_meta.is_deleted_) {
            //生成 undo log，复用已有的RID
            std::optional<UndoLink> existing_prev_link = txn_mgr->GetUndoLink(existing_rid);
            UndoLink prev_version = existing_prev_link.has_value() ? *existing_prev_link : UndoLink();
            //当复用已删除的RID时，传入的base_tuple为nullptr
            UndoLog reuse_undo_log =
                GenerateNewUndoLog(&table_info_->schema_, nullptr, &new_tuple, existing_ts, prev_version);
            UndoLink reuse_undo_link = txn->AppendUndoLog(reuse_undo_log);
            //尝试用UpdateTupleAndUndoLink更新该RID
            bool update_success = UpdateTupleAndUndoLink(
                txn_mgr, existing_rid, reuse_undo_link, table_heap_, txn, {curr_txn_id, false}, new_tuple,
                // Check function
                [existing_ts](const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink> undo_link) {
                  return meta.ts_ == existing_ts && meta.is_deleted_;
                });
            if (update_success) {
              txn->AppendWriteSet(table_info_->oid_, existing_rid);
              reused_deleted_rid = true;
              break;
            }
          }
        }

        txn->SetTainted();
        throw ExecutionException("UpdateExecutor::Next failed due to duplicate primary key.");
      }
    }

    if (reused_deleted_rid) {
      update_count++;
      continue;
    }

    //插入新的tuple
    auto inserted_rid = table_heap_->InsertTuple({txn_ts, false}, new_tuple, exec_ctx_->GetLockManager(),
                                                 exec_ctx_->GetTransaction(), table_info_->oid_);
    if (!inserted_rid.has_value()) {
      throw ExecutionException("UpdateExecutor::Next failed to insert tuple.");
    }
    txn->AppendWriteSet(table_info_->oid_, *inserted_rid);

    //插入索引
    for (auto index_info : indexes_) {
      auto index_key = new_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                              index_info->index_->GetKeyAttrs());
      if (!index_info->index_->InsertEntry(index_key, *inserted_rid, exec_ctx_->GetTransaction())) {
        txn->SetTainted();
        throw ExecutionException("UpdateExecutor::Next failed due to index insertion failure.");
      }
    }

    update_count++;
  }

  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(update_count));
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

}  // namespace bustub
