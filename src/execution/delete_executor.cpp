//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  // 初始化child executor
  child_executor_->Init();
  auto *catalog = exec_ctx_->GetCatalog();
  // 获取要删除的表
  table_info_ = catalog->GetTable(plan_->GetTableOid()).get();
  // 获得表的堆（实际存储数据的地方）
  table_heap_ = table_info_->table_.get();
  // 获得表的索引信息
  auto indexes = catalog->GetTableIndexes(table_info_->name_);
  for (const auto &index : indexes) {
    indexes_.push_back(index.get());
  }
  //初始化状态量
  deleted_ = false;
}
/*auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  //先检查是否已经执行过删除
  if (deleted_) {
    return false;
  }
  deleted_ = true;
  // Delete logic here

  //计数器，记录删除了多少行
  int delete_count = 0;
  Tuple old_tuple;
  RID old_rid;

  // 1. 收集所有需要删除的元组，避免 Halloween Problem 和迭代器失效
  std::vector<std::pair<Tuple, RID>> tuples_to_delete;
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    tuples_to_delete.emplace_back(old_tuple, old_rid);
  }

  // 2. 执行删除操作
  for (const auto &[curr_old_tuple, curr_old_rid] : tuples_to_delete) {
    //删除表中的tuple
    for (auto index_info : indexes_) {
      //根据tuple和索引的schema生成索引键值
      auto index_key = curr_old_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                                   index_info->index_->GetKeyAttrs());
      //从索引中删除对应的键值对
      index_info->index_->DeleteEntry(index_key, curr_old_rid, exec_ctx_->GetTransaction());
    }
    //从表堆中删除tuple
    //因为没有直接物理删除tuple的方法
    //这里我们通过更新tuple的元信息来标记该tuple为已删除
    //即逻辑删除

    TupleMeta meta = table_heap_->GetTupleMeta(curr_old_rid);
    meta.is_deleted_ = true;
    table_heap_->UpdateTupleMeta(meta, curr_old_rid);
    delete_count++;
  }
  //构造输出tuple，包含删除的行数
  std::vector<Value> values;
  values.push_back(ValueFactory::GetIntegerValue(delete_count));
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}
*/
auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  //先检查是否已经执行过删除
  if (deleted_) {
    return false;
  }

  auto txn = exec_ctx_->GetTransaction();
  //当前事务id
  timestamp_t curr_txn_id = txn->GetTransactionId();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  //当前事务的读取时间戳（最新已提交版本的时间戳）
  auto read_ts = txn->GetReadTs();

  deleted_ = true;
  // delete logic here
  uint32_t delete_count = 0;
  Tuple old_tuple;
  RID old_rid;

  // 1. 收集所有需要删除的元组，避免 Halloween Problem 和迭代器失效
  std::vector<std::pair<Tuple, RID>> tuples_to_delete;
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    tuples_to_delete.emplace_back(old_tuple, old_rid);
  }

  // 执行删除操作
  for (const auto &[curr_old_tuple, curr_old_rid] : tuples_to_delete) {
    auto tuple_meta = table_heap_->GetTupleMeta(curr_old_rid);
    // 读取的是该 tuple 在元数据里保存的 MVCC 标识/时间戳 —— 它不是仅仅“临时”的任意值，
    // 而是用来表示该行当前版本的版本信息：
    // 若含有 TXN_START_ID 标志位，则 tuple_ts 表示一个未提交的事务 id（即该行被某个事务占用）；
    // 若不含该标志位，则 tuple_ts 表示该行最后一次提交的时间戳（commit ts）。
    auto tuple_ts = tuple_meta.ts_;

    // 如果是write-write冲突，直接报错
    // 如果是未提交事务或读取时间戳落后于当前事务ID，说明有冲突
    // 检测到冲突时，将事务状态设为 TAINTED，并抛出 ExecutionException
    if ((((tuple_ts & TXN_START_ID) != 0) && (tuple_ts != curr_txn_id)) ||
        ((tuple_ts > read_ts) && ((tuple_ts & TXN_START_ID) == 0))) {
      txn->SetTainted();
      throw ExecutionException("DeleteExecutor::Next failed due to write-write conflict.");
    }

    //处理undolog
    //判断是否是第一次修改自己所修改的tuple
    //第一次则需要用GenerateNewUndoLog生成新的undolog
    //否则需要用GenerateUpdatedUndoLog生成更新后的undolog
    bool is_self_modified = (tuple_ts == curr_txn_id);
    UndoLog undo_log;

    std::optional<UndoLink> prev_link = txn_mgr->GetUndoLink(curr_old_rid);
    //对undolog的处理
    bool delete_success = false;
    if (!is_self_modified) {
      //第一次修改
      //获取之前的版本链接
      UndoLink prev_version;
      if (prev_link.has_value()) {
        prev_version = *prev_link;
      } else {
        prev_version = UndoLink();
      }
      // 调用 GenerateNewUndoLog，把当前主表里的旧值存进日志
      // 这里的target_tuple为nullptr，表示删除操作
      undo_log = GenerateNewUndoLog(&table_info_->schema_, &curr_old_tuple, nullptr, tuple_ts, prev_version);
      // 在事务里新建一条日志记录
      UndoLink new_undo_link = txn->AppendUndoLog(undo_log);
      // 原子性地修改主表数据，并将主表的 UndoLink 指向这条新日志
      /*原子性地同时更新主表中 curr_old_rid 位置的 tuple 内容（用 curr_old_tuple 覆盖），
      并将该 tuple 的 UndoLink 指向新的 undo log（new_undo_link）。
      这样可以保证主表和 UndoLink 的一致性，防止并发下出现部分更新。
      通常用于第一次被本事务修改该行时，需要新建 UndoLog 并让主表指向它。
      返回值为 bool，表示更新是否成功。*/
      delete_success = UpdateTupleAndUndoLink(
          txn_mgr, curr_old_rid, new_undo_link, table_heap_, txn, {curr_txn_id, true}, curr_old_tuple,
          // Check function
          [tuple_ts](const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink> undo_link) {
            return meta.ts_ == tuple_ts;
          });
    } else {
      // 不是第一次修改，获取之前的undolog
      if (prev_link.has_value() && prev_link->IsValid()) {
        UndoLog old_undo_log = txn_mgr->GetUndoLog(*prev_link);
        // 将本次修改与事务中已有的旧日志合并。
        // 这里的target_tuple为nullptr，表示删除操作
        undo_log = GenerateUpdatedUndoLog(&table_info_->schema_, &curr_old_tuple, nullptr, old_undo_log);
        // 直接覆盖原来的日志，不增加日志数量。
        txn->ModifyUndoLog(prev_link->prev_log_idx_, undo_log);
      }

      // 只改主表内容，不改 UndoLink（因为它已经指向正确的日志位置了）
      /*在表的主存储（TableHeap）中，直接用 curr_old_tuple 的内容覆盖 curr_old_rid 位置的旧 tuple 数据，
      同时将该 tuple 的元数据（TupleMeta）中的事务 ID 设置为 curr_txn_id，is_deleted 标记为 true。
      这个操作不会修改 UndoLink，只更新主表内容，适用于同一事务多次更新同一行的情况
      （即已经有 UndoLog 记录，无需再新建 UndoLog，只需覆盖主表数据）。
      返回值为 bool，表示更新是否成功。*/
      delete_success = table_heap_->UpdateTupleInPlace(
          {curr_txn_id, true}, curr_old_tuple, curr_old_rid,
          // Check function
          [tuple_ts](const TupleMeta &meta, const Tuple &tuple, RID rid) { return meta.ts_ == tuple_ts; });
    }

    if (delete_success) {
      txn->AppendWriteSet(table_info_->oid_, curr_old_rid);
      // 维护索引，由4.2要求：不再调用 index->DeleteEntry(...) 来移除索引条目。
      // 只做表中 tuple 的 UpdateTupleAndUndoLink（写 undo log）并标记 is_deleted。
      /*for (auto index_info : indexes_) {
        auto old_key = curr_old_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
      index_info->index_->GetKeyAttrs());
        //删除旧的索引项
        index_info->index_->DeleteEntry(old_key, curr_old_rid, txn);
      }*/
      delete_count++;
    } else {
      throw ExecutionException("DeleteExecutor::Next failed to update tuple.");
    }
  }
  // 构造返回的tuple，表示更新的行数
  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(delete_count));
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

}  // namespace bustub
