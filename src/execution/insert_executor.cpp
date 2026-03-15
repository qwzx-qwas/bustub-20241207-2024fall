//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/insert_executor.h"
#include "type/value_factory.h"

namespace bustub {

namespace {

/** 作用：把子执行器输出按目标表 schema 重新物化，统一触发 VECTOR 维度校验。 */
auto MaterializeTupleForSchema(const Tuple &tuple, const Schema &source_schema, const Schema &target_schema) -> Tuple {
  std::vector<Value> values;
  values.reserve(target_schema.GetColumnCount());
  for (uint32_t col_idx = 0; col_idx < target_schema.GetColumnCount(); col_idx++) {
    values.push_back(tuple.GetValue(&source_schema, col_idx));
  }
  return Tuple(values, &target_schema);
}

/** 作用：识别当前索引是否属于向量索引，便于只对该类索引执行阶段一维护逻辑。 */
auto IsVectorIndex(const IndexInfo *index_info) -> bool {
  return index_info->index_->GetVectorDistanceMetric().has_value();
}

}  // namespace

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  // 初始化child executor
  child_executor_->Init();
  auto *catalog = exec_ctx_->GetCatalog();
  // 获取要插入的表
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
/*auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // 先检查是否已经执行过插入
  if (executed_) {
    return false;
  }
  executed_ = true;
  // Insert logic here

  // 计数器，记录插入了多少行
  int insert_count = 0;
  Tuple child_tuple;
  RID child_rid;

  // 1. 收集所有需要插入的元组，避免 Halloween Problem 和无限循环
  std::vector<Tuple> tuples_to_insert;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    tuples_to_insert.push_back(child_tuple);
  }

  // 2. 执行插入操作
  for (const auto &tuple_entry : tuples_to_insert) {
    // 构造 TupleMeta
    TupleMeta meta{0, false};
    // 插入到表的堆中
    auto inserted_rid = table_heap_->InsertTuple(meta, tuple_entry, exec_ctx_->GetLockManager(),
                                                 exec_ctx_->GetTransaction(), table_info_->oid_);
    if (inserted_rid.has_value()) {
      insert_count++;
      // 更新相关索引
      for (auto index_info : indexes_) {
        // 根据tuple和索引的schema生成索引键值
        Tuple index_key = tuple_entry.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                                   index_info->index_->GetKeyAttrs());
        // 插入索引
        index_info->index_->InsertEntry(index_key, *inserted_rid, exec_ctx_->GetTransaction());
      }
    }
  }
  // 构造返回的tuple，表示插入的行数
  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(insert_count));
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}*/

// MVCC版InsertExecutor::Next
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // 先检查是否已经执行过插入
  if (executed_) {
    return false;
  }
  executed_ = true;
  // Insert logic here

  // 计数器，记录插入了多少行
  int insert_count = 0;
  Tuple child_tuple;
  RID child_rid;

  // 1. 收集所有需要插入的元组，避免 Halloween Problem 和无限循环
  std::vector<Tuple> tuples_to_insert;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    tuples_to_insert.push_back(child_tuple);
  }
  // 获取当前事务
  auto txn = exec_ctx_->GetTransaction();
  const auto &child_output_schema = child_executor_->GetOutputSchema();
  // 获取事务的临时时间戳(一个事务内的所有操作被视为一个整体。
  // 在事务提交之前，该事务修改的所有行都打上相同的“临时标记”。)
  timestamp_t txn_ts = txn->GetTransactionTempTs();
  // 事务管理器
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  // 当前事务id
  timestamp_t curr_txn_id = txn->GetTransactionId();
  // 当前事务的读取时间戳
  auto read_ts = txn->GetReadTs();

  // 构造元数据
  TupleMeta meta = {txn_ts, false};

  // 2. 执行插入操作
  for (const auto &raw_tuple_entry : tuples_to_insert) {
    auto tuple_entry = MaterializeTupleForSchema(raw_tuple_entry, child_output_schema, table_info_->schema_);
    // 主键唯一性检查(先查index)
    bool reused_deleted_rid = false;
    for (auto index_info : indexes_) {
      if (index_info->is_primary_key_) {
        //根据tuple和索引的schema生成索引键值
        Tuple index_key = tuple_entry.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                                   index_info->index_->GetKeyAttrs());
        std::vector<RID> scan_result;
        index_info->index_->ScanKey(index_key, &scan_result, exec_ctx_->GetTransaction());
        if (!scan_result.empty()) {
          // 如果该索引指向的RID对应的tuple是deleted,应尝试用UpdateTupleAndUndoLink更新该RID
          if (scan_result.size() == 1) {
            RID existing_rid = scan_result[0];
            auto [existing_meta, existing_tuple] = table_info_->table_->GetTuple(existing_rid);
            auto tuple_ts = existing_meta.ts_;

            // write-write冲突检测
            if ((((tuple_ts & TXN_START_ID) != 0) && (tuple_ts != curr_txn_id)) ||
                ((tuple_ts > read_ts) && ((tuple_ts & TXN_START_ID) == 0))) {
              txn->SetTainted();
              throw ExecutionException("InsertExecutor::Next failed due to write-write conflict.");
            }

            if (existing_meta.is_deleted_) {
              // 生成 undo log，复用已有的RID
              std::optional<UndoLink> prev_link = txn_mgr->GetUndoLink(existing_rid);
              UndoLink prev_version = prev_link.has_value() ? *prev_link : UndoLink();
              // 当复用已删除的RID时，传入的base_tuple为nullptr
              UndoLog undo_log =
                  GenerateNewUndoLog(&table_info_->schema_, nullptr, &tuple_entry, tuple_ts, prev_version);
              UndoLink new_undo_link = txn->AppendUndoLog(undo_log);

              // 尝试用UpdateTupleAndUndoLink更新该RID
              bool update_success = UpdateTupleAndUndoLink(
                  txn_mgr, existing_rid, new_undo_link, table_heap_, txn, {curr_txn_id, false}, tuple_entry,
                  [tuple_ts](const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink> undo_link) {
                    return meta.ts_ == tuple_ts && meta.is_deleted_;
                  });
              if (update_success) {
                txn->AppendWriteSet(table_info_->oid_, existing_rid);
                for (auto maintained_index : indexes_) {
                  if (!IsVectorIndex(maintained_index)) {
                    continue;
                  }
                  Tuple index_key = tuple_entry.KeyFromTuple(table_info_->schema_, *maintained_index->index_->GetKeySchema(),
                                                             maintained_index->index_->GetKeyAttrs());
                  if (!maintained_index->index_->InsertEntry(index_key, existing_rid, exec_ctx_->GetTransaction())) {
                    txn->SetTainted();
                    throw ExecutionException("InsertExecutor::Next failed due to vector index insertion failure.");
                  }
                }
                insert_count++;
                reused_deleted_rid = true;
                break;
              }
            }
          }
          txn->SetTainted();
          throw ExecutionException("InsertExecutor::Next failed due to duplicate primary key.");
        }
      }
    }

    // 如果复用了已删除的RID，则跳过后续插入流程
    if (reused_deleted_rid) {
      continue;
    }

    // 写入Table Heap
    auto inserted_rid = table_heap_->InsertTuple(meta, tuple_entry, exec_ctx_->GetLockManager(),
                                                 exec_ctx_->GetTransaction(), table_info_->oid_);
    // 必须检查 inserted_rid 是否有值才能加入写集合
    if (!inserted_rid.has_value()) {
      continue;
    }
    // 记录写集合：调用 txn->AppendWriteSet(rid) 将新生成的 RID 加入事务的写集合中，
    // 以便后续提交或回滚
    txn->AppendWriteSet(table_info_->oid_, *inserted_rid);

    // 插入索引
    for (auto index_info : indexes_) {
      // 根据tuple和索引的schema生成索引键值
      Tuple index_key = tuple_entry.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                                 index_info->index_->GetKeyAttrs());
      if (!index_info->index_->InsertEntry(index_key, *inserted_rid, exec_ctx_->GetTransaction())) {
        txn->SetTainted();
        throw ExecutionException("InsertExecutor::Next failed due to index insertion failure.");
      }
    }

    insert_count++;
  }

  // 构造返回的tuple，表示插入的行数
  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(insert_count));
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}
}  // namespace bustub
