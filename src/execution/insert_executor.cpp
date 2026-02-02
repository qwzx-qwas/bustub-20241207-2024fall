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

#include "execution/executors/insert_executor.h"
#include "type/value_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  //初始化child executor
  child_executor_->Init();
  auto *catalog = exec_ctx_->GetCatalog();
  //获取要插入的表
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
/*auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  //先检查是否已经执行过插入
  if (executed_) {
    return false;
  }
  executed_ = true;
  // Insert logic here

  //计数器，记录插入了多少行
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
      //更新相关索引
      for (auto index_info : indexes_) {
        //根据tuple和索引的schema生成索引键值
        Tuple index_key = tuple_entry.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                                   index_info->index_->GetKeyAttrs());
        //插入索引
        index_info->index_->InsertEntry(index_key, *inserted_rid, exec_ctx_->GetTransaction());
      }
    }
  }
  //构造返回的tuple，表示插入的行数
  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(insert_count));
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}*/

//MVCC版InsertExecutor::Next
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {

  //执行插入操作
    if (executed_) {
    return false;
  }
  executed_ = true;
  // Insert logic here

  //计数器，记录插入了多少行
  int insert_count = 0;
  Tuple child_tuple;
  RID child_rid;

  // 1. 收集所有需要插入的元组，避免 Halloween Problem 和无限循环
  std::vector<Tuple> tuples_to_insert;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    tuples_to_insert.push_back(child_tuple);
  }
  //获取当前事务
  auto txn = exec_ctx_->GetTransaction();
  //获取事务的临时时间戳(一个事务内的所有操作被视为一个整体。
  // 在事务提交之前，该事务修改的所有行都打上相同的“临时标记”。)
  timestamp_t txn_ts = txn->GetTransactionTempTs();

  //构造元数据
  TupleMeta meta = {txn_ts, false};
  
  // 2. 执行插入操作
  for (const auto &tuple_entry : tuples_to_insert) {
      //1.在TableHeap中创建Tuple

  //2设置Metadata
  //Timestamp：必须设置为当前事务的 临时时间戳
  //IsDeleted：设置为 false


  auto inserted_rid = table_heap_->InsertTuple(meta, tuple_entry, exec_ctx_->GetLockManager(),
                                               exec_ctx_->GetTransaction(), table_info_->oid_);


  //3记录写集合：调用 txn->AppendWriteSet(rid) 将新生成的 RID 加入事务的写集合中，
  // 以便后续提交或回滚

  //必须检查 inserted_rid 是否有值才能加入写集合
  if (inserted_rid.has_value()) {
    txn->AppendWriteSet(table_info_->oid_, *inserted_rid);
  }
  //4.无需undolog
  
  if (inserted_rid.has_value()) {
      insert_count++;
      //更新相关索引
      for (auto index_info : indexes_) {
        //根据tuple和索引的schema生成索引键值
        Tuple index_key = tuple_entry.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                                   index_info->index_->GetKeyAttrs());
        //插入索引
        index_info->index_->InsertEntry(index_key, *inserted_rid, exec_ctx_->GetTransaction());
      }
    }
  }
  //构造返回的tuple，表示插入的行数
  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(insert_count));
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}
}  // namespace bustub
