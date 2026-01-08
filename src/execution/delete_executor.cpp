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

#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  //初始化child executor
  child_executor_->Init();
  auto *catalog = exec_ctx_->GetCatalog();
  //获取要删除的表
  table_info_ = catalog->GetTable(plan_->GetTableOid()).get();
  //获得表的堆（实际存储数据的地方）
  table_heap_ = table_info_->table_.get();
  //获得表的索引信息
  auto indexes = catalog->GetTableIndexes(table_info_->name_);
  for (const auto &index : indexes) {
    indexes_.push_back(index.get());
  }
  //初始化状态量
  deleted_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
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
    /*从表堆中删除tuple
    因为没有直接物理删除tuple的方法
    这里我们通过更新tuple的元信息来标记该tuple为已删除
    即逻辑删除
    */
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

}  // namespace bustub
