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

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}
//需要先删除旧的tuple，然后插入新的tuple
//需要注意更新索引

void UpdateExecutor::Init() {
  //初始化child executor
  child_executor_->Init();
  auto *catalog = exec_ctx_->GetCatalog();
  //获取要更新的表
  table_info = catalog->GetTable(plan_->TableOid());
  //获得表的堆（实际存储数据的地方）
  table_heap = table_info->table_.get();
  //获得表的索引信息
  indexes_ = catalog->GetTableIndexes(table_info->name_);
  //初始化状态量
  executed_ = false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  //先检查是否已经执行过更新
  if (executed_) {
    return false;
  }
  executed_ = true;
  // Update logic here
  uint32_t update_count = 0;
  Tuple old_tuple;
  RID old_rid;
  //从child executor中获取要更新的tuples
  while(child_executor_->Next(&old_tuple, &old_rid)) {
    //根据更新计划生成新的tuple
    std::vector<Value> updated_values;
    //构造更新后的值列表
    for(const auto &expr : plan_->target_expressions_) {
      updated_values.push_back(expr->Evaluate(&old_tuple, &table_info->schema_));
    }
    //构造新的tuple
    Tuple new_tuple(updated_values, &table_info->schema_);
    //先维护索引
    for (auto index_info : indexes_) {
      //根据旧tuple和索引的schema生成索引键值
      auto old_key = old_tuple.KeyFromTuple(table_info->schema_, *index_info->index_->GetKeySchema(),
                                           index_info->index_->GetKeyAttrs());
      //删除旧的索引项
      index_info->index_->DeleteEntry(old_key, old_rid, exec_ctx_->GetTransaction());
      //注意：新键的插入需要在下面拿到new_rid之后进行
    }
    //标记删除旧tuple
    TupleMeta old_meta = table_heap->GetTupleMeta(old_rid);
    old_meta.is_deleted_ = true;
    table_heap->UpdateTupleMeta(old_rid, old_meta);

    //插入新的tuple
    auto new_rid_opt = table_heap->InsertTuple(TupleMeta{0, false}, new_tuple, exec_ctx_->GetLockManager(),
                                               exec_ctx_->GetTransaction(), table_info->oid_);
    if (new_rid_opt.has_value()) {
      RID new_rid = new_rid_opt.value();
      update_count++;
      //更新相关索引
      for (auto index_info : indexes_) {
        //根据新tuple和索引的schema生成索引键值
        auto new_key = new_tuple.KeyFromTuple(table_info->schema_, *index_info->index_->GetKeySchema(),
                                             index_info->index_->GetKeyAttrs());
        //插入新的索引项
        index_info->index_->InsertEntry(new_key, new_rid, exec_ctx_->GetTransaction());
      }
  }
}
  //构造返回的tuple，表示更新的行数
  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(update_count));
  *tuple = Tuple(values, &GetOutputSchema());;
  return true;  
}

}  // namespace bustub
