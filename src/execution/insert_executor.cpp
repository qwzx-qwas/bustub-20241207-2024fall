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

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {}

void InsertExecutor::Init() {
    //初始化child executor
    child_executor_->Init();
    auto *catalog = exec_ctx_->GetCatalog();
    //获取要插入的表
    table_info = catalog->GetTable(plan_->TableOid());
    //获得表的堆（实际存储数据的地方）
    table_heap = table_info->table_.get();
    //获得表的索引信息
    indexes_ = catalog->GetTableIndexes(table_info->name_);
    //初始化状态量
    executed_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
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
    //从child executor中获取要插入的tuples
    while (child_executor_->Next(&child_tuple, &child_rid)) {
        // 构造 TupleMeta
        TupleMeta meta{0, false};
        // 插入到表的堆中
        auto inserted_rid = table_heap->InsertTuple(meta, child_tuple, exec_ctx_->GetLockManager(),
                                                    exec_ctx_->GetTransaction(), table_info->oid_);
        if (inserted_rid.has_value()) {
            insert_count++;
            //更新相关索引
            for (auto index_info : indexes_) {
                //根据tuple和索引的schema生成索引键值
                Tuple index_key = child_tuple.KeyFromTuple(table_info->schema_, *index_info->index_->GetKeySchema(),
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
