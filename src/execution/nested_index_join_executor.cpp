//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

//遍历左表加索引查询右表

void NestIndexJoinExecutor::Init() {
  //初始化子执行器
  child_executor_->Init();
  //重置状态
  result_rids_.clear();
  rid_index_ = 0;

  auto catalog = exec_ctx_->GetCatalog();
  index_info_ = catalog->GetIndex(plan_->GetIndexOid()).get();
  table_info_ = catalog->GetTable(index_info_->table_name_).get();
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto table_heap = table_info_->table_.get();

  while (true) {
    // 1. 如果当前的 result_rids_ 还没处理完，继续处理
    while (rid_index_ < result_rids_.size()) {
      auto right_rid = result_rids_[rid_index_];
      rid_index_++;  // 移动到下一个

      auto [meta, right_tuple] = table_heap->GetTuple(right_rid);
      if (!meta.is_deleted_) {
        //构造连接元组
        std::vector<Value> values;
        for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuple_.GetValue(&child_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
          values.emplace_back(right_tuple.GetValue(&plan_->InnerTableSchema(), i));
        }
        *tuple = Tuple(values, &plan_->OutputSchema());
        return true;
      }
    }

    // 如果是Left Join且没有匹配的右表元组，产生一个Right Tuple为Null的连接元组
    // 注意：只有在 rid_index_ 为 0 即从未进入上方 while 循环发任何数据，且确实是个空结果集时才发
    // 但这里状态机很微妙：result_rids_.empty() 为真意味着刚才ScanKey没查到，或者已经发完了
    // 若要支持Left Join，要在查完 Key 后立刻判断 empty

    // 2. 如果 result_rids_ 已经处理完，取下一个左表元组
    // 此时 rid_index_ == result_rids_.size()，说明上一轮的数据发完了
    // 但我们需要知道上一轮到底发没发过数据？如果是LEFT JOIN且没发过数据，得发一个
    // 由于状态机重置是在下面做的，所以无法区分“刚处理完一批”还是“刚查完发现是空的”
    // 所以逻辑需要微调：在ScanKey之后立刻判断是否Empty

    //从左表获取下一个元组
    if (!child_executor_->Next(&left_tuple_, rid)) {
      //左表元组已经耗尽，连接结束
      return false;
    }

    // 重置 RIDs 列表
    result_rids_.clear();
    rid_index_ = 0;

    //构造索引键值
    auto key_value = plan_->KeyPredicate()->Evaluate(&left_tuple_, child_executor_->GetOutputSchema());
    std::vector<Value> key_values = {key_value};
    Tuple index_key_tuple(key_values, index_info_->index_->GetKeySchema());

    //通过索引查找右表RIDs，填充到成员变量 result_rids_ 中
    index_info_->index_->ScanKey(index_key_tuple, &result_rids_, exec_ctx_->GetTransaction());

    // 处理 Left Join 空匹配情况
    if (result_rids_.empty() && plan_->GetJoinType() == JoinType::LEFT) {
      // 构造含 NULL 的输出
      std::vector<Value> values;
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple_.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
        values.emplace_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple(values, &plan_->OutputSchema());
      return true;
    }

    // 循环回到开头，开始消耗新获取的 result_rids_
  }
}

}  // namespace bustub
