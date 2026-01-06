//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_executor_(std::move(left_child)),
      right_child_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Fall 2024: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/*创建的hash表是基于右子执行器（考虑到left join需要流式输出）的且是pipe breaker
 在init阶段完成哈希表的构建
*/

void HashJoinExecutor::Init() {
  //初始化左右子执行器
  left_child_executor_->Init();
  right_child_executor_->Init();
  //重置哈希表和输出缓冲区
  ht_.clear();
  result_buffer_.clear();

  //初始化Schema
  left_schema_ = left_child_executor_->GetOutputSchema();
  right_schema_ = right_child_executor_->GetOutputSchema();
  
  //基于右子执行器构建哈希表
  Tuple right_tuple;
  RID right_rid;
  while (right_child_executor_->Next(&right_tuple, &right_rid)) {
    HashJoinKey key;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      key.column_values_.emplace_back(expr->Evaluate(&right_tuple, right_schema_));
    }
    ht_[key].emplace_back(right_tuple);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    //先检查输出缓冲区是否有结果
    //有结果则直接输出
    if (!result_buffer_.empty()) {
      *tuple = result_buffer_.back();
      result_buffer_.pop_back();
      //hash join 生成的元组不关心rid，可以直接返回一个默认rid，即无效值
      *rid = tuple->GetRid();
      return true;
    }

    Tuple left_tuple;
    RID left_rid;
    //从左子执行器中拉取下一个元组
    if (!left_child_executor_->Next(&left_tuple, &left_rid)) {
      return false;
    }

    HashJoinKey key;
    //计算左元组的连接键
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      key.column_values_.emplace_back(expr->Evaluate(&left_tuple, left_schema_));
    }

    auto it = ht_.find(key);
    
    //找到了匹配的右元组
    if (it != ht_.end()) {
      for (auto iter = it->second.rbegin(); iter != it->second.rend(); ++iter) {
        const auto &right_tuple = *iter;
        std::vector<Value> values;
        values.reserve(plan_->OutputSchema().GetColumnCount());

        for (uint32_t i = 0; i < left_schema_.GetColumnCount(); ++i) {
          values.emplace_back(left_tuple.GetValue(&left_schema_, i));
        }
        for (uint32_t i = 0; i < right_schema_.GetColumnCount(); ++i) {
          values.emplace_back(right_tuple.GetValue(&right_schema_, i));
        }
        result_buffer_.emplace_back(values, &plan_->OutputSchema());
      }
    } else if (plan_->GetJoinType() == JoinType::LEFT) {
      //left join 且没有找到匹配的右元组
      //输出【左元组+右元组全空值】
      std::vector<Value> values;
      values.reserve(plan_->OutputSchema().GetColumnCount());

      for (uint32_t i = 0; i < left_schema_.GetColumnCount(); ++i) {
        values.emplace_back(left_tuple.GetValue(&left_schema_, i));
      }
      for (uint32_t i = 0; i < right_schema_.GetColumnCount(); ++i) {
        values.emplace_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
      }
      result_buffer_.emplace_back(values, &plan_->OutputSchema());
    }
  }
}

}  // namespace bustub
