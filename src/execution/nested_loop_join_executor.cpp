//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  // 初始化子执行器
  left_executor_->Init();
  // 注意之后right_executor每次在处理新left_tuple时都要重新初始化
  right_executor_->Init();
  left_tuple_valid_ = false;
  found_matched_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 循环直到找到符合条件的连接元组或者左表元组耗尽
  while (true) {
    // 如果当前没有正在处理的左表元组，则从左表获取下一个元组
    if (!left_tuple_valid_) {
      if (!left_executor_->Next(&left_tuple_, rid)) {
        //左表元组已经耗尽，连接结束
        return false;
      }
      // 获取到新的左表元组，标记为有效
      left_tuple_valid_ = true;
      // 重置found_matched_，准备处理新的左表元组
      found_matched_ = false;
      // 每次处理新左表元组时，重置右表执行器
      right_executor_->Init();
    }

    //尝试从右表获取下一个元组进行连接
    Tuple right_tuple;
    RID right_rid;
    if (right_executor_->Next(&right_tuple, &right_rid)) {
      // 右表还有元组，检查连接条件
      auto value = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                    right_executor_->GetOutputSchema());
      // 判断是否匹配，注意NULL值的处理
      if (!value.IsNull() && value.GetAs<bool>()) {
        // 说明找到了匹配的连接元组
        found_matched_ = true;
        // 构造输出元组
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple(values, &plan_->OutputSchema());
        return true;
      }
      // 继续尝试右表的下一个元组
    } else {
      // 右表元组已经耗尽，检查连接类型（因为left join需要特殊处理）
      if (plan_->GetJoinType() == JoinType::LEFT && !found_matched_) {
        // 对于LEFT JOIN，当前左表元组没有匹配的右表元组，需要输出左表元组与NULL填充的右表部分
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(
              ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
        }
        *tuple = Tuple(values, &plan_->OutputSchema());
        // 重置状态，准备处理下一个左表元组
        left_tuple_valid_ = false;
        return true;
      }
      // 当前左表元组处理完毕，重置状态，准备处理下一个左表元组
      left_tuple_valid_ = false;
    }
  }
}
}  // namespace bustub
