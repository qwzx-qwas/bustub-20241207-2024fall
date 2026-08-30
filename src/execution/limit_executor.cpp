//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  // 初始化子执行器
  child_executor_->Init();
  count_ = 0;
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 检查是否达到限制数量
  if (count_ >= plan_->GetLimit()) {
    return false;
  }

  // 从子执行器获取下一个元组
  if (child_executor_->Next(tuple, rid)) {
    count_++;
    return true;
  }

  return false;
}

}  // namespace bustub
