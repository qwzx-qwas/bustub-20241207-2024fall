//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

//聚合属于pipebreaker,这意味着它需要从子执行器中拉取所有的元组,完成聚合计算,然后才能产出结果

/*
还要注意考虑空表聚合的情况
没有Gruop By的聚合,空表时也要产出一行结果
有Group By的聚合,空表时不产出结果
*/

//采用延迟计算的方式
// Init只负责初始化子执行器,不进行聚合计算
// Next负责在第一次调用时完成聚合计算,后续调用则直接从聚合结果中产出元组
void AggregationExecutor::Init() {
  //初始化子执行器
  child_executor_->Init();
  //重置聚合哈希表
  aht_.Clear();
  aht_iterator_ = aht_.Begin();
  //重置状态量
  is_built_ = false;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  //检查状态，是否是第一次调用Next
  if (!is_built_) {
    //第一次调用，进行聚合计算
    Tuple child_tuple;
    RID child_rid;
    //从子执行器中拉取所有元组，插入到聚合哈希表中
    //每个元组按照Group By表达式计算出聚合键,按照聚合表达式计算出聚合值
    while (child_executor_->Next(&child_tuple, &child_rid)) {
      auto akey = MakeAggregateKey(&child_tuple);
      auto aval = MakeAggregateValue(&child_tuple);
      aht_.InsertCombine(akey, aval);
    }
    //处理空表且无Group By的情况
    if (aht_.Begin() == aht_.End() && plan_->GetGroupBys().empty()) {
      auto initial_agg_val = aht_.GenerateInitialAggregateValue();
      //手动插入初始聚合值
      //因为直接使用InsertCombine不会检查是否为空，而是直接+1
      //这会导致将空表的count(*)错误地计算为1
      std::vector<Value> values;
      values.insert(values.end(), initial_agg_val.aggregates_.begin(), initial_agg_val.aggregates_.end());
      *tuple = Tuple(values, &plan_->OutputSchema());
      is_built_ = true;
      return true;
    }
    //重置迭代器到聚合结果的开始位置
    aht_iterator_ = aht_.Begin();
    //更新状态量
    is_built_ = true;
  }

  //如果是空表且有Group By,则直接返回false
  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  //构造输出元组
  std::vector<Value> values;
  const auto &key = aht_iterator_.Key();
  const auto &val = aht_iterator_.Val();

  values.insert(values.end(), key.group_bys_.begin(), key.group_bys_.end());
  values.insert(values.end(), val.aggregates_.begin(), val.aggregates_.end());

  *tuple = Tuple(values, &plan_->OutputSchema());
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
